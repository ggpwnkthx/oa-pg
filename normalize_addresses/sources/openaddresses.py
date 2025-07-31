# normalize_addresses/sources/openaddresses.py

import os
import gzip
import contextlib
import threading
from time import perf_counter
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
import asyncio
import concurrent.futures

import aiofiles  # type: ignore
import httpx     # type: ignore
import orjson     # type: ignore

from dataclasses import dataclass

from ..utils import _clean_ws, normalize_one, normalize_region
from ..models import InputAddress
from .base import AddressSource
from ..logging_config import logger


@dataclass(slots=True)
class Job:
    id: str
    source_name: str


@dataclass(slots=True)
class DownloadResult:
    path: Path
    bytes: int
    duration: float


class OAAsyncClient:
    """Minimal async wrapper around the OpenAddresses batch API."""

    def __init__(
        self,
        api_url: str = os.getenv(
            "OA_API_URL", "https://batch.openaddresses.io/api"),
        download_url: str = os.getenv(
            "OA_DOWNLOAD_URL",
            "https://v2.openaddresses.io/batch-prod/job/{id}/source.geojson.gz",
        ),
        token: Optional[str] = None,
        username: Optional[str] = os.getenv("OA_USERNAME"),
        password: Optional[str] = os.getenv("OA_PASSWORD"),
        login_timeout: float = 30.0,
        request_timeout: float = 30.0,
        max_connections: int = 10,
    ):
        self.api_url = api_url.rstrip("/")
        self.download_url = download_url
        self.token = token
        self.username = username
        self.password = password
        self._timeout = httpx.Timeout(request_timeout)
        self._limits = httpx.Limits(
            max_connections=max_connections, max_keepalive_connections=max_connections
        )
        self._client: Optional[httpx.AsyncClient] = None
        self._auth_lock = asyncio.Lock()

    async def __aenter__(self) -> "OAAsyncClient":
        await self._ensure_client()
        if self.token and self._client:
            self._client.headers.update(
                {"Authorization": f"Bearer {self.token}"})
        elif self.username and self.password:
            await self._login()
        else:
            logger.warning(
                "OpenAddresses client: continuing without authentication")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _ensure_client(self) -> None:
        if self._client is None:
            # Enable HTTP/2 - gives higher throughput with many small requests
            self._client = httpx.AsyncClient(
                http2=True, timeout=self._timeout, limits=self._limits
            )

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                "Client not initialized; use 'async with OAAsyncClient()'")
        return self._client

    async def _login(self) -> None:
        if not (self.username and self.password):
            raise RuntimeError("Username/password required")
        async with self._auth_lock:
            if self.token:
                return
            client = self._require_client()
            resp = await client.post(
                f"{self.api_url}/login",
                json={"username": self.username, "password": self.password},
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("token")
            if not token:
                raise RuntimeError("No token returned upon login")
            self.token = token
            client.headers.update({"Authorization": f"Bearer {self.token}"})

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        client = self._require_client()
        resp = await client.request(method, url, **kwargs)
        if resp.status_code == 401 and self.username and self.password:
            await self._login()
            resp = await client.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp

    async def fetch_jobs(self, source: str, layer: str, timeout: float = 30.0) -> List[Job]:
        resp = await self._request(
            "GET",
            f"{self.api_url}/data",
            params={"source": source, "layer": layer},
            timeout=timeout,
        )
        data = resp.json()
        jobs: List[Job] = []
        for item in data:
            jid = item.get("job")
            src = item.get("source")
            if jid and src:
                jobs.append(Job(id=str(jid), source_name=src))
        logger.info("OA: fetched %d jobs for %s/%s", len(jobs), source, layer)
        return jobs

    async def download_job(
        self,
        job_id: str,
        dest: Path,
        timeout: float = 120.0,
        chunk_size: int = 1 << 15,
    ) -> DownloadResult:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            size = dest.stat().st_size
            logger.debug("OA: skipping existing %s (%d bytes)",
                         dest.name, size)
            return DownloadResult(path=dest, bytes=size, duration=0.0)

        url = self.download_url.format(id=job_id)
        client = self._require_client()
        start = perf_counter()
        n_bytes = 0
        logger.info("OA: start download job %s -> %s", job_id, dest.name)

        async with client.stream("GET", url, timeout=timeout) as resp:
            resp.raise_for_status()
            # NOTE: sendfile zero-copy would be faster; aiofiles keeps code portable
            async with aiofiles.open(dest, "wb") as fd:
                async for chunk in resp.aiter_bytes(chunk_size):
                    n_bytes += len(chunk)
                    await fd.write(chunk)

        duration = perf_counter() - start
        mib = n_bytes / (1024 * 1024)
        rate = mib / duration if duration > 0 else 0.0
        logger.info(
            "OA: finished job %s -> %s: %.2f MiB in %.2fs (%.2f MiB/s)",
            job_id,
            dest.name,
            mib,
            duration,
            rate,
        )
        return DownloadResult(path=dest, bytes=n_bytes, duration=duration)


class OpenAddressesSource(AddressSource):
    """OpenAddresses adapter. Downloads jobs & yields InputAddress."""

    def __init__(
        self,
        source: str,
        layer: str,
        tmp_dir: Path,
        max_connections: int = 16,
        jobs_limit: Optional[int] = None,
        token: Optional[str] = None,
        parse_timeout: float = 900.0,
    ):
        self.source = source
        self.layer = layer
        self.tmp_dir = tmp_dir
        self.max_connections = max_connections
        self.jobs_limit = jobs_limit
        self.token = token
        self.parse_timeout = parse_timeout

        # One thread pool that fans out to all producer tasks
        self._producer_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(max_connections, (os.cpu_count() or 8) * 2)
        )

    # ------------------------------------------------------------------ #
    # GeoJSON streaming helper                                           #
    # ------------------------------------------------------------------ #
    async def _iter_geojson_props(self, gz_path: Path) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream 'properties' objects from an OA .geojson.gz file.
        """
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[Optional[Dict[str, Any]]
                             ] = asyncio.Queue(maxsize=65536)
        stop_event = threading.Event()

        def _put(item: Optional[Dict[str, Any]]) -> None:
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                if item is not None:
                    logger.warning("OA: queue backpressure; stopping producer for %s",
                                   gz_path.name)
                    stop_event.set()
                else:
                    asyncio.create_task(queue.put(None))

        def _safe_put(item: Optional[Dict[str, Any]]) -> bool:
            if stop_event.is_set() and item is not None:
                return False
            loop.call_soon_threadsafe(_put, item)
            return True

        def producer() -> None:
            n = 0
            start = perf_counter()

            class Fallback(Exception):
                pass

            def _emit(obj: Dict[str, Any]) -> int:
                cnt = 0
                t = obj.get("type")
                if t == "Feature":
                    props = obj.get("properties") or {}
                    if not _safe_put(props):
                        return 0
                    return 1
                if t == "FeatureCollection" and isinstance(obj.get("features"), list):
                    for feat in obj["features"]:
                        if stop_event.is_set():
                            break
                        if isinstance(feat, dict):
                            props = feat.get("properties") or {}
                            if not _safe_put(props):
                                break
                            cnt += 1
                    return cnt
                props = obj.get("properties")
                if isinstance(props, dict):
                    _safe_put(props)
                    return 1
                return 0

            try:
                with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace", newline="") as fh:
                    first = False
                    for raw in fh:
                        if stop_event.is_set():
                            break
                        line = raw.strip()
                        if not line:
                            continue
                        if line.startswith("\x1e"):
                            line = line.lstrip("\x1e").strip()
                            if not line:
                                continue
                        try:
                            obj = orjson.loads(line)
                        except Exception:
                            if not first:
                                raise Fallback()
                            continue
                        first = True
                        n += _emit(obj)

                elapsed = perf_counter() - start
                rate = n / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "OA: parsed %s → %s features in %.2fs (%.1f f/s, mode=ndjson)",
                    gz_path.name, f"{n:,}", elapsed, rate,
                )

            except Exception as e:
                logger.exception(
                    "OA: unexpected error streaming %s: %s", gz_path.name, e)

            finally:
                _safe_put(None)

        # Kick-off the producer exactly once
        self._producer_executor.submit(producer)

        # Consumer side
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            stop_event.set()

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #
    def _compose_raw(self, props: Dict[str, Any], job: Job) -> Tuple[str, Optional[str]]:
        _country, _region, _city = job.source_name.split("/")
        number = props.get("number") or ""
        street = (props.get("street") or props.get(
            "street_name") or "").upper()
        unit = props.get("unit") or ""
        city = (
            (props.get("city") or props.get("locality")
             or _city.replace("city_of_", "").replace("_", " "))
            .upper()
        )
        district = props.get("district") or ""
        raw_region = props.get("region") or props.get("state") or _region
        country = (props.get("country") or _country).upper()
        region = normalize_region(str(raw_region), country).upper()
        postcode = props.get("postcode") or ""

        left = " ".join(p for p in [str(number).strip(), str(
            street).strip(), str(unit).strip()] if p)
        right = ", ".join(
            p for p in [str(city).strip(), str(district).strip(), region, str(postcode).strip()] if p
        )
        raw = left
        if right:
            raw = f"{left}, {right}"
        if country:
            raw = f"{raw}, {country}"

        cc: Optional[str] = country.strip().upper() if country else None
        return _clean_ws(raw), cc

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    async def records(self) -> AsyncGenerator[InputAddress, None]:
        async with OAAsyncClient(max_connections=self.max_connections, token=self.token) as client:
            jobs = await client.fetch_jobs(self.source, self.layer)
            if self.jobs_limit is not None:
                jobs = jobs[: self.jobs_limit]
            elif len(jobs) > 500:
                logger.warning(
                    "OA: %d jobs selected (no --jobs-limit). This may take a long time; "
                    "use --jobs-limit to test first.", len(jobs)
                )

            self.tmp_dir.mkdir(parents=True, exist_ok=True)
            http_sem = asyncio.Semaphore(self.max_connections)

            async def _dl(job: Job) -> Tuple[Path, Job]:
                dest = self.tmp_dir / f"oa_{job.id}.geojson.gz"
                async with http_sem:
                    await client.download_job(job.id, dest)
                return dest, job

            dl_tasks = [asyncio.create_task(
                _dl(j), name=f"oa_dl_{j.id}") for j in jobs]
            out_q: asyncio.Queue[Any] = asyncio.Queue(
                maxsize=self.max_connections * 8)
            parse_sem = asyncio.Semaphore(
                min(self.max_connections, (os.cpu_count() or 8) * 2)
            )
            parser_tasks: List[asyncio.Task] = []

            async def parse_file(p: Path, j: Job) -> None:
                yielded = 0
                start = perf_counter()
                async with parse_sem:
                    logger.info("OA: parsing begin → %s", p.name)
                    try:
                        async for props in self._iter_geojson_props(p):
                            raw, cc = self._compose_raw(props, j)
                            await out_q.put(
                                InputAddress(address_raw=raw,
                                             country_code=cc, extras=props)
                            )
                            yielded += 1
                    except asyncio.TimeoutError:
                        logger.error(
                            "OA: parse timeout → %s; skipping remainder", p.name)
                    finally:
                        dur = perf_counter() - start
                        rate = yielded / dur if dur else 0.0
                        logger.info("OA: finished %s → %s records in %.2fs (%.1f r/s)",
                                    p.name, f"{yielded:,}", dur, rate)

            async def orchestrate() -> None:
                for dl in asyncio.as_completed(dl_tasks):
                    try:
                        p, j = await dl
                    except Exception as e:
                        logger.exception("OA: download failed: %s", e)
                        continue
                    parser_tasks.append(asyncio.create_task(parse_file(p, j)))

                await asyncio.gather(*parser_tasks, return_exceptions=True)
                await out_q.put(None)  # sentinel

            orch = asyncio.create_task(orchestrate())
            try:
                while True:
                    item = await out_q.get()
                    if item is None:
                        break
                    yield item
            finally:
                orch.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await orch
