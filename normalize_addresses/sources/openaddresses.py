import os
import contextlib
from time import perf_counter
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
import asyncio
import concurrent.futures
import zlib
import random

import aiofiles  # type: ignore
import httpx     # type: ignore
import orjson     # type: ignore

from dataclasses import dataclass

from ..utils import _clean_ws, normalize_region
from ..models import InputAddress
from .base import AddressSource
from ..logging_config import logger

from dotenv import load_dotenv
load_dotenv()

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
        http2: bool = True,
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
        self._http2 = http2
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
            # Enable HTTP/2 by default - higher throughput with many small requests
            self._client = httpx.AsyncClient(
                http2=self._http2, timeout=self._timeout, limits=self._limits
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
    """Zero-copy streaming OpenAddresses adapter with resilient retries."""

    def __init__(
        self,
        source: str,
        layer: str,
        max_connections: int = 16,
        jobs_limit: Optional[int] = None,
        token: Optional[str] = None,
        *,
        stream_max_retries: int = 5,
        stream_backoff_base: float = 0.5,
        stream_timeout: float = 120.0,
    ):
        self.source = source
        self.layer = layer
        self.max_connections = max_connections
        self.jobs_limit = jobs_limit
        self.token = token

        # Streaming robustness knobs
        self.stream_max_retries = max(1, stream_max_retries)
        self.stream_backoff_base = max(0.0, stream_backoff_base)
        self.stream_timeout = max(10.0, stream_timeout)

        self._producer_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(max_connections, (os.cpu_count() or 8) * 2)
        )

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

    async def _iter_geojson_props_streaming(
        self,
        client: httpx.AsyncClient,
        job: Job,
        *,
        timeout: float,
        max_retries: int,
        backoff_base: float,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream and decompress a .geojson.gz file from OpenAddresses, yielding GeoJSON 'properties'.
        Resilient to HTTP/2 stream resets with exponential backoff retries.

        NOTE: On retry we restart the stream (cannot resume mid-GZIP). This can
        re-emit features already seen before the failure. Downstream dedupe mitigates this.
        """
        url = f"https://v2.openaddresses.io/batch-prod/job/{job.id}/source.geojson.gz"
        attempt = 0

        while True:
            attempt += 1
            decompressor = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
            buffer = b""
            count = 0
            start = perf_counter()

            try:
                logger.info("OA: streaming job %s from %s (attempt %d/%d)",
                            job.id, url, attempt, max_retries)
                async with client.stream("GET", url, timeout=timeout) as resp:
                    resp.raise_for_status()

                    async for chunk in resp.aiter_bytes():
                        try:
                            buffer += decompressor.decompress(chunk)
                        except zlib.error as e:
                            logger.exception(
                                "Decompression error in job %s (attempt %d): %s", job.id, attempt, e)
                            raise

                        while b"\n" in buffer:
                            line, buffer = buffer.split(b"\n", 1)
                            line = line.strip()
                            if not line:
                                continue
                            if line.startswith(b"\x1e"):
                                line = line.lstrip(b"\x1e").strip()
                                if not line:
                                    continue
                            try:
                                obj = orjson.loads(line)
                                if obj.get("type") == "Feature":
                                    yield obj.get("properties") or {}
                                    count += 1
                                elif obj.get("type") == "FeatureCollection":
                                    for feat in obj.get("features", []):
                                        yield feat.get("properties") or {}
                                        count += 1
                                elif isinstance(obj.get("properties"), dict):
                                    yield obj["properties"]
                                    count += 1
                            except Exception:
                                logger.debug(
                                    "Skipping malformed JSON in job %s", job.id)

                # Handle leftover trailing data
                buffer += decompressor.flush()
                for raw_line in buffer.split(b"\n"):
                    line = raw_line.strip()
                    if not line:
                        continue
                    if line.startswith(b"\x1e"):
                        line = line.lstrip(b"\x1e").strip()
                        if not line:
                            continue
                    try:
                        obj = orjson.loads(line)
                        if obj.get("type") == "Feature":
                            yield obj.get("properties") or {}
                            count += 1
                        elif obj.get("type") == "FeatureCollection":
                            for feat in obj.get("features", []):
                                yield feat.get("properties") or {}
                                count += 1
                        elif isinstance(obj.get("properties"), dict):
                            yield obj["properties"]
                            count += 1
                    except Exception:
                        logger.debug(
                            "Skipping final malformed JSON in job %s", job.id)

                duration = perf_counter() - start
                logger.info(
                    "OA: parsed job %s → %d features in %.2fs (%.1f f/s) [attempt %d]",
                    job.id, count, duration, count / duration if duration else 0.0, attempt
                )
                # Success; exit retry loop
                return

            except (
                httpx.RemoteProtocolError,
                httpx.ReadError,
                httpx.ConnectError,
                httpx.TimeoutException,
                zlib.error,
            ) as e:
                if attempt >= max_retries:
                    logger.error(
                        "OA: streaming job %s failed after %d attempts: %s",
                        job.id, attempt, repr(e)
                    )
                    # Reraise to let caller handle/log
                    raise
                # Backoff with small jitter
                wait = backoff_base * (2 ** (attempt - 1)) + \
                    random.uniform(0.0, 0.25)
                logger.warning(
                    "OA: stream error job %s (attempt %d/%d): %s; retrying in %.2fs",
                    job.id, attempt, max_retries, repr(e), wait
                )
                await asyncio.sleep(wait)
                # Loop to retry with a fresh connection/decompressor
                continue

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

            parse_sem = asyncio.Semaphore(self.max_connections)
            out_q: asyncio.Queue[Any] = asyncio.Queue(
                maxsize=self.max_connections * 8)

            async def parse_job(job: Job) -> None:
                async with parse_sem:
                    try:
                        async for props in self._iter_geojson_props_streaming(
                            client._require_client(),
                            job,
                            timeout=self.stream_timeout,
                            max_retries=self.stream_max_retries,
                            backoff_base=self.stream_backoff_base,
                        ):
                            # Very basic validation check
                            if not props.get("number") or not props.get("street"):
                                continue
                            raw, cc = self._compose_raw(props, job)
                            await out_q.put(InputAddress(address_raw=raw, country_code=cc, extras=props))
                    except Exception as e:
                        logger.exception("OA: error in job %s: %s", job.id, e)

            orchestrator = asyncio.create_task(
                self._orchestrate_jobs(jobs, parse_job, out_q))

            try:
                while True:
                    item = await out_q.get()
                    if item is None:
                        break
                    yield item
            finally:
                orchestrator.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await orchestrator

    async def _orchestrate_jobs(
        self,
        jobs: List[Job],
        parse_job_fn,
        out_q: asyncio.Queue
    ) -> None:
        tasks = [asyncio.create_task(parse_job_fn(
            job), name=f"oa_job_{job.id}") for job in jobs]
        await asyncio.gather(*tasks, return_exceptions=True)
        await out_q.put(None)
