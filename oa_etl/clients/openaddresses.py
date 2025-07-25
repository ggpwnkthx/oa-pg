"""Async client for interacting with the OpenAddresses batch API."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import httpx
import aiofiles
from time import perf_counter

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Job:
    """Metadata describing an OpenAddresses processing job."""

    id: str
    source_name: str


@dataclass(slots=True)
class DownloadResult:
    """Outcome of a successful archive download."""

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
        """Create a new client configured for the OpenAddresses batch API.

        Parameters
        ----------
        api_url:
            Base URL of the batch API.
        download_url:
            Template URL for job archive downloads.
        token:
            Optional bearer token to use instead of username/password.
        username, password:
            Credentials used when obtaining a token.
        login_timeout:
            Timeout in seconds for the login request.
        request_timeout:
            Timeout applied to normal API requests.
        max_connections:
            Maximum number of concurrent HTTP connections.
        """
        self.api_url = api_url.rstrip("/")
        self.download_url = download_url
        self.username = username
        self.password = password
        self.token = token
        self._login_timeout = httpx.Timeout(login_timeout)
        self._timeout = httpx.Timeout(request_timeout)
        self._limits = httpx.Limits(
            max_connections=max_connections, max_keepalive_connections=max_connections
        )
        self._client: httpx.AsyncClient | None = None
        self._auth_lock = asyncio.Lock()

    async def __aenter__(self) -> "OAAsyncClient":
        """Enter the async context manager.

        Ensures the HTTP client exists and performs authentication if
        credentials are available. Returns ``self`` so the client can be used
        within the ``async with`` block.
        """
        await self._ensure_client()
        if self.token and self._client:
            self._client.headers.update(
                {"Authorization": f"Bearer {self.token}"})
        elif self.username and self.password:
            await self._login()
        else:
            logger.warning("Continuing without authentication")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Close the HTTP client when leaving the context manager."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _ensure_client(self) -> None:
        """Instantiate the underlying :class:`httpx.AsyncClient` if missing."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout, limits=self._limits)

    def _require_client(self) -> httpx.AsyncClient:
        """Return the initialized HTTP client or raise ``RuntimeError``."""
        if self._client is None:
            raise RuntimeError(
                "Client not initialized; use 'async with OAAsyncClient()'")
        return self._client

    async def _login(self) -> None:
        """Authenticate using stored credentials and store the new token."""
        if not (self.username and self.password):
            raise RuntimeError("Username/password required")
        async with self._auth_lock:
            if self.token:
                return
            client = self._require_client()
            resp = await client.post(
                f"{self.api_url}/login",
                json={"username": self.username, "password": self.password},
                timeout=self._login_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("token")
            if not token:
                raise RuntimeError("No token returned upon login")
            self.token = token
            client.headers.update({"Authorization": f"Bearer {self.token}"})

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Perform an HTTP request with automatic re-authentication.

        Parameters
        ----------
        method:
            HTTP method such as ``"GET"`` or ``"POST"``.
        url:
            Fully qualified request URL.
        **kwargs:
            Passed directly to :func:`httpx.AsyncClient.request`.
        """
        client = self._require_client()
        resp = await client.request(method, url, **kwargs)
        if resp.status_code == 401 and self.username and self.password:
            await self._login()
            resp = await client.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp

    async def fetch_jobs(self, source: str, layer: str, timeout: float = 30.0) -> List[Job]:
        """Return the list of available jobs for a given source and layer.

        Parameters
        ----------
        source:
            Data source identifier (e.g. ``us/ca``).
        layer:
            Layer name such as ``addresses``.
        timeout:
            Request timeout in seconds.
        """
        url = f"{self.api_url}/data"
        params = {"source": source, "layer": layer}
        resp = await self._request("GET", url, params=params, timeout=timeout)
        data = resp.json()
        jobs: List[Job] = []
        for item in data:
            jid = item.get("job")
            src = item.get("source")
            if jid and src:
                jobs.append(Job(id=str(jid), source_name=src))
        logger.info("Fetched %d jobs", len(jobs))
        return jobs

    async def download_job(
        self,
        job_id: str,
        dest: Path,
        timeout: float = 60.0,
        chunk_size: int = 8192,
    ) -> DownloadResult:
        """Download a job archive and save it to ``dest``.

        Parameters
        ----------
        job_id:
            Identifier of the job to download.
        dest:
            Destination file path for the archive.
        timeout:
            Maximum seconds to wait for the entire download.
        chunk_size:
            Size of chunks read from the network.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            size = dest.stat().st_size
            logger.debug("Skipping existing: %s (%d bytes)", dest, size)
            return DownloadResult(path=dest, bytes=size, duration=0.0)

        url = self.download_url.format(id=job_id)
        client = self._require_client()
        start = perf_counter()
        n_bytes = 0

        async with client.stream("GET", url, timeout=timeout) as resp:
            resp.raise_for_status()
            async with aiofiles.open(dest, "wb") as fd:
                async for chunk in resp.aiter_bytes(chunk_size):
                    n_bytes += len(chunk)
                    await fd.write(chunk)

        duration = perf_counter() - start
        logger.debug(
            "Downloaded job %s to %s: %.2f MiB in %.3fs (%.2f MiB/s)",
            job_id,
            dest,
            n_bytes / (1024 * 1024),
            duration,
            (n_bytes / (1024 * 1024)) /
            duration if duration > 0 else float("inf"),
        )
        return DownloadResult(path=dest, bytes=n_bytes, duration=duration)
