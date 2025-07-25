from __future__ import annotations

import asyncio
import gzip
import logging
import random
from time import perf_counter
from typing import Any, Awaitable, Callable, TypeVar, overload

import httpx
import orjson
from psycopg_pool import PoolTimeout
import psycopg

logger = logging.getLogger(__name__)

T = TypeVar("T")


@overload
async def retry_logic(
    func: Callable[..., Awaitable[T]],
    max_retries: int,
    backoff_factor: float,
    *args: Any,
    **kwargs: Any
) -> T: ...


@overload
async def retry_logic(
    func: Callable[..., Awaitable[None]],
    max_retries: int,
    backoff_factor: float,
    *args: Any,
    **kwargs: Any
) -> None: ...


async def retry_logic(
    func: Callable[..., Awaitable[Any]],
    max_retries: int,
    backoff_factor: float,
    *args: Any,
    **kwargs: Any
) -> Any:
    retries = 0

    retriable = (
        httpx.RequestError,
        asyncio.TimeoutError,
        PoolTimeout,
        psycopg.Error,
        OSError,
        gzip.BadGzipFile,
        orjson.JSONDecodeError,
    )

    while retries <= max_retries:
        try:
            attempt_start = perf_counter()
            result = await func(*args, **kwargs)
            duration = perf_counter() - attempt_start
            logger.info(
                "Function %s succeeded in %.3f s on attempt %d/%d",
                getattr(func, "__name__", str(func)),
                duration,
                retries + 1,
                max_retries,
            )
            return result

        except retriable as exc:
            retries += 1
            wait_time = random.uniform(1, backoff_factor ** retries)
            logger.warning(
                "Function %s encountered %s, retrying in %.1f s (%d/%d)",
                getattr(func, "__name__", str(func)),
                type(exc).__name__,
                wait_time,
                retries,
                max_retries,
            )
            await asyncio.sleep(wait_time)
            if retries > max_retries:
                logger.error(
                    "Function %s failed after %d retries",
                    getattr(func, "__name__", str(func)),
                    max_retries,
                )
                raise

        except Exception:
            raise

    raise AssertionError("unreachable")  # pragma: no cover
