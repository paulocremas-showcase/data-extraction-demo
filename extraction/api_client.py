"""Paginated vendor API client with retry-on-429/5xx.

Treating HTTP 429 as retryable (not a permanent client error) instead of
giving up after one hit is the fix for a real production incident: a shared
per-account rate limit across multiple pipelines caused a recurring failure
where a whole day's page could be lost. Same 3-attempt backoff pattern used
to fix it there.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Iterator, Optional

import httpx


def _is_retryable(status: int) -> bool:
    return status == 429 or 500 <= status < 600


@dataclass
class Page:
    items: list[dict]
    next_cursor: Optional[str]


class VendorAPIClient:
    """Talks to a real paginated vendor API. For a zero-setup local demo,
    use `extraction.mock_source.MockVendorSource` instead, it implements the
    same `iter_pages()` contract."""

    def __init__(self, base_url: str, max_retries: int = 3):
        self.base_url = base_url
        self.max_retries = max_retries
        self._client = httpx.Client(timeout=30)

    def get_page(self, cursor: Optional[str] = None) -> Page:
        params = {"cursor": cursor} if cursor else {}
        backoff = 1.0
        for attempt in range(self.max_retries):
            resp = self._client.get(f"{self.base_url}/items", params=params)
            if resp.status_code == 200:
                data = resp.json()
                return Page(items=data["items"], next_cursor=data.get("next_cursor"))
            if _is_retryable(resp.status_code) and attempt < self.max_retries - 1:
                time.sleep(backoff + random.uniform(0, 0.5))  # noqa: S311 -- retry jitter, not a security use
                backoff *= 3
                continue
            resp.raise_for_status()
        raise RuntimeError("unreachable")

    def iter_pages(self, start_cursor: Optional[str] = None) -> Iterator[Page]:
        cursor = start_cursor
        while True:
            page = self.get_page(cursor)
            yield page
            if not page.next_cursor:
                return
            cursor = page.next_cursor
