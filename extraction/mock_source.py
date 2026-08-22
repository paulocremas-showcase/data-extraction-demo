"""In-memory fake vendor API for `--demo` mode: no network calls, no
credentials. Implements the same `iter_pages(start_cursor) -> Iterator[Page]`
contract as `api_client.VendorAPIClient`, so `pipeline.run()` doesn't need to
know or care which one it's talking to.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Iterator, Optional


@dataclass
class Page:
    items: list[dict]
    next_cursor: Optional[str]


class MockVendorSource:
    def __init__(self, item_count: int = 2500, page_size: int = 250, seed: int = 42):
        self.item_count = item_count
        self.page_size = page_size
        self._rng = random.Random(seed)  # noqa: S311 -- deterministic mock data, not a security use

    def iter_pages(self, start_cursor: Optional[str] = None) -> Iterator[Page]:
        offset = int(start_cursor) if start_cursor else 0
        while offset < self.item_count:
            end = min(offset + self.page_size, self.item_count)
            items = [
                {
                    "id": f"item-{i}",
                    "name": f"Demo Product {i}",
                    "price": round(self._rng.uniform(10, 500), 2),
                }
                for i in range(offset, end)
            ]
            next_cursor = str(end) if end < self.item_count else None
            yield Page(items=items, next_cursor=next_cursor)
            offset = end
