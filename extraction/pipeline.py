"""Resumable extraction pipeline: pages through a vendor API and writes to a
sink in chunks, persisting a cursor after each chunk so a crash mid-run
costs at most one chunk of rework, not a full restart.
"""
from __future__ import annotations

import argparse
import logging

from .api_client import VendorAPIClient
from .mock_source import MockVendorSource
from .sinks import BigQuerySink, Sink, SQLiteSink

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def run(source, sink: Sink, chunk_size: int = 500) -> int:
    cursor = sink.load_cursor()
    total = 0
    chunk: list[dict] = []

    for page in source.iter_pages(start_cursor=cursor):
        chunk.extend(page.items)
        total += len(page.items)
        if len(chunk) >= chunk_size:
            sink.write_chunk(chunk, cursor=page.next_cursor)
            log.info("wrote chunk of %d items (total %d)", len(chunk), total)
            chunk = []

    if chunk:
        sink.write_chunk(chunk, cursor=None)
        log.info("wrote final chunk of %d items (total %d)", len(chunk), total)

    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--demo", action="store_true", help="use in-memory mock data, no network or GCP needed"
    )
    parser.add_argument("--base-url", default="https://api.example-vendor.com")
    parser.add_argument("--project", default=None, help="BigQuery project (ignored in --demo mode)")
    parser.add_argument("--chunk-size", type=int, default=500)
    args = parser.parse_args()

    if args.demo:
        source = MockVendorSource(item_count=2500, page_size=250)
        sink: Sink = SQLiteSink("demo.db")
    else:
        source = VendorAPIClient(args.base_url)
        sink = BigQuerySink(project=args.project, dataset="raw", table="items")

    total = run(source, sink, chunk_size=args.chunk_size)
    log.info("done, %d items extracted", total)


if __name__ == "__main__":
    main()
