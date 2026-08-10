"""Smoke tests, not a full suite. Run directly: python tests/test_pipeline.py"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from extraction.mock_source import MockVendorSource
from extraction.pipeline import run
from extraction.sinks import SQLiteSink


def test_pipeline_extracts_all_items_and_clears_cursor():
    with tempfile.TemporaryDirectory() as tmp:
        source = MockVendorSource(item_count=100, page_size=30)
        sink = SQLiteSink(f"{tmp}/test.db")

        total = run(source, sink, chunk_size=50)
        assert total == 100

        (count,) = sink.conn.execute("SELECT COUNT(*) FROM items").fetchone()
        assert count == 100
        assert sink.load_cursor() is None


def test_pipeline_resumes_from_cursor():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = f"{tmp}/test.db"

        # first run only sees the first 30 items, as if it crashed after page 1
        source = MockVendorSource(item_count=100, page_size=30)
        first_page = next(source.iter_pages())
        sink = SQLiteSink(db_path)
        sink.write_chunk(first_page.items, cursor=first_page.next_cursor)

        # a fresh run against the same sink should resume, not restart
        resumed_sink = SQLiteSink(db_path)
        assert resumed_sink.load_cursor() == first_page.next_cursor

        run(MockVendorSource(item_count=100, page_size=30), resumed_sink, chunk_size=30)
        (count,) = resumed_sink.conn.execute("SELECT COUNT(*) FROM items").fetchone()
        assert count == 100


if __name__ == "__main__":
    test_pipeline_extracts_all_items_and_clears_cursor()
    test_pipeline_resumes_from_cursor()
    print("all tests passed")
