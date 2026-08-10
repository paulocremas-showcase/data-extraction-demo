"""Storage sinks for extracted data.

`BigQuerySink` is the real production sink, unmodified GCP client code.
`SQLiteSink` exists only so `--demo` mode runs with zero cloud setup; the
pipeline logic in `pipeline.py` doesn't know which one it's writing to.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Optional, Protocol


class Sink(Protocol):
    def load_cursor(self) -> Optional[str]: ...
    def write_chunk(self, items: list[dict], cursor: Optional[str]) -> None: ...


class SQLiteSink:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, payload TEXT)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS cursor_state "
            "(id INTEGER PRIMARY KEY CHECK (id = 1), cursor TEXT)"
        )
        self.conn.commit()

    def load_cursor(self) -> Optional[str]:
        row = self.conn.execute("SELECT cursor FROM cursor_state WHERE id = 1").fetchone()
        return row[0] if row else None

    def write_chunk(self, items: list[dict], cursor: Optional[str]) -> None:
        self.conn.executemany(
            "INSERT OR REPLACE INTO items (id, payload) VALUES (?, ?)",
            [(item["id"], json.dumps(item)) for item in items],
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO cursor_state (id, cursor) VALUES (1, ?)", (cursor,)
        )
        self.conn.commit()


class BigQuerySink:
    """Production sink. Requires google-cloud-bigquery and GCP credentials;
    imported lazily so the --demo path has no GCP dependency at all."""

    def __init__(self, project: Optional[str], dataset: str, table: str):
        from google.cloud import bigquery

        self._bq = bigquery
        self.client = bigquery.Client(project=project)
        self.table_ref = f"{project}.{dataset}.{table}"
        self.cursor_table_ref = f"{project}.{dataset}._pipeline_cursor"

    def load_cursor(self) -> Optional[str]:
        rows = list(
            self.client.query(f"SELECT cursor FROM `{self.cursor_table_ref}` WHERE id = 1").result()
        )
        return rows[0].cursor if rows else None

    def write_chunk(self, items: list[dict], cursor: Optional[str]) -> None:
        errors = self.client.insert_rows_json(self.table_ref, items)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        # parameterized, never string-formatted, even for an internal cursor value
        job_config = self._bq.QueryJobConfig(
            query_parameters=[self._bq.ScalarQueryParameter("cursor", "STRING", cursor)]
        )
        self.client.query(
            f"MERGE `{self.cursor_table_ref}` T USING (SELECT @cursor AS cursor) S "
            "ON T.id = 1 "
            "WHEN MATCHED THEN UPDATE SET cursor = S.cursor "
            "WHEN NOT MATCHED THEN INSERT (id, cursor) VALUES (1, S.cursor)",
            job_config=job_config,
        ).result()
