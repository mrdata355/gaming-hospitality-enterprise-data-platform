from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

import ibm_db
import ibm_db_dbi


@dataclass(frozen=True)
class Watermark:
    value: int
    captured_at: datetime


class DB2IncrementalExtractor:
    """DB2 extractor using a monotonic source sequence as a replayable watermark."""

    def __init__(self, connection_string: str, *, fetch_size: int = 10_000):
        self.connection_string = connection_string
        self.fetch_size = fetch_size

    def rows(
        self,
        *,
        table: str,
        columns: Sequence[str],
        watermark_column: str,
        low_watermark: int,
        high_watermark: int,
        predicates: str = "1=1",
    ) -> Iterator[dict[str, Any]]:
        safe_identifiers = [table, watermark_column, *columns]
        if any(not ident.replace("_", "").replace(".", "").isalnum() for ident in safe_identifiers):
            raise ValueError("Only alphanumeric, underscore and dotted identifiers are allowed")
        sql = f"""
            SELECT {', '.join(columns)}
            FROM {table}
            WHERE {watermark_column} > ?
              AND {watermark_column} <= ?
              AND ({predicates})
            ORDER BY {watermark_column}
            WITH UR
        """
        raw = ibm_db.connect(self.connection_string, "", "")
        with closing(ibm_db_dbi.Connection(raw)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(sql, (low_watermark, high_watermark))
                names = [description[0].lower() for description in cursor.description]
                while batch := cursor.fetchmany(self.fetch_size):
                    for row in batch:
                        yield dict(zip(names, row, strict=True))

    def capture_high_watermark(self, table: str, watermark_column: str) -> Watermark:
        if not all(x.replace("_", "").replace(".", "").isalnum() for x in (table, watermark_column)):
            raise ValueError("Invalid identifier")
        raw = ibm_db.connect(self.connection_string, "", "")
        with closing(ibm_db_dbi.Connection(raw)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(f"SELECT COALESCE(MAX({watermark_column}), 0) FROM {table} WITH UR")
                value = int(cursor.fetchone()[0])
        return Watermark(value=value, captured_at=datetime.now(timezone.utc))
