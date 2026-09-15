from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Sequence

import pyodbc


@dataclass(frozen=True)
class LoadAudit:
    pipeline_name: str
    batch_id: str
    row_count: int
    started_at: datetime
    completed_at: datetime


class SqlServerWarehouse:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    @contextmanager
    def connection(self) -> Iterator[pyodbc.Connection]:
        connection = pyodbc.connect(self.connection_string, autocommit=False)
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def executemany(self, statement: str, rows: Iterable[Sequence[Any]]) -> int:
        started = datetime.now(timezone.utc)
        materialized = list(rows)
        with self.connection() as connection:
            cursor = connection.cursor()
            cursor.fast_executemany = True
            cursor.executemany(statement, materialized)
        _ = started
        return len(materialized)

    def execute_procedure(self, procedure: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        if not procedure.replace("_", "").replace(".", "").isalnum():
            raise ValueError("Invalid stored-procedure identifier")
        placeholders = ",".join("?" for _ in parameters)
        statement = f"EXEC {procedure} {placeholders}" if placeholders else f"EXEC {procedure}"
        with self.connection() as connection:
            cursor = connection.cursor()
            cursor.execute(statement, tuple(parameters))
            return cursor.fetchall() if cursor.description else []
