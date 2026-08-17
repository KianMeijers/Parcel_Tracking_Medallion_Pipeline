"""Executes a validated read-only query against the gold DuckDB views and
caps the returned rows so a single query_gold call can't flood the model's
context with an unbounded result set.
"""

from dataclasses import dataclass

import duckdb

from agent.sql_guard import validate_sql

DEFAULT_ROW_LIMIT = 200


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple]
    row_count: int
    truncated: bool


def run_query(connection: duckdb.DuckDBPyConnection, sql: str, row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult:
    validate_sql(sql)

    cursor = connection.execute(sql)
    fetched = cursor.fetchmany(row_limit + 1)
    truncated = len(fetched) > row_limit
    rows = fetched[:row_limit]
    columns = [d[0] for d in cursor.description]

    return QueryResult(columns=columns, rows=rows, row_count=len(rows), truncated=truncated)
