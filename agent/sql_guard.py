"""Read-only SQL guard for the query_gold tool.

Parses submitted SQL with sqlglot rather than matching keywords, so a
semicolon inside a string literal can't be mistaken for a statement
separator and a write disguised as an unfamiliar DuckDB statement (PRAGMA,
ATTACH, COPY, ...) can't slip through an incomplete keyword blocklist.
Only a single SELECT (or UNION/other set operation) is allowed through;
everything else raises QueryNotAllowedError.
"""

import sqlglot
from sqlglot import exp


class QueryNotAllowedError(Exception):
    """Raised when submitted SQL isn't a single read-only SELECT statement."""


def validate_sql(sql: str) -> None:
    if not sql or not sql.strip():
        raise QueryNotAllowedError("SQL statement is empty")

    try:
        statements = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
    except sqlglot.errors.ParseError as e:
        raise QueryNotAllowedError(f"could not parse SQL: {e}") from e

    if len(statements) != 1:
        raise QueryNotAllowedError(f"exactly one SELECT statement is allowed, got {len(statements)}")

    statement = statements[0]
    if not isinstance(statement, exp.Query):
        raise QueryNotAllowedError(f"only SELECT statements are allowed, got {type(statement).__name__}")
