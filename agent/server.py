"""MCP stdio server exposing the gold layer as one read-only query tool.

Registered with Claude Code via .mcp.json. Views are registered once at
import time against the real (non-tmp_path) catalog, so the process picks
up whatever the pipeline last committed - restart the server after a fresh
pipeline run to see new data.
"""

from mcp.server import MCPServer

from agent.duckdb_views import GOLD_VIEWS, register_gold_views
from agent.query_service import DEFAULT_ROW_LIMIT, QueryResult, run_query
from agent.schema_resource import load_schema_markdown
from src.common.catalog import get_catalog

mcp = MCPServer("gold-query")

_catalog = get_catalog()
_connection = register_gold_views(_catalog)


def _format_result(result: QueryResult) -> str:
    if not result.columns:
        return "(no columns returned)"

    lines = ["\t".join(result.columns)]
    lines.extend("\t".join("" if v is None else str(v) for v in row) for row in result.rows)

    if result.truncated:
        lines.append(f"[truncated to first {len(result.rows)} rows - narrow with WHERE/GROUP BY/LIMIT to see more]")

    return "\n".join(lines)


@mcp.resource("gold://schema")
def gold_schema() -> str:
    """Column definitions, join keys, and semantic caveats for the gold views."""
    return load_schema_markdown()


_QUERY_GOLD_DESCRIPTION = f"""Run a single read-only SQL SELECT against the gold layer.

Queryable views: {", ".join(GOLD_VIEWS)} (shipments is one row per parcel).
Fetch the gold://schema resource first for column definitions and
important semantic caveats (in particular: "shipped" vs "accepted"
timestamps are different moments, and on-time/transit fields are
legitimately NULL for parcels never scanned by the carrier - don't drop
those rows from shipment counts).

Only a single SELECT statement is allowed - no DDL/DML, no multiple
statements. Results are capped at {DEFAULT_ROW_LIMIT} rows; a rejected
query raises an error describing why (retry with a corrected query rather
than giving up).
"""


@mcp.tool(description=_QUERY_GOLD_DESCRIPTION)
def query_gold(sql: str) -> str:
    result = run_query(_connection, sql)
    return _format_result(result)


if __name__ == "__main__":
    mcp.run()
