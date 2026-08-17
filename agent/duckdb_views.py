"""Registers the gold Iceberg tables as queryable DuckDB views.

Only these three GOLD tables are ever loaded into the DuckDB connection. 
Bronze and silver are not registered, so there is no query path to them
regardless of what the SQL guard does or doesn't catch. 
"""

import duckdb
from pyiceberg.catalog import Catalog

GOLD_VIEWS: dict[str, str] = {
    "shipments": "gold.shipments",
    "dim_carriers": "gold.dim_carriers",
    "dim_organisations": "gold.dim_organisations",
}


def open_query_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:", config={"enable_external_access": False})


def register_gold_views(
    catalog: Catalog, connection: duckdb.DuckDBPyConnection | None = None
) -> duckdb.DuckDBPyConnection:
    con = connection or open_query_connection()
    for view_name, table_identifier in GOLD_VIEWS.items():
        if not catalog.table_exists(table_identifier):
            continue
        catalog.load_table(table_identifier).scan().to_duckdb(view_name, connection=con)
    return con
