"""Local Iceberg catalog used across the bronze/silver/gold layers.

A single SQLite-backed catalog with warehouse root at data/ so that tables
created in the "bronze" namespace physically land under data/bronze/,
"silver" under data/silver/, and "gold" under data/gold/ - mirroring the
medallion layout that already exists on disk.
"""

from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"


def get_catalog(warehouse_dir: Path | None = None) -> SqlCatalog:
    """Build the local Iceberg catalog. Tests pass warehouse_dir=tmp_path to
    get an isolated warehouse instead of touching the real data/ directory.
    """
    warehouse_dir = warehouse_dir or DATA_DIR
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    catalog_db = warehouse_dir / "catalog.db"
    return SqlCatalog(
        "local",
        **{
            # pyiceberg's URI parser mis-splits Windows drive letters (e.g.
            # "C:/..." or "file:///C:/...") into scheme "c" or a path with a
            # stray leading slash. "file:C:/..." (single colon, no slashes)
            # is the one form it parses back into a bare "C:/..." path.
            "uri": f"sqlite:///{catalog_db.as_posix()}",
            "warehouse": f"file:{warehouse_dir.as_posix()}",
        },
    )


def ensure_namespace(catalog: SqlCatalog, namespace: str) -> None:
    if namespace not in {ns[0] for ns in catalog.list_namespaces()}:
        catalog.create_namespace(namespace)


def get_or_create_table(catalog: Catalog, identifier: str, schema: Schema, partition_spec: PartitionSpec) -> Table:
    namespace = identifier.split(".")[0]
    ensure_namespace(catalog, namespace)
    if catalog.table_exists(identifier):
        return catalog.load_table(identifier)
    return catalog.create_table(identifier, schema=schema, partition_spec=partition_spec)


def overwrite_table(table: Table, rows: list[dict], overwrite_filter: str | None = None) -> None:
    """Write `rows` into `table`, replacing whatever the filter matches (or
    the whole table, if no filter is given). Builds the Arrow table from the
    table's own committed schema rather than a module-level Schema constant,
    so struct/list fields convert the way Iceberg actually stored them.
    """
    arrow_table = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    if overwrite_filter is None:
        table.overwrite(arrow_table)
    else:
        table.overwrite(arrow_table, overwrite_filter=overwrite_filter)
