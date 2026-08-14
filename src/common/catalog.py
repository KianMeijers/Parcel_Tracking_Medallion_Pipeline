"""Local Iceberg catalog used across the bronze/silver/gold layers.

A single SQLite-backed catalog with warehouse root at data/ so that tables
created in the "bronze" namespace physically land under data/bronze/,
"silver" under data/silver/, and "gold" under data/gold/ - mirroring the
medallion layout that already exists on disk.
"""

from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog

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
