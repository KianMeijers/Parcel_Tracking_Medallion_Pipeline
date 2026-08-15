import json
from pathlib import Path

import pytest

from src.common.catalog import get_catalog
from src.gold.dimensions import build_carriers, build_organisations

CARRIERS = [
    {"carrier_code": "carrier_a", "carrier_name": "PostNL", "sla_hours": 24, "countries_served": ["NL", "DE"]},
    {"carrier_code": "carrier_b", "carrier_name": "DHL Parcel", "sla_hours": 48, "countries_served": ["NL", "DE", "FR"]},
]

ORGANISATIONS = [
    {"organisation_id": 4471, "name": "Shop 4471", "country": "NL", "plan": "growth", "created_at": "2026-01-23T00:00:00Z"},
]


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


@pytest.fixture
def catalog(tmp_path):
    return get_catalog(warehouse_dir=tmp_path / "warehouse")


@pytest.fixture
def reference_dir(tmp_path) -> Path:
    ref_dir = tmp_path / "reference"
    ref_dir.mkdir()
    _write_jsonl(ref_dir / "carriers.json", CARRIERS)
    _write_jsonl(ref_dir / "organisations.json", ORGANISATIONS)
    return ref_dir


def test_loads_carriers_with_sla_and_country_list(catalog, reference_dir):
    result = build_carriers(catalog, reference_dir=reference_dir)

    assert result.rows_loaded == 2
    rows = {r["carrier_code"]: r for r in catalog.load_table("gold.dim_carriers").scan().to_arrow().to_pylist()}
    assert rows["carrier_a"]["sla_hours"] == 24
    assert list(rows["carrier_b"]["countries_served"]) == ["NL", "DE", "FR"]


def test_loads_organisations(catalog, reference_dir):
    result = build_organisations(catalog, reference_dir=reference_dir)

    assert result.rows_loaded == 1
    rows = catalog.load_table("gold.dim_organisations").scan().to_arrow().to_pylist()
    assert rows[0]["organisation_id"] == 4471
    assert rows[0]["plan"] == "growth"


def test_rerunning_does_not_duplicate(catalog, reference_dir):
    build_carriers(catalog, reference_dir=reference_dir)
    build_carriers(catalog, reference_dir=reference_dir)

    rows = catalog.load_table("gold.dim_carriers").scan().to_arrow().to_pylist()
    assert len(rows) == 2
