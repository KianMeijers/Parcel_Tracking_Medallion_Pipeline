from pathlib import Path

SCHEMA_RESOURCE_PATH = Path(__file__).parent / "resources" / "gold_schema.md"


def load_schema_markdown() -> str:
    return SCHEMA_RESOURCE_PATH.read_text(encoding="utf-8")
