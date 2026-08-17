import pytest

from agent.sql_guard import QueryNotAllowedError, validate_sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM shipments",
        "SELECT carrier_code, COUNT(*) FROM shipments WHERE organisation_id = 4471 GROUP BY carrier_code",
        "SELECT * FROM shipments ORDER BY created_at DESC LIMIT 10",
        "WITH recent AS (SELECT * FROM shipments) SELECT * FROM recent",
        "SELECT * FROM shipments s JOIN dim_carriers c ON s.carrier_code = c.carrier_code",
        "SELECT 1 UNION SELECT 2",
        "SELECT * FROM dim_organisations WHERE name = 'a;b'",
    ],
)
def test_allows_read_only_select_variants(sql):
    validate_sql(sql)  # does not raise


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM shipments; DROP TABLE shipments;",
        "DROP TABLE shipments",
        "INSERT INTO shipments VALUES (1)",
        "UPDATE shipments SET is_on_time = true",
        "DELETE FROM shipments",
        "CREATE TABLE evil AS SELECT 1",
        "ALTER TABLE shipments ADD COLUMN x INT",
        "PRAGMA database_list",
        "ATTACH 'evil.db' AS evil",
        "COPY shipments TO 'evil.csv'",
    ],
)
def test_rejects_non_select_statements(sql):
    with pytest.raises(QueryNotAllowedError):
        validate_sql(sql)


def test_rejects_multiple_select_statements():
    with pytest.raises(QueryNotAllowedError, match="exactly one SELECT statement"):
        validate_sql("SELECT 1; SELECT 2;")


@pytest.mark.parametrize("sql", ["", "   ", "\n\t"])
def test_rejects_empty_or_whitespace_only_sql(sql):
    with pytest.raises(QueryNotAllowedError, match="empty"):
        validate_sql(sql)


def test_rejects_unparseable_sql_with_a_readable_message():
    with pytest.raises(QueryNotAllowedError, match="could not parse SQL"):
        validate_sql("SELEKT * FROM shipments")
