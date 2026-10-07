"""Static checks on the SQL files: documented, pointing at the right tables, safe to publish."""
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANALYTICS = sorted((ROOT / "sql").glob("0[1-9]_*.sql"))


def test_all_nine_analytics_queries_exist():
    assert len(ANALYTICS) == 9
    assert [p.name[:2] for p in ANALYTICS] == [f"0{i}" for i in range(1, 10)]


@pytest.mark.parametrize("path", ANALYTICS, ids=lambda p: p.name)
def test_analytics_query_is_documented_and_targets_the_catalog(path):
    text = path.read_text(encoding="utf-8")
    assert text.lstrip().startswith("-- Business question:"), "first line must state the business question"
    assert "finsight_main_database." in text
    assert re.search(r"\bselect\b", text, re.IGNORECASE)
    assert "YOUR-BUCKET" not in text


def test_setup_script_defines_every_table_and_uses_a_placeholder_bucket():
    text = (ROOT / "sql" / "00_setup_tables.sql").read_text(encoding="utf-8")
    for table in ("fact_transactions", "monthly_spend_by_customer", "category_summary_monthly",
                  "account_summary", "portfolio_summary"):
        assert f"CREATE EXTERNAL TABLE finsight_main_database.{table}" in text
    assert "CREATE TABLE finsight_main_database.fact_transactions_flat" in text      # the baseline
    assert text.count("MSCK REPAIR TABLE") >= 3                                       # partitioned tables
    assert "YOUR-BUCKET" in text and "finsight-de-proj-data" not in text             # nothing account-specific


def test_benchmark_pairs_compare_flat_against_partitioned():
    text = (ROOT / "benchmarks" / "athena_benchmark_queries.sql").read_text(encoding="utf-8")
    blocks = re.split(r"(?m)^-- (Q\d \w+)\s*$", text)
    labelled = {blocks[i]: blocks[i + 1] for i in range(1, len(blocks) - 1, 2)}
    for q in ("Q1", "Q2", "Q3"):
        baseline, optimized = labelled[f"{q} baseline"], labelled[f"{q} optimized"]
        assert "fact_transactions_flat" in baseline and "txn_date" in baseline
        assert "fact_transactions_flat" not in optimized
        assert re.search(r"\byear\s*=", optimized) and "month" in optimized
    assert "monthly_spend_by_customer" in text                                        # Q4, the aggregate table
