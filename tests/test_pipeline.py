import csv
import datetime as dt
import json
from decimal import Decimal

from pyspark.sql import functions as F

from conftest import RUN_DATE, load_notebook


# ---------------------------------------------------------------- parsing

def test_parse_number_handles_danish_and_english_formats(spark, lib):
    df = spark.createDataFrame([("1234,50",), ("1.234,50",), ("1234.50",), ("-399,00",), ("",), (None,)], "v string")
    got = [r[0] for r in df.select(lib["parse_number"]("v")).collect()]
    assert got == [Decimal("1234.50"), Decimal("1234.50"), Decimal("1234.50"), Decimal("-399.00"), None, None]


# ---------------------------------------------------------------- generator

def test_generator_is_reproducible(gen):
    day = dt.date(2025, 11, 28)
    assert gen["pos_lines_for_day"](day) == gen["pos_lines_for_day"](day)
    assert gen["web_export_for_day"](day) == gen["web_export_for_day"](day)


def test_next_day_matches_backfill(tmp_path):
    gen = load_notebook("00_generate_source_data")  # own copy, so PLANTED counters stay untouched
    root = tmp_path / "landing"
    gen["backfill"](str(root), dt.date(2024, 6, 30))
    day = gen["next_day"](str(root))
    assert day == dt.date(2024, 7, 1)
    with open(root / "pos" / "daily" / "pos_2024-07-01.csv", encoding="utf-8") as f:
        daily = list(csv.reader(f, delimiter=";"))[1:]
    assert daily == gen["pos_lines_for_day"](day)


# ---------------------------------------------------------------- silver

def test_planted_problems_end_up_in_quarantine_with_the_right_reason(silver, gen):
    got = {r["dq_reason"]: r["count"] for r in silver["quarantine"].groupBy("dq_reason").count().collect()}
    planted = gen["PLANTED"]
    assert got == {
        "unknown_store": planted["unknown_store"],
        "unknown_sku": planted["unknown_sku"],
        "zero_quantity": planted["zero_quantity"],
        "missing_price": planted["missing_price"],
        "before_store_opened": planted["before_store_opened"],
    }


def test_resent_file_and_status_updates_are_deduplicated(silver, landing):
    with open(landing / "pos" / "pos_2025-03_resend.csv", encoding="utf-8") as f:
        resend_lines = sum(1 for _ in f) - 1
    updates = 0
    for path in (landing / "webshop").glob("*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            o = json.loads(line)
            if o["exported_at"].endswith("T06:00:00Z"):
                updates += len(o["lines"])
    removed = silver["incoming"].count() - silver["deduped"].count()
    assert removed == resend_lines + updates


def test_store_codes_are_cleaned(silver, gen):
    assert silver["incoming"].where("_fixed_store_code").count() == gen["PLANTED"]["store_code_format"]
    assert silver["valid"].where(F.col("store_code") != F.upper(F.trim("store_code"))).count() == 0


def test_newest_webshop_export_wins(silver, gen):
    # find an order that was exported as pending and later cancelled
    day = dt.date(2025, 11, 10)
    order = next(o for o in gen["web_orders_created"](day) if o["pending_first"] and o["final_status"] == "cancelled")
    statuses = {r["order_status"] for r in silver["valid"].where(F.col("order_id") == order["order_id"]).collect()}
    assert statuses == {"cancelled"}


def test_exchange_rates_are_carried_forward(silver):
    fx = {(r["rate_date"], r["currency"]): (r["dkk_per_unit"], r["rate_source"]) for r in silver["fx"].collect()}
    gbp_23 = fx[(dt.date(2025, 12, 23), "GBP")]
    assert fx[(dt.date(2025, 12, 26), "GBP")] == (gbp_23[0], "carried forward")   # planted holiday gap
    sat, fri = dt.date(2026, 3, 7), dt.date(2026, 3, 6)
    assert fx[(sat, "EUR")] == (fx[(fri, "EUR")][0], "carried forward")
    assert fx[(sat, "DKK")][0] == Decimal("1.000000")


def test_amounts_are_converted_to_the_ore(silver):
    rows = silver["valid"].where("discount_pct > 0 AND currency = 'EUR'").limit(50).collect()
    assert rows
    for r in rows:
        net_local = (r["quantity"] * r["unit_price_local"] * (1 - r["discount_pct"] / 100)).quantize(Decimal("0.01"))
        assert r["net_amount_local"] == net_local
        assert r["net_amount_dkk"] == (net_local * r["fx_rate_to_dkk"]).quantize(Decimal("0.01"))


def test_every_line_is_loaded_or_quarantined(silver):
    loaded = silver["valid"].select("sales_line_id")
    held = silver["quarantine"].select("sales_line_id")
    assert loaded.join(held, "sales_line_id").count() == 0
    assert silver["deduped"].count() == loaded.count() + held.count()


# ---------------------------------------------------------------- gold

def test_fiscal_calendar(gold):
    d = {r["date"]: r for r in gold["dim_date"].collect()}
    assert d[dt.date(2024, 6, 1)]["fiscal_year"] == "FY24/25"
    assert d[dt.date(2025, 5, 31)]["fiscal_year"] == "FY24/25"
    assert d[dt.date(2025, 6, 1)]["fiscal_year"] == "FY25/26"
    assert d[dt.date(2025, 6, 1)]["fiscal_month_no"] == 1
    assert d[dt.date(2026, 5, 15)]["fiscal_month_no"] == 12


def test_fact_sales_has_only_completed_sales(silver, gold):
    completed = silver["valid"].where("order_status = 'completed'").count()
    assert gold["fact_sales"].count() == completed
    assert silver["valid"].where("order_status = 'cancelled'").count() > 0


def test_gross_margin_and_targets_are_realistic(gold):
    f = gold["fact_sales"].agg(F.sum("net_revenue_dkk").alias("rev"), F.sum("gross_profit_dkk").alias("gp")).first()
    margin = float(f["gp"] / f["rev"])
    assert 0.50 < margin < 0.62
    fy = gold["fact_sales"].where("date_key BETWEEN 20250601 AND 20260531").agg(F.sum("net_revenue_dkk")).first()[0]
    target = gold["fact_target"].where("date_key BETWEEN 20250601 AND 20260531").agg(F.sum("target_dkk")).first()[0]
    assert 0.8 < float(fy / target) < 1.2


# ---------------------------------------------------------------- data quality gate

def test_all_blocking_rules_pass_on_a_clean_run(lib, dq_tables):
    results = lib["dq_rules"](dq_tables, RUN_DATE)
    failed = [r for r in results if r["severity"] == "blocking" and r["status"] == "fail"]
    assert failed == []
    assert {r["rule_id"] for r in results} == {f"DQ{i:02d}" for i in range(1, 14)}


def test_gate_catches_a_missing_line(lib, dq_tables):
    one = dq_tables["silver_sales"].select("sales_line_id").first()[0]
    t = dict(dq_tables, silver_sales=dq_tables["silver_sales"].where(F.col("sales_line_id") != one))
    failed = {r["rule_id"] for r in lib["dq_rules"](t, RUN_DATE) if r["status"] == "fail"}
    assert "DQ03" in failed


def test_gate_catches_revenue_that_does_not_reconcile(lib, dq_tables):
    fact = dq_tables["fact_sales"]
    first = fact.select("sales_line_id").first()[0]
    bumped = fact.withColumn("net_revenue_dkk",
                             F.when(F.col("sales_line_id") == first, F.col("net_revenue_dkk") + F.lit(0.01)).otherwise(F.col("net_revenue_dkk")))
    res = {r["rule_id"]: r for r in lib["dq_rules"](dict(dq_tables, fact_sales=bumped), RUN_DATE)}
    assert res["DQ09"]["status"] == "fail" and res["DQ09"]["failed_rows"] == 1


def test_gate_catches_an_orphan_key(lib, dq_tables):
    stores = dq_tables["dim_store"].where("store_code != 'CPH01'")
    res = {r["rule_id"]: r for r in lib["dq_rules"](dict(dq_tables, dim_store=stores), RUN_DATE)}
    assert res["DQ05"]["status"] == "fail" and res["DQ05"]["failed_rows"] > 0


def test_freshness_warning_when_data_is_old(lib, dq_tables):
    res = {r["rule_id"]: r for r in lib["dq_rules"](dq_tables, "2026-10-25")}
    assert res["DQ12"]["status"] == "fail" and res["DQ12"]["severity"] == "warning"
