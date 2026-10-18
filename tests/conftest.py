"""Runs the notebook code locally with plain PySpark.

The notebooks are loaded from notebooks/*.ipynb and their code cells are executed, except
magic commands (%run, %pip) and the cell marked '# RUN' that writes files in Fabric.
"""
import datetime as dt
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
HISTORY_END = dt.date(2026, 10, 17)
RUN_DATE = "2026-10-18"


def load_notebook(name, **params):
    nb = json.loads((ROOT / "notebooks" / f"{name}.ipynb").read_text(encoding="utf-8"))
    ns = {}
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        if src.lstrip().startswith("%") or src.lstrip().startswith("# RUN"):
            continue
        exec(compile(src, f"{name}.ipynb", "exec"), ns)
        if "parameters" in cell["metadata"].get("tags", []):
            ns.update(params)
    return ns


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("retail-lakehouse-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def gen():
    return load_notebook("00_generate_source_data")


@pytest.fixture(scope="session")
def landing(gen, tmp_path_factory):
    root = tmp_path_factory.mktemp("lakehouse") / "landing"
    gen["backfill"](str(root), HISTORY_END)
    return root


@pytest.fixture(scope="session")
def lib(spark):
    return load_notebook("nb_lib_retail")


@pytest.fixture(scope="session")
def bronze(spark, lib, landing):
    """Reads landing the same way 01_bronze_ingest does: everything as text, plus lineage."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    def lineage(df):
        return (df.withColumn("_source_file", F.col("_metadata.file_path"))
                  .withColumn("_ingested_at", F.lit("2026-10-18 05:00:00").cast("timestamp"))
                  .withColumn("_batch_id", F.lit("test")))

    def csv(path, cols, sep=","):
        schema = T.StructType([T.StructField(c, T.StringType()) for c in cols])
        return lineage(spark.read.option("header", True).option("sep", sep).schema(schema).csv(str(path)))

    return {
        "pos": csv(landing / "pos" / "*.csv", lib["POS_COLUMNS"], ";").cache(),
        "web": lineage(spark.read.schema(lib["WEB_SCHEMA"]).json(str(landing / "webshop" / "*.jsonl"))).cache(),
        "products": csv(landing / "master/products/products.csv",
                        ["sku", "product_name", "category", "subcategory", "list_price_dkk", "unit_cost_dkk", "launch_date", "discontinued_date"]),
        "stores": csv(landing / "master/stores/stores.csv",
                      ["store_code", "store_name", "city", "country", "region", "channel", "currency", "opened_on", "closed_on"]),
        "fx": csv(landing / "master/fx_rates/fx_rates.csv", ["rate_date", "currency", "dkk_per_unit"]),
        "targets": csv(landing / "master/sales_targets/sales_targets.csv", ["month", "city", "target_dkk"]),
    }


@pytest.fixture(scope="session")
def silver(spark, lib, bronze):
    products = lib["standardize_products"](bronze["products"]).cache()
    stores = lib["standardize_stores"](bronze["stores"]).cache()
    targets = lib["standardize_targets"](bronze["targets"]).cache()
    fx = lib["fx_daily"](bronze["fx"], dt.date(2024, 5, 20), dt.date.fromisoformat(RUN_DATE)).cache()
    valid, quarantine, incoming, deduped = lib["silver_sales_from_bronze"](
        bronze["pos"], bronze["web"], stores, products, fx, RUN_DATE)
    return dict(products=products, stores=stores, targets=targets, fx=fx, valid=valid.cache(),
                quarantine=quarantine.cache(), incoming=incoming.cache(), deduped=deduped.cache())


@pytest.fixture(scope="session")
def gold(spark, lib, silver):
    return {
        "dim_date": lib["build_dim_date"](spark, dt.date(2024, 6, 1), dt.date(2027, 5, 31)).cache(),
        "dim_city": lib["build_dim_city"](silver["stores"]).cache(),
        "dim_store": lib["build_dim_store"](silver["stores"], RUN_DATE).cache(),
        "dim_product": lib["build_dim_product"](silver["products"], RUN_DATE).cache(),
        "fact_sales": lib["build_fact_sales"](silver["valid"], silver["products"]).cache(),
        "fact_target": lib["build_fact_target"](silver["targets"]).cache(),
    }


@pytest.fixture(scope="session")
def dq_tables(bronze, silver, gold):
    return {
        "bronze_pos": bronze["pos"], "bronze_web": bronze["web"],
        "silver_sales": silver["valid"], "quarantine": silver["quarantine"],
        **{k: v for k, v in gold.items()},
    }
