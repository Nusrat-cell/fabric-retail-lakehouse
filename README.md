# Retail lakehouse on Microsoft Fabric

A medallion lakehouse for Nordklang, a fictional premium audio brand with 15 stores and two
webshops. Raw exports from the tills and the webshop land in OneLake. PySpark notebooks turn
them into bronze, silver and gold Delta tables, a data quality gate decides if the numbers can
be trusted, and a Direct Lake semantic model sits on top for Power BI.

I built it to learn Fabric by doing the whole path once, from messy files to a model a finance
team could use. All data is synthetic.

```
landing files            bronze (as text)        silver (typed, trusted)       gold (star schema)
POS csv (; , decimal)  -> pos_sales_raw      \
webshop jsonl          -> web_orders_raw      >-> sales_lines (MERGE)   ->   fact_sales, fact_target
fx, products, stores,  -> *_raw              /    sales_quarantine           dim_date, dim_store,
targets                                           products, stores, fx       dim_city, dim_product
                                                                                    |
                                    04 data quality gate (fails the pipeline)       v
                                                                         Direct Lake model "Nordklang Sales"
```

Everything runs from one pipeline, `pl_daily_load`: simulate the next day, bronze, silver,
gold, quality gate. The pipeline itself is created from code (`tools/create_pipeline.py`).

## Notebooks

| Notebook | What it does |
|---|---|
| `00_generate_source_data` | Writes the source files. Each day has its own seed, so a backfill and a day by day run give identical data. Problems are planted on purpose. |
| `01_bronze_ingest` | Loads only new files (path and modified time checked against `meta.ingestion_log`) as text, with source file, batch id and load time on every row. |
| `02_silver_transform` | Cleans and types the data, picks one FX rate per currency per day, quarantines bad lines with a reason, and writes with Delta MERGE using a watermark. |
| `03_gold_model` | Builds the star schema. Keys are hashes of the business key, so they stay the same between runs. Fiscal year runs June to May. |
| `04_data_quality_gate` | Runs blocking rules and warnings. A failed blocking rule raises an error and stops the pipeline. |
| `05_semantic_model` | Creates the Direct Lake model, relationships, date table and DAX measures as code with Semantic Link Labs. |
| `nb_lib_retail` | The transformation functions. They take and return DataFrames and touch no tables, so they are tested locally. |

## The planted problems

The generator adds the kind of faults real feeds have, and the pipeline has to deal with each:

| Problem | What happens |
|---|---|
| The same file sent twice | MERGE on `sales_line_id`, nothing changes |
| Webshop order exported again when its status changes | Newest export wins, the line is updated in place |
| Store code in a different format | Cleaned in silver |
| Unknown store or unknown SKU | Quarantine, with the reason |
| Quantity zero, missing price | Quarantine |
| Sale dated before the store opened | Quarantine |
| No FX rate on weekends or holidays, and a gap in GBP | Last published rate carried forward, and the row says it was carried |
| Product master with a different number format | Parsed in one function that reads Danish and English formats |

Every line is either loaded or quarantined. A test checks that the two add up to the source.

## Tests

18 pytest tests run the same functions on local PySpark against a full synthetic history, about
a minute in total. They cover the number parsing, reproducible data, each planted problem
ending in quarantine with the right reason, idempotent re-sends, FX carry forward, amounts to
the øre, the fiscal calendar, margin and targets in a realistic range, and the quality gate
(it must pass on a clean run and fail on a missing line, on revenue that does not reconcile
and on an orphan key). GitHub Actions runs them on every push.

```bash
pip install -r requirements-dev.txt     # needs Java 17
cd tests && python -m pytest -q
```

## Deploy

`tools/deploy_to_fabric.py` creates or updates the notebooks in a workspace from this repo, and
`tools/create_pipeline.py` creates `pl_daily_load` (and runs it when `RUN` is set).

## What I ran in Fabric

Workspace "Nordklang Retail Analytics" on a Fabric trial capacity.

* `pl_daily_load` ran to the end and succeeded: bronze, silver, gold and the quality gate.
* The `Nordklang Sales` semantic model was created by `05_semantic_model`.
* The synthetic history gives about DKK 210 M revenue in fiscal year 2025/26 at a gross margin
  of about 57.7 percent. These numbers come from the generator, they are not real.

Not done yet: a Power BI report on top of the model. The model is built, the report is the
next step.

## What worked

* **Keeping the logic in plain functions.** Because `nb_lib_retail` never reads a table, I
  could test everything locally and only debug Fabric problems in Fabric.
* **A quarantine table instead of dropping rows.** Whenever a number looked wrong, the answer
  was a query on `silver.sales_quarantine`.
* **MERGE plus a watermark.** Re-running a day, or sending a file twice, changes nothing.
* **A gate that fails the run.** It is easier to fix a red pipeline than to find out later that
  a report was wrong.
* **Defining the model as code.** The semantic model can be rebuilt in another workspace.

## What did not work

* **Listing the landing folder with Spark.** The first bronze version used a recursive Hadoop
  file listing. On OneLake it failed, and two bronze runs went red. `notebookutils.fs.ls`
  walking the folders works, and that is what `01_bronze_ingest` uses now.
* **Deploying a second time.** The deploy script got a 409 `ItemDisplayNameAlreadyInUse` for
  notebooks that already existed. It now updates the notebook when the name is taken.
* **Installing PySpark locally.** A plain `pip install` of PySpark did not build in my
  environment. A clean virtual environment with a pinned version (see
  `requirements-dev.txt`) fixed it.
* **Clicking things together.** My first attempts to set things up in the browser were slow and
  fragile. Putting the pipeline and the semantic model in code was faster and repeatable.

## Next steps

* Power BI report: revenue against target by city, margin by category, and a page that shows
  the quality gate results and the quarantine reasons.
* Incremental refresh of the gold facts instead of a full rebuild.
* Separate test and production workspaces, deployed from the same repo.
