# The shop: one small dbt project that exercises the whole platform

Little data, real compute. Git holds two small seed files (200 customers, 30 products, about 12 KB). The order lines are
**generated inside Trino** from the line number (default 200 000 lines, seconds on the dev node), so the example needs no download
and stores nothing large. Then it is an ordinary dbt data model, one layer per catalog:

| Layer | Catalog | What | Materialization |
|---|---|---|---|
| bronze | `bronze.shop_dbt` | the seeds `customers`, `products` and the generated `order_lines` | table |
| silver | `silver.shop_dbt` | `stg_customers`, `stg_products`, `stg_order_lines` (clean names, types) and `customers_snapshot` (history of segment and country) | view / snapshot |
| gold | `gold.shop_dbt` | `daily_revenue` (incremental merge), `order_lines_enriched` (incremental append by day), `customer_ltv` (window functions: rank, cumulative share, enforced contract), `product_mix` (share of revenue, rank in category) | table / incremental |

24 data tests: unique, not null, relationships, accepted values, the custom `is_positive` and two singular tests (revenue is
never negative; the units of the marts equal the units of the lines), plus one unit test.

## Check it without a cluster

```bash
python3 -m venv .venv && .venv/bin/pip install dbt-core dbt-trino
make check DBT=.venv/bin/dbt      # dbt parse: 8 models, 1 snapshot, 2 seeds, 24 tests, 1 unit test
make inventory DBT=.venv/bin/dbt  # every resource by name: 35 in all
```

## What the project shows

- **Snapshot** `customers_snapshot` — `check` strategy on `segment` and `country`: a changed customer gets a second version.
- **Incremental append** `order_lines_enriched` — each run adds only the days after the newest one already loaded.
- **Incremental merge** `daily_revenue` — each run recomputes the last three days and merges them on the day.
- **Contract** `customer_ltv` — `contract.enforced`, every column with its `data_type`, checked at build time.
- **Generic test** `is_positive` in `tests/generic/` — used on the revenue and line-revenue columns.
- **Singular tests** `revenue_is_not_negative` and `units_reconcile` — the marts agree with their inputs.
- **Unit test** `customer_ltv_ranks_by_value` — fixed input rows, the expected rank per customer, no cluster table needed.
- **Macro** `money` — one place that rounds money to cents and gives it the contract's `decimal(18, 2)`.
- **Persisted docs** — descriptions of the marts and their columns are written into the Trino catalog (the platform's catalog shows them).
- **Exposure** `shop_dashboard` — names the dashboard that reads the three marts.
- **Tags** `load` (bronze and staging) and `marts` (gold and the snapshot) — `dbt build --select tag:marts` runs one layer.

## Run it on the platform

In a JupyterHub terminal (the notebook image has the platform's Trino address and your token):

```bash
pip install dbt-core dbt-trino
export DBT_TRINO_HOST=trino.stardelt.svc.cluster.local DBT_TRINO_JWT="$TRINO_TOKEN"
make demo
```

A token lives five minutes: set it and run right away; the whole run must finish inside that time (at the default scale it does,
see below). `make demo SCALE=1000000 FULL=1` builds a million lines (`FULL=1` rebuilds the incremental `daily_revenue` as well: a later run
recomputes only the last three days, so change the scale only together with `FULL=1`). Then look at the results:

```sql
select * from gold.shop_dbt.daily_revenue order by order_date desc limit 7;
select * from gold.shop_dbt.customer_ltv order by value_rank limit 10;
select * from gold.shop_dbt.product_mix order by revenue desc limit 10;
```

Run `make demo` a second time: `daily_revenue` is merged, not rebuilt. `make teardown` drops everything the example created
(`bronze.shop_dbt`, `silver.shop_dbt`, `gold.shop_dbt`).

## Notes

- The catalogs `bronze`, `silver` and `gold` and the right to write to them come from the platform; a person outside the group that
  may write sees `Access Denied: Cannot access catalog gold`.
- `sequence` in Trino is limited to 10 000 entries, so the generator builds `scale` lines as 1000 blocks of 1000; `scale` is a
  multiple of 1000 and the model says so when it is not.
- The same project becomes a scheduled pipeline (a `dbt` task) and a bundle with `dev`, `test` and `prod` targets in the later
  slices of stardelt-demos#31.
- **Status:** the SQL parses as Trino SQL and the project parses in dbt, but nobody has run it against the live cluster yet. The first
  live run is the check; a failure is a bug in this example, file it.

Without a terminal on the cluster: `examples/shop-bundle` is the same shop as a bundle of three SQL pipelines, one per layer,
published and run with `stardelt bundle deploy` and `stardelt bundle run`.
