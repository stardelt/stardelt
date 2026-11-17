# Retail medallion

A synthetic retail warehouse on Spark Connect: `retail_spark_ingest.ipynb` generates 1 500 000
orders plus 200 stores and 5 000 products and writes them to `retailref.reference` and
`retail.bronze`; `retail_gold.ipynb` builds the gold model — `retail.silver.orders` and the
`retail.gold.daily_sales`, `category_sales`, `store_sales` and `sales_kpis` tables — from that
bronze data.

It runs as the Pipeline `retail-medallion` (`pipeline.yaml`): two `notebook` tasks (`ingest`, then
`gold`), scheduled `0 5 * * *` in `Europe/Berlin`, as the ServicePrincipal
`pipeline-retail-analytics` (`runAs`) for the owner group `retail-analytics`.

The `objectVersion` values in `pipeline.yaml` are the versions the two notebooks had when they were
uploaded to the lab. Uploading the same file again through Nova stores a new version, so the
Pipeline has to be applied with that new version (or changed through the pipeline UI), never with
the literals below.

The demo is currently dark on the lab (platform#1713): `spark-connect-ingress` refuses pipeline
notebook pods by design, so the `ingest` task fails before the first write.

Provenance: exported 2026-11-17 from the lab's `notebooks` bucket at versions
`retail-analytics/retail_spark_ingest.ipynb` = `671ba4bb597f8ed679e5eacad127c706` and
`retail-analytics/retail_gold.ipynb` = `671ba4bb56954f973058859f85771261`; the notebook content is
unchanged from those objects.
