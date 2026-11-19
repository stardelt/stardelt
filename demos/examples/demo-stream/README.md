# demo-stream

The demo's stream: a StreamApp group `demo` owns and the group `visitors` may read, so the public
Streams page shows a running app. `make demo-stream` from `platform/` prints the plan and changes
nothing; `make demo-stream YES=1` deploys the bundle (one on-demand pipeline `demo-stream-setup`
that creates the sink table, because a StreamApp reads its sink's schema and never creates it),
runs that pipeline once, and applies the StreamApp. The data lands in `gold.shop_dbt.pegelonline`,
fed by the hourly `pegelonline_hourly` DAG's topic `pegelonline.measurements`. It needs
`make demo-access YES=1` (the ServicePrincipal `pipeline-demo`) and `make demo-dbt-grant YES=1`
(the `modify` grant on the `gold` warehouse) first.
