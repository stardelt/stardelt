# stardelt — the notebook library

The helper that sits between a notebook and the platform's warehouse. A notebook has the
native names already: `spark`, `display` and `%sql`.

```python
df = spark.table("warehouse.demo.trips")            # spark is already there, as you
df.groupBy("day").count().show()
spark.createDataFrame(pdf).writeTo("warehouse.demo.trips_sample").createOrReplace()
```

```text
%sql SELECT * FROM warehouse.demo.trips LIMIT 10
```

```python
import stardelt
key = stardelt.connection("weather-api")          # what the platform adds: secrets, publish, compute
```

`stardelt.sql`, `stardelt.spark` and `stardelt.write_df` are deprecated in 0.4 and removed in 0.5.

The API reference is in [`stardelt/README.md`](stardelt/README.md) — it ships with the
package, so `help(stardelt.spark)` points at the same text.

## What it talks to

| | version | why it matters |
|---|---|---|
| Spark Connect **client** (`pyspark-client`) | same Spark line as the server: `4.1.x` | a newer client mis-parses the server's settings (`3221225472b`) and every `createDataFrame` fails with an opaque `int()` error |
| Trino (the `trino` client) | 0.340 | the `sql()` and `write_df()` paths |
| pandas | 2.2.x | the frames the notebook hands over |

`stardelt.spark()` refuses to work when the client and the server are on different Spark
lines, and says so in one sentence naming both versions.

## Install

The platform's notebook image installs a **pinned** release of this library; a plain
notebook can install the same pin:

```bash
pip install "stardelt @ https://github.com/stardelt/stardelt/releases/download/v0.0.4/stardelt-0.0.4-py3-none-any.whl"
```

## Container image

`ghcr.io/stardelt/stardelt-cli:dev` carries the `stardelt` command, and nothing else, for
jobs that run outside a notebook (the platform's table-maintenance CronJob):

```bash
docker run --rm ghcr.io/stardelt/stardelt-cli:dev maintenance run --help
```

## On a laptop

A local kernel (VS Code, Jupyter) reaches the warehouse through Nova's statement API with
the sign-in you store once — no workspace needed:

```bash
stardelt login --host https://<nova>
```

```python
import stardelt

stardelt.sql("SELECT * FROM warehouse.demo.trips LIMIT 10")   # up to 100 000 rows
```

`stardelt.spark()` and `stardelt.write_df()` need the platform: connect VS Code to your
workspace (Nova → Profile → VS Code).

## Releases

The library is released with the platform: one version for the repository, tag `v0.0.N`
(stardelt/stardelt#2). Each GitHub release carries the wheel and the sdist. Version numbers
are `0.0.x` until `0.1.0`; `0.0.4` follows `0.4.3`, so upgrade an older install with
`pip install --force-reinstall`.

## Development

```bash
pip install -e ".[test]"
python -m pytest tests -q
```

The tests need no cluster: the Spark and Trino surfaces are faked.
