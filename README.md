# Traffic Prediction with Spark

A batch pipeline in Scala and Apache Spark that forecasts urban traffic speed for the next 30 minutes, for every road segment of a city.

It reads speed, volume and static road features from Parquet files, builds time-series features with Spark window functions, trains two Random Forest models (speed and volume) with MLlib, and then forecasts the next six 5-minute steps for each segment.

Built for a university Big Data Processing course (Part I: batch processing with Spark).

## Data

The pipeline expects three Parquet files in the layout of the *city-M* traffic benchmark ([paper](https://arxiv.org/abs/2510.02278), [data on Kaggle](https://www.kaggle.com/datasets/mightyneghbor/city-traffic-benchmarks)):

| File | Layout |
| --- | --- |
| speed | a `timestamp` column (5-minute steps) and one `node_$n` column per road segment, values may be null |
| volume | same layout as speed |
| static features | one row per road segment, row `n` (1-based) describes `node_$n`, numeric columns only |

The subsets used for the course are not included. The benchmark on Kaggle is distributed as `.npz` files, so it has to be converted to this layout first.

To try the pipeline without the real data, generate a synthetic dataset with the same layout:

```sh
uv run scripts/make_sample_data.py --out dataset
```

Options: `--measurements` (default 2000), `--segments` (default 100), `--total-nodes` (default 500), `--null-rate`, `--seed`.

## Build and run

Requirements: Java 17 or 21 and [sbt](https://www.scala-sbt.org/). Spark is pulled in by sbt.

```sh
sbt "run dataset/city_M_speed_2000_100.parquet dataset/city_M_volume_2000_100.parquet dataset/city_M_static_features.parquet"
```

Arguments:

```
Main <speed.parquet> <volume.parquet> <static.parquet> [models-dir]
```

`models-dir` defaults to `models/`. Without a `spark.master` setting the job runs with `local[*]`.

### On a Spark cluster (Docker)

`docker-compose.yml` starts a Spark 4.0 master, one worker and a dev container with sbt, Spark and uv. It is also set up as a VS Code dev container.

```sh
docker compose up -d --build
docker compose exec dev bash

# inside the dev container
uv run scripts/make_sample_data.py --out dataset
sbt assembly
spark-submit --master spark://spark-master:7077 --class solution.Main \
  target/scala-2.13/traffic-prediction.jar \
  dataset/city_M_speed_2000_100.parquet dataset/city_M_volume_2000_100.parquet \
  dataset/city_M_static_features.parquet
```

The Spark master UI is on http://localhost:8080.

## Output

Progress and test metrics first, then a line of `=` followed by one line per 5-minute step. Each line holds the predicted speeds of all segments, sorted by node id. Run on the synthetic dataset:

```
Starting Spark job on local[*]
Loading data...loaded 100 nodes.
Preparing training data...done.
Training model...done.
Speed RMSE on test data:  2.25 (last value baseline: 2.21)
Volume RMSE on test data: 0.80 (last value baseline: 0.72)
Saving model...done (models/).
Predictions for next 30 minutes:
========
28.62,43.30,79.90,41.39,88.16,25.84,40.32,27.73,61.90,68.35, ...
28.21,42.89,81.25,41.57,88.16,25.69,41.02,27.63,62.34,66.38, ...
...
```

The trained models are saved with `model.write.overwrite().save(...)` as `models/speed_output.model` and `models/volume_output.model`.

## How it works

1. **Unpivot.** The wide speed and volume tables (one column per segment) are turned into a long table `(timestamp, node_id, value)` with `stack`, null measurements are dropped, and the two are joined on `timestamp` and `node_id`.
2. **Static features.** Each static row gets its 1-based row number as `node_id` (`zipWithIndex`, which keeps the file order) and is joined with the measurements.
3. **Time-series features.** With a window partitioned by `node_id` and ordered by `timestamp`:
   - lag features: speed and volume at `t-1` and `t-2`,
   - rolling averages of speed and volume over the previous 30 minutes (6 steps, the current step excluded),
   - minute of the day.

   Only past values are used, so the targets never leak into the features.
4. **Models.** The features are assembled into a vector and two `RandomForestRegressor`s (50 trees) are trained, one for speed and one for volume. The split follows time: the first 70% of the timeline is used for training, the rest for testing. The RMSE is reported next to a "last value" baseline.
5. **Forecast.** Starting from the last 6 measurements of each segment, both models predict the next step. That prediction becomes the newest history row and the process repeats 6 times (30 minutes). Volume is forecast too because the speed model uses volume lags.

The intermediate DataFrames that are used several times are persisted, and the forecast loop uses `localCheckpoint` to keep the lineage short.

### Limitations

- Segments with fewer than 6 recent measurements are left out of the forecast.
- Lags follow rows, not timestamps: when a measurement is missing, `t-1` is the previous available one.
- Errors add up in the recursive forecast, so later steps are less accurate.

## Project structure

```
.
├── build.sbt                         # sbt build (Spark 4.0, Scala 2.13, sbt-assembly)
├── project/                          # sbt version and plugins
├── src/main/scala/solution/Main.scala
├── src/main/resources/log4j2.properties
├── scripts/make_sample_data.py       # synthetic dataset generator
├── docker-compose.yml                # Spark master, worker and dev container
└── .devcontainer/                    # dev container image and VS Code settings
```
