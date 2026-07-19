# IMDb Lakehouse to StarRocks Pipeline

PySpark ETL pipeline that ingests public IMDb datasets, builds a medallion lakehouse (bronze → silver → gold), loads gold Parquet into StarRocks, and benchmarks Spark SQL against StarRocks on the same analytics queries.

## Architecture

```
IMDb TSV (bronze)  →  Spark ETL (silver/gold Parquet)  →  StarRocks (OLAP)  →  benchmark
```


| Layer  | Location                  | Description                     |
| ------ | ------------------------- | ------------------------------- |
| Bronze | `data/bronze/imdb/`       | Raw TSV files                   |
| Silver | `data/lake/silver/`       | Cleaned per-table Parquet       |
| Gold   | `data/lake/gold/`         | Denormalized analytics tables   |
| OLAP   | StarRocks `imdb` database | Loaded gold tables for querying |


Gold tables: `titles_enriched`, `episodes_enriched`, `cast_credits`.

## Docker stack

`docker-compose.yml` runs three services on a shared `pipeline` network. Host `./data` is mounted at `/mnt/pipeline` in every container (StarRocks must not mount over `/data` — that path is used internally by the image).


| Service        | Image                               | Container      | Memory | Host ports                                        |
| -------------- | ----------------------------------- | -------------- | ------ | ------------------------------------------------- |
| `spark-master` | `apache/spark:3.5.8-java17-python3` | `spark-master` | 1 GB   | 7077 (cluster), 8080 (master UI), 4040 (Spark UI) |
| `spark-worker` | `apache/spark:3.5.8-java17-python3` | `spark-worker` | 2.5 GB | 8081 (worker UI)                                  |
| `starrocks`    | `starrocks/allin1-ubuntu`           | `starrocks`    | 4 GB   | 9030 (MySQL), 8030 (HTTP), 8040 (BE)              |


Spark worker settings: `SPARK_WORKER_MEMORY=2G`, `SPARK_WORKER_CORES=2`.

Project code is mounted at `/app` on Spark containers. `pymysql` is baked into the custom Spark image (`docker/Dockerfile.spark`) so `benchmark.py` can import it when run via `spark-submit`.

**StarRocks connection (from host):**


| Setting  | Value       |
| -------- | ----------- |
| Host     | `127.0.0.1` |
| Port     | `9030`      |
| User     | `root`      |
| Password | *(empty)*   |
| Database | `imdb`      |


From inside Docker (e.g. `benchmark.py` in `spark-master`), the host is `starrocks` on port `9030`.

## Prerequisites

- Docker Desktop with **12 GB RAM** allocated (Settings → Resources)
- Python 3.11+ on the host
- ~25 GB free disk for TSV + Parquet + StarRocks storage

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Quick start

```bash
# 1. Build images and start infrastructure
docker compose up -d --build

# 2. Download bronze data (Kaggle + IMDb episode file)
python download.py

# 3. Run ETL on the Spark cluster
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/etl_job.py

# 4. Load gold Parquet into StarRocks
python load_to_olap.py

# 5. Run Spark vs StarRocks benchmark
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/benchmark.py
```

You can also run `python benchmark.py` on the host if PySpark is installed and gold data exists under `data/lake/gold/`.

## Bronze data sources


| File                                                                               | Source                                                                                              |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `name.basics.tsv`, `title.basics.tsv`, `title.principals.tsv`, `title.ratings.tsv` | [Kaggle IMDb dataset](https://www.kaggle.com/datasets/ashirwadsangwan/imdb-dataset) via `kagglehub` |
| `title.episode.tsv`                                                                | [IMDb official](https://datasets.imdbws.com/title.episode.tsv.gz)                                   |


If download fails, place all five TSV files in `data/bronze/imdb/` and re-run `python download.py`. Kaggle may require `~/.kaggle/kaggle.json` if consent is enforced.

## Data model

**Ingested tables:** `title.basics`, `title.ratings`, `title.episode`, `title.principals`, `name.basics`.

**Excluded:** `title.akas`.

**Dropped columns:**

- `title.basics.originalTitle`
- `title.principals.ordering`, `title.principals.job`
- `name.basics.birthYear`, `deathYear`, `primaryProfession`, `knownForTitles`

**Derived fields on `title_basics`:** `start_decade`, `genres_array`, `primary_genre`.

### Partitioning


| Layer  | Table               | Partition columns           |
| ------ | ------------------- | --------------------------- |
| Silver | `title_basics`      | `titleType`, `start_decade` |
| Silver | `title_principals`  | `category`                  |
| Gold   | `titles_enriched`   | `titleType`, `start_decade` |
| Gold   | `episodes_enriched` | `start_decade`              |
| Gold   | `cast_credits`      | `category`                  |


All Parquet output uses Snappy compression.

## Benchmark

`benchmark.py` runs six analytics queries (defined in `[queries/analytics.sql](queries/analytics.sql)`) against both engines:


| Query | Description                                    |
| ----- | ---------------------------------------------- |
| Q1    | Top 20 movies by rating (min 10k votes)        |
| Q2    | Episodes per season for top 10 series by votes |
| Q3    | Average rating by primary genre and decade     |
| Q4    | Episode vs series average rating gap           |
| Q5    | Top 25 actors/actresses by credit count        |
| Q6    | Average runtime by title type and decade       |


### Methodology

- Each query runs **3 times** per engine; the reported value is the **median** latency in milliseconds.
- **Spark** reads gold Parquet from `/mnt/pipeline/lake/gold/` via `spark.read.parquet` and registers temp views.
- **StarRocks** runs the same SQL against loaded tables in the `imdb` database (Broker Load from gold Parquet).
- Speedup = Spark median ÷ StarRocks median.
- Results are written to `data/benchmark_results.txt` (overwritten each run). When run inside Docker, the file appears on the host via the `./data` volume mount.

### Sample results (local Docker, 16 GB host RAM)

Measured with the stack above: 1 Spark worker (2 cores, 2 GB), StarRocks 4 GB limit, full IMDb bronze dataset.

```
Query    Spark (ms)   StarRocks (ms)    Speedup
------------------------------------------------
Q1           3711.9            120.1      30.9x
Q2           6861.3            293.2      23.4x
Q3           8043.8            204.5      39.3x
Q4           2006.3            273.6       7.3x
Q5          23183.0           6510.8       3.6x
Q6           8102.3            108.9      74.4x
```

StarRocks is faster on these workloads because data is columnar, indexed with DUPLICATE KEY, and queried without distributed JVM planning overhead. Q5 is the heaviest query (~100M cast credits) and shows the smallest gap.

### Useful URLs while running


| URL                                            | Purpose                            |
| ---------------------------------------------- | ---------------------------------- |
| [http://localhost:8080](http://localhost:8080) | Spark master UI                    |
| [http://localhost:8081](http://localhost:8081) | Spark worker UI                    |
| [http://localhost:4040](http://localhost:4040) | Spark application UI (during jobs) |
| [http://localhost:8030](http://localhost:8030) | StarRocks HTTP API                 |




