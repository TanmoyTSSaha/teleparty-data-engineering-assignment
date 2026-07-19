"""Benchmark Spark SQL vs StarRocks for analytics queries Q1-Q6."""

from __future__ import annotations

import logging
import os
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pymysql
from pyspark.sql import SparkSession

import config

logger = logging.getLogger(__name__)

BENCHMARK_QUERIES = {
    "Q1": """
        SELECT primaryTitle, averageRating, numVotes, startYear
        FROM titles_enriched
        WHERE titleType = 'movie' AND numVotes >= 10000
        ORDER BY averageRating DESC, numVotes DESC
        LIMIT 20
    """,
    "Q2": """
        WITH top_series AS (
            SELECT parentTconst
            FROM episodes_enriched
            GROUP BY parentTconst
            ORDER BY MAX(seriesNumVotes) DESC
            LIMIT 10
        )
        SELECT e.seriesTitle, e.seasonNumber, COUNT(*) AS episode_count
        FROM episodes_enriched e
        INNER JOIN top_series t ON e.parentTconst = t.parentTconst
        GROUP BY e.seriesTitle, e.seasonNumber
        ORDER BY e.seriesTitle, e.seasonNumber
    """,
    "Q3": """
        SELECT primary_genre, start_decade,
               AVG(averageRating) AS avg_rating,
               COUNT(*) AS title_count
        FROM titles_enriched
        WHERE primary_genre IS NOT NULL AND averageRating IS NOT NULL
        GROUP BY primary_genre, start_decade
        ORDER BY start_decade, avg_rating DESC
    """,
    "Q4": """
        SELECT seriesTitle,
               AVG(episodeAverageRating) AS avg_episode_rating,
               AVG(seriesAverageRating) AS avg_series_rating,
               AVG(episodeAverageRating) - AVG(seriesAverageRating) AS rating_gap
        FROM episodes_enriched
        WHERE episodeAverageRating IS NOT NULL
          AND seriesAverageRating IS NOT NULL
        GROUP BY seriesTitle
        HAVING COUNT(*) >= 10
        ORDER BY rating_gap DESC
        LIMIT 20
    """,
    "Q5": """
        SELECT primaryName, COUNT(*) AS credit_count
        FROM cast_credits
        WHERE category IN ('actor', 'actress')
        GROUP BY primaryName
        ORDER BY credit_count DESC
        LIMIT 25
    """,
    "Q6": """
        SELECT titleType, start_decade,
               AVG(runtimeMinutes) AS avg_runtime_minutes,
               COUNT(*) AS title_count
        FROM titles_enriched
        WHERE runtimeMinutes IS NOT NULL
        GROUP BY titleType, start_decade
        ORDER BY start_decade, titleType
    """,
}


@dataclass
class BenchmarkResult:
    query_id: str
    spark_ms: float
    starrocks_ms: float
    speedup: float


def resolve_gold_dir() -> str:
    if Path("/mnt/pipeline/lake/gold").exists():
        return config.DOCKER_GOLD_DIR
    return str(config.GOLD_DIR)


def resolve_results_path() -> Path:
    if Path("/mnt/pipeline").exists():
        return Path(config.DOCKER_BENCHMARK_RESULTS_PATH)
    return config.BENCHMARK_RESULTS_PATH


def format_results_report(results: list[BenchmarkResult], runs: int) -> str:
    lines = [
        "IMDb pipeline benchmark: Spark SQL vs StarRocks",
        f"Generated (UTC): {datetime.now(timezone.utc).isoformat()}",
        f"Runs per query: {runs}",
        "",
        f"{'Query':<6} {'Spark (ms)':>12} {'StarRocks (ms)':>16} {'Speedup':>10}",
        "-" * 48,
    ]
    for row in results:
        lines.append(
            f"{row.query_id:<6} {row.spark_ms:12.1f} {row.starrocks_ms:16.1f} "
            f"{row.speedup:9.1f}x"
        )
    lines.extend(
        [
            "",
            "Note: Spark reads gold Parquet; StarRocks reads loaded OLAP tables.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_results_report(path: Path, results: list[BenchmarkResult], runs: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_results_report(results, runs), encoding="utf-8")


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("imdb-benchmark")
        .config("spark.sql.adaptive.enabled", "true")
    )
    master = os.environ.get("SPARK_MASTER_URL", config.SPARK_MASTER)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def register_spark_views(spark: SparkSession, gold_dir: str) -> None:
    spark.read.parquet(f"{gold_dir}/titles_enriched").createOrReplaceTempView(
        "titles_enriched"
    )
    spark.read.parquet(f"{gold_dir}/episodes_enriched").createOrReplaceTempView(
        "episodes_enriched"
    )
    spark.read.parquet(f"{gold_dir}/cast_credits").createOrReplaceTempView(
        "cast_credits"
    )


def time_spark(spark: SparkSession, sql: str, runs: int = 3) -> float:
    durations: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        spark.sql(sql).collect()
        durations.append((time.perf_counter() - start) * 1000)
    return statistics.median(durations)


def get_starrocks_connection() -> pymysql.Connection:
    return pymysql.connect(
        host=config.resolve_starrocks_host(),
        port=config.STARROCKS_PORT,
        user=config.STARROCKS_USER,
        password=config.STARROCKS_PASSWORD,
        database=config.STARROCKS_DATABASE,
        autocommit=True,
    )


def time_starrocks(conn: pymysql.Connection, sql: str, runs: int = 3) -> float:
    durations: list[float] = []
    with conn.cursor() as cursor:
        for _ in range(runs):
            start = time.perf_counter()
            cursor.execute(sql)
            cursor.fetchall()
            durations.append((time.perf_counter() - start) * 1000)
    return statistics.median(durations)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    gold_dir = resolve_gold_dir()
    if not Path(gold_dir).exists():
        logger.error("Gold data not found at %s", gold_dir)
        return 1

    spark = create_spark()
    try:
        register_spark_views(spark, gold_dir)
    except Exception:
        spark.stop()
        logger.exception("Failed to register Spark views")
        return 1

    try:
        conn = get_starrocks_connection()
    except pymysql.Error as exc:
        spark.stop()
        logger.error("StarRocks connection failed: %s", exc)
        return 1

    runs = 3
    results: list[BenchmarkResult] = []

    for query_id, sql in BENCHMARK_QUERIES.items():
        try:
            spark_ms = time_spark(spark, sql, runs=runs)
            sr_ms = time_starrocks(conn, sql, runs=runs)
            speedup = spark_ms / sr_ms if sr_ms > 0 else float("inf")
            results.append(
                BenchmarkResult(
                    query_id=query_id,
                    spark_ms=spark_ms,
                    starrocks_ms=sr_ms,
                    speedup=speedup,
                )
            )
        except Exception as exc:
            logger.error("%s failed: %s", query_id, exc)

    conn.close()
    spark.stop()

    if not results:
        logger.error("No benchmark results collected")
        return 1

    output_path = resolve_results_path()
    write_results_report(output_path, results, runs)
    logger.info("Benchmark results written to %s", output_path)
    print(f"Benchmark results written to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
