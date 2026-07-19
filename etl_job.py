"""PySpark ETL: bronze TSV -> silver Parquet -> gold Parquet."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

import config

logger = logging.getLogger(__name__)


def resolve_data_paths() -> tuple[str, str, str]:
    """Return bronze, silver, and gold base paths for local or Docker execution."""
    if Path("/mnt/pipeline/bronze/imdb").exists():
        bronze = config.DOCKER_BRONZE_DIR
        silver = config.DOCKER_SILVER_DIR
        gold = config.DOCKER_GOLD_DIR
    else:
        bronze = str(config.BRONZE_IMDB_DIR)
        silver = str(config.SILVER_DIR)
        gold = str(config.GOLD_DIR)
    return bronze, silver, gold


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("imdb-etl")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    master = os.environ.get("SPARK_MASTER_URL", config.SPARK_MASTER)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def read_tsv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("nullValue", config.NULL_TOKEN)
        .schema(schema)
        .csv(path)
    )


def write_parquet(
    df: DataFrame,
    path: str,
    partition_cols: list[str] | None = None,
    repartition_cols: list[str] | None = None,
) -> None:
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if repartition_cols:
        df = df.repartition(*repartition_cols)
        writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)


def build_title_basics(spark: SparkSession, bronze_dir: str) -> DataFrame:
    schema = StructType(
        [
            StructField("tconst", StringType(), True),
            StructField("titleType", StringType(), True),
            StructField("primaryTitle", StringType(), True),
            StructField("originalTitle", StringType(), True),
            StructField("isAdult", StringType(), True),
            StructField("startYear", StringType(), True),
            StructField("endYear", StringType(), True),
            StructField("runtimeMinutes", StringType(), True),
            StructField("genres", StringType(), True),
        ]
    )
    df = read_tsv(spark, f"{bronze_dir}/title.basics.tsv", schema)
    return (
        df.drop("originalTitle")
        .withColumn("isAdult", F.col("isAdult").cast(IntegerType()))
        .withColumn("startYear", F.col("startYear").cast(IntegerType()))
        .withColumn("endYear", F.col("endYear").cast(IntegerType()))
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast(IntegerType()))
        .withColumn(
            "start_decade",
            F.when(
                F.col("startYear").isNotNull(),
                (F.floor(F.col("startYear") / 10) * 10).cast(StringType()),
            ).otherwise(F.lit("unknown")),
        )
        .withColumn(
            "genres_array",
            F.when(
                F.col("genres").isNull() | (F.col("genres") == "\\N"),
                F.array().cast("array<string>"),
            ).otherwise(F.split(F.col("genres"), ",")),
        )
        .withColumn("primary_genre", F.element_at(F.col("genres_array"), 1))
    )


def build_title_ratings(spark: SparkSession, bronze_dir: str) -> DataFrame:
    schema = StructType(
        [
            StructField("tconst", StringType(), True),
            StructField("averageRating", DoubleType(), True),
            StructField("numVotes", LongType(), True),
        ]
    )
    return read_tsv(spark, f"{bronze_dir}/title.ratings.tsv", schema)


def build_title_episode(spark: SparkSession, bronze_dir: str) -> DataFrame:
    schema = StructType(
        [
            StructField("tconst", StringType(), True),
            StructField("parentTconst", StringType(), True),
            StructField("seasonNumber", StringType(), True),
            StructField("episodeNumber", StringType(), True),
        ]
    )
    return (
        read_tsv(spark, f"{bronze_dir}/title.episode.tsv", schema)
        .withColumn("seasonNumber", F.col("seasonNumber").cast(IntegerType()))
        .withColumn("episodeNumber", F.col("episodeNumber").cast(IntegerType()))
    )


def build_title_principals(spark: SparkSession, bronze_dir: str) -> DataFrame:
    schema = StructType(
        [
            StructField("tconst", StringType(), True),
            StructField("ordering", IntegerType(), True),
            StructField("nconst", StringType(), True),
            StructField("category", StringType(), True),
            StructField("job", StringType(), True),
            StructField("characters", StringType(), True),
        ]
    )
    return read_tsv(spark, f"{bronze_dir}/title.principals.tsv", schema).select(
        "tconst", "nconst", "category", "characters"
    )


def build_name_basics(spark: SparkSession, bronze_dir: str) -> DataFrame:
    schema = StructType(
        [
            StructField("nconst", StringType(), True),
            StructField("primaryName", StringType(), True),
            StructField("birthYear", StringType(), True),
            StructField("deathYear", StringType(), True),
            StructField("primaryProfession", StringType(), True),
            StructField("knownForTitles", StringType(), True),
        ]
    )
    return read_tsv(spark, f"{bronze_dir}/name.basics.tsv", schema).select(
        "nconst", "primaryName"
    )


def run_silver(spark: SparkSession, bronze_dir: str, silver_dir: str) -> None:
    logger.info("Building silver layer")
    title_basics = build_title_basics(spark, bronze_dir)
    title_ratings = build_title_ratings(spark, bronze_dir)
    title_episode = build_title_episode(spark, bronze_dir)
    title_principals = build_title_principals(spark, bronze_dir)
    name_basics = build_name_basics(spark, bronze_dir)

    write_parquet(
        title_basics,
        f"{silver_dir}/title_basics",
        partition_cols=["titleType", "start_decade"],
        repartition_cols=["titleType", "start_decade"],
    )
    write_parquet(title_ratings, f"{silver_dir}/title_ratings")
    write_parquet(title_episode, f"{silver_dir}/title_episode")
    write_parquet(
        title_principals,
        f"{silver_dir}/title_principals",
        partition_cols=["category"],
        repartition_cols=["category"],
    )
    write_parquet(name_basics, f"{silver_dir}/name_basics")

    return None


def run_gold(spark: SparkSession, silver_dir: str, gold_dir: str) -> None:
    logger.info("Building gold layer")

    basics = spark.read.parquet(f"{silver_dir}/title_basics")
    ratings = spark.read.parquet(f"{silver_dir}/title_ratings")
    episode = spark.read.parquet(f"{silver_dir}/title_episode")
    principals = spark.read.parquet(f"{silver_dir}/title_principals")
    names = spark.read.parquet(f"{silver_dir}/name_basics")

    titles_enriched = (
        basics.join(ratings, "tconst", "left")
        .withColumn("is_rated", F.col("averageRating").isNotNull())
    )
    write_parquet(
        titles_enriched,
        f"{gold_dir}/titles_enriched",
        partition_cols=["titleType", "start_decade"],
        repartition_cols=["titleType", "start_decade"],
    )

    ep = basics.alias("ep")
    parent = basics.alias("parent")
    er = ratings.alias("er")
    pr = ratings.alias("pr")

    episodes_enriched = (
        episode.alias("e")
        .join(ep, F.col("e.tconst") == F.col("ep.tconst"), "inner")
        .join(parent, F.col("e.parentTconst") == F.col("parent.tconst"), "inner")
        .join(er, F.col("e.tconst") == F.col("er.tconst"), "left")
        .join(pr, F.col("e.parentTconst") == F.col("pr.tconst"), "left")
        .select(
            F.col("e.tconst").alias("tconst"),
            F.col("e.parentTconst").alias("parentTconst"),
            F.col("e.seasonNumber").alias("seasonNumber"),
            F.col("e.episodeNumber").alias("episodeNumber"),
            F.col("ep.primaryTitle").alias("episodeTitle"),
            F.col("parent.primaryTitle").alias("seriesTitle"),
            F.col("ep.startYear").alias("episodeStartYear"),
            F.col("parent.startYear").alias("seriesStartYear"),
            F.col("ep.runtimeMinutes").alias("episodeRuntimeMinutes"),
            F.col("parent.runtimeMinutes").alias("seriesRuntimeMinutes"),
            F.col("ep.genres").alias("episodeGenres"),
            F.col("parent.genres").alias("seriesGenres"),
            F.col("ep.primary_genre").alias("episodePrimaryGenre"),
            F.col("parent.primary_genre").alias("seriesPrimaryGenre"),
            F.col("er.averageRating").alias("episodeAverageRating"),
            F.col("er.numVotes").alias("episodeNumVotes"),
            F.col("pr.averageRating").alias("seriesAverageRating"),
            F.col("pr.numVotes").alias("seriesNumVotes"),
            F.when(
                F.col("ep.start_decade").isNotNull(),
                F.col("ep.start_decade"),
            )
            .when(
                F.col("parent.start_decade").isNotNull(),
                F.col("parent.start_decade"),
            )
            .otherwise(F.lit("unknown"))
            .alias("start_decade"),
        )
    )
    write_parquet(
        episodes_enriched,
        f"{gold_dir}/episodes_enriched",
        partition_cols=["start_decade"],
        repartition_cols=["start_decade"],
    )

    cast_credits = (
        principals.alias("p")
        .join(basics.alias("b"), F.col("p.tconst") == F.col("b.tconst"), "inner")
        .join(names.alias("n"), F.col("p.nconst") == F.col("n.nconst"), "inner")
        .join(ratings.alias("r"), F.col("p.tconst") == F.col("r.tconst"), "left")
        .select(
            F.col("p.tconst"),
            F.col("p.nconst"),
            F.col("p.category"),
            F.col("p.characters"),
            F.col("n.primaryName"),
            F.col("b.titleType"),
            F.col("b.primaryTitle"),
            F.col("b.startYear"),
            F.col("b.start_decade"),
            F.col("b.primary_genre"),
            F.col("r.averageRating"),
            F.col("r.numVotes"),
        )
    )
    write_parquet(
        cast_credits,
        f"{gold_dir}/cast_credits",
        partition_cols=["category"],
        repartition_cols=["category"],
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    bronze_dir, silver_dir, gold_dir = resolve_data_paths()

    for name in config.REQUIRED_BRONZE_FILES:
        path = Path(bronze_dir) / name
        if not path.exists():
            logger.error("Missing bronze file: %s", path)
            return 1

    Path(silver_dir).mkdir(parents=True, exist_ok=True)
    Path(gold_dir).mkdir(parents=True, exist_ok=True)

    spark = create_spark()
    try:
        run_silver(spark, bronze_dir, silver_dir)
        run_gold(spark, silver_dir, gold_dir)
        logger.info("ETL completed. Silver: %s Gold: %s", silver_dir, gold_dir)
    except Exception:
        logger.exception("ETL failed")
        return 1
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
