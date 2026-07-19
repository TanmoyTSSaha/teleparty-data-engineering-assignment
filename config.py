"""Shared paths, constants, and Spark schemas for the IMDb pipeline."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

BRONZE_IMDB_DIR = PROJECT_ROOT / "data" / "bronze" / "imdb"
SILVER_DIR = PROJECT_ROOT / "data" / "lake" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "lake" / "gold"
BENCHMARK_RESULTS_PATH = PROJECT_ROOT / "data" / "benchmark_results.txt"
SCHEMA_DIR = PROJECT_ROOT / "schema" / "starrocks"
QUERIES_DIR = PROJECT_ROOT / "queries"

KAGGLE_DATASET = "ashirwadsangwan/imdb-dataset"
KAGGLE_VERSION = "versions/895"

EPISODE_URL = "https://datasets.imdbws.com/title.episode.tsv.gz"
EPISODE_FILENAME = "title.episode.tsv"

KAGGLE_FILES = [
    "name.basics.tsv",
    "title.basics.tsv",
    "title.principals.tsv",
    "title.ratings.tsv",
]

REQUIRED_BRONZE_FILES = KAGGLE_FILES + [EPISODE_FILENAME]

# Docker-internal paths (mounted from ./data; avoid /data on StarRocks — it uses /data/deploy)
DOCKER_DATA_ROOT = "/mnt/pipeline"
DOCKER_BRONZE_DIR = f"{DOCKER_DATA_ROOT}/bronze/imdb"
DOCKER_SILVER_DIR = f"{DOCKER_DATA_ROOT}/lake/silver"
DOCKER_GOLD_DIR = f"{DOCKER_DATA_ROOT}/lake/gold"
DOCKER_BENCHMARK_RESULTS_PATH = f"{DOCKER_DATA_ROOT}/benchmark_results.txt"

STARROCKS_HOST = "localhost"
STARROCKS_PORT = 9030
STARROCKS_USER = "root"
STARROCKS_PASSWORD = ""
STARROCKS_DATABASE = "imdb"


def resolve_starrocks_host() -> str:
    """Pick StarRocks host for host vs Docker execution."""
    if os.environ.get("STARROCKS_HOST"):
        return os.environ["STARROCKS_HOST"]
    if Path("/mnt/pipeline").exists():
        return "starrocks"
    host = STARROCKS_HOST
    if host == "localhost":
        return "127.0.0.1"
    return host


SPARK_MASTER = "spark://spark-master:7077"

NULL_TOKEN = "\\N"
