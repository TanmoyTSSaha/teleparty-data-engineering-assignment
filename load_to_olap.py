"""Load gold Parquet datasets into StarRocks via Broker Load."""

from __future__ import annotations

import logging
import sys
import time
import uuid
from pathlib import Path

import pymysql

import config

logger = logging.getLogger(__name__)

GOLD_TABLES = [
    {
        "table": "titles_enriched",
        "path": f"{config.DOCKER_GOLD_DIR}/titles_enriched",
        "file_columns": (
            "tconst, primaryTitle, isAdult, startYear, endYear, runtimeMinutes, "
            "genres, genres_array, primary_genre, averageRating, numVotes, is_rated"
        ),
        "path_columns": ("titleType", "start_decade"),
    },
    {
        "table": "episodes_enriched",
        "path": f"{config.DOCKER_GOLD_DIR}/episodes_enriched",
        "file_columns": (
            "tconst, parentTconst, seasonNumber, episodeNumber, episodeTitle, "
            "seriesTitle, episodeStartYear, seriesStartYear, episodeRuntimeMinutes, "
            "seriesRuntimeMinutes, episodeGenres, seriesGenres, episodePrimaryGenre, "
            "seriesPrimaryGenre, episodeAverageRating, episodeNumVotes, "
            "seriesAverageRating, seriesNumVotes"
        ),
        "path_columns": ("start_decade",),
    },
    {
        "table": "cast_credits",
        "path": f"{config.DOCKER_GOLD_DIR}/cast_credits",
        "file_columns": (
            "tconst, nconst, characters, primaryName, titleType, primaryTitle, "
            "startYear, start_decade, primary_genre, averageRating, numVotes"
        ),
        "path_columns": ("category",),
    },
]


def wait_for_starrocks(host: str, port: int, timeout_sec: int = 120) -> None:
    """Wait until StarRocks MySQL port accepts connections."""
    deadline = time.time() + timeout_sec
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=config.STARROCKS_USER,
                password=config.STARROCKS_PASSWORD,
                connect_timeout=3,
            )
            conn.close()
            return
        except pymysql.Error as exc:
            last_error = exc
            time.sleep(3)
    raise ConnectionError(
        f"StarRocks is not reachable at {host}:{port}. "
        "Run `docker compose up -d starrocks` and wait for it to become healthy."
    ) from last_error


def get_connection() -> pymysql.Connection:
    host = config.resolve_starrocks_host()
    wait_for_starrocks(host, config.STARROCKS_PORT)
    return pymysql.connect(
        host=host,
        port=config.STARROCKS_PORT,
        user=config.STARROCKS_USER,
        password=config.STARROCKS_PASSWORD,
        autocommit=True,
    )


def run_sql_file(conn: pymysql.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    with conn.cursor() as cursor:
        for statement in statements:
            logger.info("Executing DDL: %s", path.name)
            cursor.execute(statement)


def apply_schema(conn: pymysql.Connection) -> None:
    for sql_file in sorted(config.SCHEMA_DIR.glob("*.sql")):
        run_sql_file(conn, sql_file)


def wait_for_load(conn: pymysql.Connection, label: str, timeout_sec: int = 3600) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT STATE, ERROR_MSG FROM information_schema.loads "
                "WHERE LABEL = %s ORDER BY CREATE_TIME DESC LIMIT 1",
                (label,),
            )
            row = cursor.fetchone()
        if not row:
            time.sleep(2)
            continue
        state, error_msg = row
        if state == "FINISHED":
            logger.info("Load %s finished", label)
            return
        if state == "CANCELLED":
            raise RuntimeError(f"Load {label} cancelled: {error_msg}")
        time.sleep(3)
    raise TimeoutError(f"Timed out waiting for load {label}")


def parquet_glob(path_columns: tuple[str, ...]) -> str:
    """Build a glob that reaches parquet files under hive-style partition dirs."""
    return "/".join(["*"] * (len(path_columns) + 1))


def broker_load_table(conn: pymysql.Connection, spec: dict) -> None:
    label = f"{spec['table']}_{uuid.uuid4().hex[:8]}"
    path_columns = ", ".join(spec["path_columns"])
    glob_pattern = parquet_glob(spec["path_columns"])
    infile = f"file://{spec['path']}/{glob_pattern}"
    sql = f"""
    LOAD LABEL imdb.{label}
    (
        DATA INFILE("{infile}")
        INTO TABLE {spec['table']}
        FORMAT AS "parquet"
        ({spec['file_columns']})
        COLUMNS FROM PATH AS ({path_columns})
    )
    WITH BROKER
    PROPERTIES ("timeout" = "3600")
    """
    with conn.cursor() as cursor:
        logger.info("Starting broker load for %s", spec["table"])
        cursor.execute(sql)
    wait_for_load(conn, label)


def verify_counts(conn: pymysql.Connection) -> None:
    with conn.cursor() as cursor:
        for spec in GOLD_TABLES:
            table = spec["table"]
            cursor.execute(f"SELECT COUNT(*) FROM imdb.{table}")
            count = cursor.fetchone()[0]
            logger.info("Row count %s: %s", table, count)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    gold_root = Path(
        config.DOCKER_GOLD_DIR if Path("/mnt/pipeline").exists() else config.GOLD_DIR
    )
    for spec in GOLD_TABLES:
        table_path = gold_root / spec["table"]
        if not table_path.exists():
            logger.error("Gold path missing: %s. Run etl_job.py first.", table_path)
            return 1

    try:
        conn = get_connection()
    except pymysql.Error as exc:
        logger.error(
            "Cannot connect to StarRocks at %s:%s — is the container running? (%s)",
            config.STARROCKS_HOST,
            config.STARROCKS_PORT,
            exc,
        )
        return 1

    try:
        apply_schema(conn)
        for spec in GOLD_TABLES:
            broker_load_table(conn, spec)
        verify_counts(conn)
    except Exception:
        logger.exception("StarRocks load failed")
        return 1
    finally:
        conn.close()

    logger.info("StarRocks load completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
