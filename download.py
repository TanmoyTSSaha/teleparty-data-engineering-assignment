"""Download IMDb bronze TSVs from Kaggle and IMDb official sources with local fallback."""

from __future__ import annotations

import gzip
import logging
import shutil
import sys
from pathlib import Path

import requests

import config

logger = logging.getLogger(__name__)


def _ensure_bronze_dir() -> Path:
    config.BRONZE_IMDB_DIR.mkdir(parents=True, exist_ok=True)
    return config.BRONZE_IMDB_DIR


def _missing_files(bronze_dir: Path) -> list[str]:
    return [name for name in config.REQUIRED_BRONZE_FILES if not (bronze_dir / name).exists()]


def _copy_tsvs_from_dir(source_dir: Path, bronze_dir: Path) -> None:
    for name in config.KAGGLE_FILES:
        src = source_dir / name
        if not src.exists():
            raise FileNotFoundError(f"Expected Kaggle file not found: {src}")
        dst = bronze_dir / name
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)
        logger.info("Copied %s -> %s", src, dst)


def _remove_kaggle_cache(bronze_dir: Path) -> None:
    cache_dir = bronze_dir / "_kaggle_cache"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        logger.info("Removed Kaggle cache at %s", cache_dir)


def download_kaggle_dataset(bronze_dir: Path) -> None:
    """Download Kaggle IMDb dataset TSVs into bronze_dir."""
    import kagglehub

    logger.info("Downloading Kaggle dataset %s", config.KAGGLE_DATASET)
    dataset_path = Path(
        kagglehub.dataset_download(
            f"{config.KAGGLE_DATASET}/{config.KAGGLE_VERSION}",
            output_dir=str(bronze_dir / "_kaggle_cache"),
        )
    )
    logger.info("Kaggle dataset extracted at %s", dataset_path)
    _copy_tsvs_from_dir(dataset_path, bronze_dir)
    _remove_kaggle_cache(bronze_dir)


def download_episode_tsv(bronze_dir: Path) -> None:
    """Download and decompress title.episode.tsv from IMDb official datasets."""
    target = bronze_dir / config.EPISODE_FILENAME
    gz_path = bronze_dir / f"{config.EPISODE_FILENAME}.gz"

    logger.info("Downloading episode data from %s", config.EPISODE_URL)
    with requests.get(config.EPISODE_URL, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(gz_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)

    with gzip.open(gz_path, "rb") as src, open(target, "wb") as dst:
        shutil.copyfileobj(src, dst)

    gz_path.unlink(missing_ok=True)
    logger.info("Wrote %s", target)


def validate_bronze(bronze_dir: Path) -> None:
    missing = _missing_files(bronze_dir)
    if missing:
        raise FileNotFoundError(
            "Missing required bronze files in "
            f"{bronze_dir}: {', '.join(missing)}. "
            "Place TSV files manually or fix network/download issues."
        )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    bronze_dir = _ensure_bronze_dir()
    missing_before = _missing_files(bronze_dir)

    if not missing_before:
        _remove_kaggle_cache(bronze_dir)
        logger.info("All bronze files already present in %s", bronze_dir)
        return 0

    kaggle_missing = [f for f in config.KAGGLE_FILES if f in missing_before]
    episode_missing = config.EPISODE_FILENAME in missing_before

    errors: list[str] = []

    if kaggle_missing:
        try:
            download_kaggle_dataset(bronze_dir)
        except Exception as exc:
            errors.append(f"Kaggle download failed: {exc}")
            logger.warning("Kaggle download failed: %s", exc)

    if config.EPISODE_FILENAME not in _missing_files(bronze_dir):
        episode_missing = False

    if episode_missing:
        try:
            download_episode_tsv(bronze_dir)
        except Exception as exc:
            errors.append(f"Episode download failed: {exc}")
            logger.warning("Episode download failed: %s", exc)

    still_missing = _missing_files(bronze_dir)
    if still_missing:
        if errors:
            for err in errors:
                logger.error(err)
        logger.error(
            "Bronze fallback incomplete. Missing: %s. "
            "Copy files into %s and re-run.",
            ", ".join(still_missing),
            bronze_dir,
        )
        return 1

    if errors:
        logger.warning("Some downloads failed; using bronze fallback files: %s", bronze_dir)

    validate_bronze(bronze_dir)
    logger.info("Bronze dataset ready in %s", bronze_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
