"""Static checks: gold reads ``flights_current`` only; no ``flights_cleaned`` in pipeline."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.contract

_GOLD_FILES = (
    'flight_origin.py',
    'flight_stats.py',
    'flights_map.py',
    'top_countries.py',
)


@pytest.fixture(scope='module')
def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def transformations_dir(repo_root: Path) -> Path:
    return repo_root / 'pipelines' / 'transformations'


@pytest.mark.parametrize('filename', _GOLD_FILES, ids=list(_GOLD_FILES))
def test_gold_file_reads_flights_current_not_cleaned(transformations_dir: Path, filename: str):
    text = (transformations_dir / filename).read_text()
    reads_current = (
        "read.table('flights_current')" in text or 'read.table("flights_current")' in text
    )
    assert reads_current, f'{filename} must batch-read flights_current for gold.'
    assert 'flights_cleaned' not in text, (
        f'{filename} must not reference flights_cleaned (table removed from pipeline).'
    )


def test_flights_cleaned_module_removed(transformations_dir: Path):
    assert not (transformations_dir / 'flights_cleaned.py').exists()


def test_pipeline_yaml_has_no_flights_cleaned_library(repo_root: Path):
    text = (repo_root / 'resources' / 'flight_data_pipeline.yml').read_text()
    assert 'flights_cleaned' not in text


def test_ingest_streams_and_silver_is_latest_snapshot_mv(transformations_dir: Path):
    ingest = (transformations_dir / 'ingest_flights.py').read_text()
    current = (transformations_dir / 'flights_current.py').read_text()
    transforms = (transformations_dir / 'flight_row_transforms.py').read_text()
    assert "readStream.format('opensky')" in ingest
    assert '@dp.table' in ingest
    assert "read.table('ingest_flights')" in current
    assert 'latest_ingest_snapshot' in current
    assert '@dp.materialized_view' in current
    assert 'def latest_ingest_snapshot' in transforms
