"""Unit tests for ``flight_row_transforms`` (silver shape for one OpenSky snapshot).

Speed is not asserted here: compare pipeline run timings in Databricks (per-node duration
before vs after) or use the Spark UI to confirm gold plans read ``flights_current`` only.
"""

import importlib
import types
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

pytestmark = pytest.mark.spark

_BRONZE_SCHEMA = StructType(
    [
        StructField('time_ingest', TimestampType(), True),
        StructField('icao24', StringType(), True),
        StructField('callsign', StringType(), True),
        StructField('origin_country', StringType(), True),
        StructField('time_position', TimestampType(), True),
        StructField('last_contact', TimestampType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('baro_altitude', DoubleType(), True),
        StructField('geo_altitude', DoubleType(), True),
        StructField('on_ground', BooleanType(), True),
        StructField('velocity', DoubleType(), True),
        StructField('true_track', DoubleType(), True),
        StructField('vertical_rate', DoubleType(), True),
        StructField('squawk', StringType(), True),
        StructField('spi', BooleanType(), True),
        StructField('category', IntegerType(), True),
    ]
)


@pytest.fixture
def flight_row_transforms(spark: SparkSession) -> types.ModuleType:
    """Import after ``spark`` exists: ``flight_row_transforms`` builds ``F.lit`` at import."""
    return importlib.import_module('flight_row_transforms')


def _recent_ts() -> datetime:
    return datetime.now(UTC) - timedelta(minutes=2)


def _bronze_row(**overrides: object) -> tuple:
    ts = _recent_ts()
    row = {
        'time_ingest': ts,
        'icao24': 'abc123',
        'callsign': '  XX1  ',
        'origin_country': 'US',
        'time_position': ts,
        'last_contact': ts,
        'latitude': 40.0,
        'longitude': -70.0,
        'baro_altitude': 1000.0,
        'geo_altitude': 1000.0,
        'on_ground': False,
        'velocity': 100.0,
        'true_track': 90.0,
        'vertical_rate': 1.0,
        'squawk': '0000',
        'spi': False,
        'category': 3,
    }
    row.update(overrides)
    return tuple(row[f.name] for f in _BRONZE_SCHEMA.fields)


def test_transform_batch_trims_callsign_heading_emitter(
    spark: SparkSession,
    flight_row_transforms: types.ModuleType,
):
    df_in = spark.createDataFrame([_bronze_row()], schema=_BRONZE_SCHEMA)
    out = flight_row_transforms.transform_bronze_to_silver_shape(df_in).collect()
    assert len(out) == 1
    r = out[0]
    assert r.callsign == 'XX1'
    assert r.heading == 'E'
    assert r.emitter_category == 'Small'
    assert r.velocity_kts == round(100.0 * 1.944, 1)


def test_transform_batch_drops_null_position(spark: SparkSession, flight_row_transforms):
    df_in = spark.createDataFrame(
        [
            _bronze_row(latitude=None),
            _bronze_row(icao24='keep', latitude=1.0, longitude=1.0),
        ],
        schema=_BRONZE_SCHEMA,
    )
    out = flight_row_transforms.transform_bronze_to_silver_shape(df_in).collect()
    assert len(out) == 1
    assert out[0].icao24 == 'keep'


def test_transform_batch_keeps_duplicate_icao24_time_position(
    spark: SparkSession,
    flight_row_transforms: types.ModuleType,
):
    ts = _recent_ts()
    df_in = spark.createDataFrame(
        [
            _bronze_row(time_position=ts, callsign='  A  '),
            _bronze_row(time_position=ts, callsign='  B  '),
        ],
        schema=_BRONZE_SCHEMA,
    )
    out = flight_row_transforms.transform_bronze_to_silver_shape(df_in).collect()
    assert len(out) == 2
    assert {r.callsign for r in out} == {'A', 'B'}


def test_latest_ingest_snapshot_keeps_only_max_batch(
    spark: SparkSession,
    flight_row_transforms: types.ModuleType,
):
    old = datetime(2000, 1, 1, tzinfo=UTC)
    new = _recent_ts()
    df_in = spark.createDataFrame(
        [
            _bronze_row(time_ingest=old, icao24='old'),
            _bronze_row(time_ingest=new, icao24='new1'),
            _bronze_row(time_ingest=new, icao24='new2'),
        ],
        schema=_BRONZE_SCHEMA,
    )
    out = flight_row_transforms.latest_ingest_snapshot(df_in).collect()
    assert {r.icao24 for r in out} == {'new1', 'new2'}


def test_transform_batch_does_not_slice_by_time_ingest(
    spark: SparkSession,
    flight_row_transforms: types.ModuleType,
):
    stale = datetime(2000, 1, 1, tzinfo=UTC)
    fresh_ts = _recent_ts()
    df_in = spark.createDataFrame(
        [
            _bronze_row(time_ingest=stale, time_position=stale, icao24='stale'),
            _bronze_row(
                time_ingest=fresh_ts,
                time_position=fresh_ts,
                icao24='fresh',
            ),
        ],
        schema=_BRONZE_SCHEMA,
    )
    out = flight_row_transforms.transform_bronze_to_silver_shape(df_in).collect()
    assert {r.icao24 for r in out} == {'stale', 'fresh'}
