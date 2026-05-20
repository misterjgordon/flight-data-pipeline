"""Shared row-level transforms from bronze ingest shape to silver flight columns."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ICAO Doc 9684 emitter category codes
_EMITTER_CATEGORIES = {
    0: 'Unknown',
    1: 'No ADS-B Category',
    2: 'Light',
    3: 'Small',
    4: 'Large',
    5: 'High Vortex Large',
    6: 'Heavy',
    7: 'High Performance',
    8: 'Rotorcraft',
    9: 'Glider',
    10: 'Lighter-than-Air',
    11: 'Parachutist',
    12: 'Ultralight',
    13: 'Reserved',
    14: 'UAV',
    15: 'Space Vehicle',
    16: 'Surface Emergency',
    17: 'Surface Service',
    18: 'Point Obstacle',
    19: 'Cluster Obstacle',
    20: 'Line Obstacle',
}

_CATEGORY_MAP = F.create_map(
    *[item for k, v in _EMITTER_CATEGORIES.items() for item in (F.lit(k), F.lit(v))]
)


def latest_ingest_snapshot(df: DataFrame) -> DataFrame:
    """Keep rows from the most recent ``time_ingest`` batch in bronze."""
    max_ingest = df.agg(F.max('time_ingest').alias('max_time_ingest'))
    return df.join(max_ingest, df.time_ingest == max_ingest.max_time_ingest).drop('max_time_ingest')


def _heading(track_col):
    """Convert true track degrees to 8-point cardinal direction."""
    return (
        F.when((track_col >= 337.5) | (track_col < 22.5), 'N')
        .when((track_col >= 22.5) & (track_col < 67.5), 'NE')
        .when((track_col >= 67.5) & (track_col < 112.5), 'E')
        .when((track_col >= 112.5) & (track_col < 157.5), 'SE')
        .when((track_col >= 157.5) & (track_col < 202.5), 'S')
        .when((track_col >= 202.5) & (track_col < 247.5), 'SW')
        .when((track_col >= 247.5) & (track_col < 292.5), 'W')
        .when((track_col >= 292.5) & (track_col < 337.5), 'NW')
    )


def transform_bronze_to_silver_shape(df: DataFrame) -> DataFrame:
    """Normalize ingest rows to the silver column layout.

    Expects one OpenSky snapshot per call (e.g. after ``latest_ingest_snapshot``).
    Applies null/quality filters and derived columns only; no dedupe.
    """
    return (
        df.filter(F.col('latitude').isNotNull() & F.col('longitude').isNotNull())
        .filter(F.col('velocity').isNull() | F.col('velocity').between(0, 600))
        .filter(F.col('geo_altitude').isNull() | F.col('geo_altitude').between(-500, 25000))
        .withColumn('callsign', F.trim(F.col('callsign')))
        .withColumn('velocity_kts', F.round(F.col('velocity') * 1.944, 1))
        .withColumn(
            'altitude_ft',
            F.round(
                F.coalesce(F.col('baro_altitude'), F.col('geo_altitude')) * 3.281,
                0,
            ),
        )
        .withColumn('vertical_rate_fpm', F.round(F.col('vertical_rate') * 196.85, 0))
        .withColumn('is_airborne', ~F.col('on_ground'))
        .withColumn('heading', _heading(F.col('true_track')))
        .withColumn('emitter_category', _CATEGORY_MAP[F.col('category')])
        .select(
            'time_ingest',
            'icao24',
            'callsign',
            'origin_country',
            'time_position',
            'last_contact',
            'latitude',
            'longitude',
            'altitude_ft',
            'baro_altitude',
            'geo_altitude',
            'is_airborne',
            'velocity',
            'velocity_kts',
            'true_track',
            'heading',
            'vertical_rate',
            'vertical_rate_fpm',
            'squawk',
            'spi',
            'category',
            'emitter_category',
        )
    )
