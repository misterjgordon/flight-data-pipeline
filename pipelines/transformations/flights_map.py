from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(comment='Gold: per-aircraft snapshot enriched with country flight count, ordered for stable dashboard colour mapping.')
def flights_map():
    # Unqualified name is intentional: DLT resolves within pipeline namespace.
    # Outside a pipeline use workspace.default.flights_cleaned.
    df = spark.read.table('flights_cleaned')

    country_counts = df \
        .filter(F.col('is_airborne')) \
        .groupBy('origin_country') \
        .agg(F.count('*').alias('aircraft_count'))

    return (
        df
        .filter(F.col('is_airborne'))
        .filter(F.col('latitude').isNotNull() & F.col('longitude').isNotNull())
        .join(country_counts, on='origin_country', how='left')
        .select(
            'icao24',
            'callsign',
            'origin_country',
            'latitude',
            'longitude',
            'altitude_ft',
            'velocity_kts',
            'heading',
            'emitter_category',
            'vertical_rate_fpm',
            'aircraft_count',
        )
        # Alphabetical order ensures stable colour palette assignment on each dashboard refresh
        .orderBy('origin_country')
    )
