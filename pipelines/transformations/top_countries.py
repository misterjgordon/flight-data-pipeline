from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment='Gold: top 10 countries by current airborne aircraft count.')
def top_countries():
    # Unqualified name is intentional: DLT resolves within pipeline namespace.
    # Outside a pipeline use workspace.default.flights_cleaned.
    df = spark.read.table('flights_cleaned')

    return (
        df
        .filter(F.col('is_airborne'))
        .groupBy('origin_country')
        .agg(F.count('*').alias('aircraft_count'))
        .orderBy(F.col('aircraft_count').desc())
        .limit(10)
    )
