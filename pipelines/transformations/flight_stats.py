from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    comment='Gold: snapshot velocity and aircraft-count metrics from flights_current.',
)
def flights_stats():
    # Unqualified name is intentional: DLT resolves within pipeline namespace.
    df = spark.read.table('flights_current')
    return df.agg(
        F.count('*').alias('num_events'),
        F.countDistinct('icao24').alias('distinct_aircraft'),
        F.max('velocity').alias('max_velocity'),
        F.round(F.avg('velocity'), 2).alias('avg_velocity'),
    )
