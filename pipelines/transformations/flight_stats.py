from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(comment='Gold: aggregate velocity and aircraft count metrics, derived from cleaned silver data.')
def flights_stats():
    # Unqualified name is intentional: DLT resolves within pipeline namespace.
    # Outside a pipeline use workspace.default.flights_cleaned.
    df = spark.read.table("flights_cleaned")
    return df.agg(
        F.count("*").alias("num_events"),
        F.countDistinct("icao24").alias("distinct_aircraft"),
        F.max("velocity").alias("max_velocity"),
        F.round(F.avg("velocity"), 2).alias("avg_velocity"),
    )