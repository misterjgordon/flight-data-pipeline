from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(comment='Gold: percentage of tracked flights per country of origin, derived from cleaned silver data.')
def flight_origin():
    # Unqualified name is intentional: DLT resolves within pipeline namespace.
    # Outside a pipeline use workspace.default.flights_cleaned.
    df = spark.read.table("flights_cleaned")
    
    origin_counts = df.groupBy("origin_country").agg(
        F.count("*").alias("flight_count")
    )

    result = origin_counts \
        .withColumn("total_flights", F.sum("flight_count").over(Window.partitionBy())) \
        .withColumn("percentage", F.round((F.col("flight_count") / F.col("total_flights")) * 100, 2)) \
        .select("origin_country", "flight_count", "percentage") \
        .orderBy(F.col("percentage").desc())
    
    return result
