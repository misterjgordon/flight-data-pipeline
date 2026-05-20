from pyspark import pipelines as dp

from flight_row_transforms import latest_ingest_snapshot, transform_bronze_to_silver_shape


@dp.materialized_view(
    comment=(
        'Silver: latest ingest batch with row transforms; recomputed each pipeline run. '
        'Gold batch-reads this table.'
    ),
    cluster_by=['time_ingest'],
)
def flights_current():
    # Unqualified name is intentional: DLT resolves within pipeline namespace.
    df = spark.read.table('ingest_flights')
    return transform_bronze_to_silver_shape(latest_ingest_snapshot(df))
