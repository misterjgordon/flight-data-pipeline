from pyspark import pipelines as dp
from pyspark_datasources import OpenSkyDataSource

spark.dataSource.register(OpenSkyDataSource)


@dp.table(
    comment=(
        'Bronze: OpenSky /states/all via streaming source (batch read not supported). '
        'Appends one microbatch per pipeline run; silver keeps latest batch only.'
    ),
)
def ingest_flights():
    return spark.readStream.format('opensky').load()
