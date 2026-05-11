import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    """Local SparkSession for unit tests."""
    return (
        SparkSession.builder
        .master('local[1]')
        .appName('databricks-unit-tests')
        .config('spark.sql.shuffle.partitions', '1')
        .getOrCreate()
    )
