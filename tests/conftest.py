import pytest
from pyspark.errors import PySparkRuntimeError
from pyspark.sql import SparkSession


def pytest_report_header(config: pytest.Config) -> list[str]:
    del config
    return [
        'tests/unit: @contract (always) + @spark (need working Java for PySpark).',
        'Default pytest addopts include -rs (skip reasons in the summary below).',
    ]


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter, exitstatus: int) -> None:
    del exitstatus
    skipped = terminalreporter.stats.get('skipped', [])
    passed = terminalreporter.stats.get('passed', [])
    if not skipped:
        return
    spark_skipped = sum(1 for rep in skipped if 'test_flight_row_transforms' in rep.nodeid)
    if spark_skipped == 0:
        return
    terminalreporter.ensure_newline()
    terminalreporter.write_sep('=', 'tests/unit — what ran vs skipped')
    terminalreporter.write_line(
        f'@contract: {len(passed)} passed — gold reads flights_current only; '
        'flights_cleaned removed from bundle.'
    )
    terminalreporter.write_line(
        f'@spark: {spark_skipped} skipped — transform logic in flight_row_transforms.py was not '
        'executed (PySpark needs a working JVM). See SKIP lines above.'
    )
    terminalreporter.write_line(
        'To run @spark tests: install a JDK, set JAVA_HOME, then: pytest tests/unit -m spark -v'
    )
    terminalreporter.write_line(
        '(Pytest repeats the same skip line once per @spark test; one JVM fix enables all of them.)'
    )


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    """Local SparkSession for unit tests."""
    try:
        return (
            SparkSession.builder.master('local[1]')
            .appName('databricks-unit-tests')
            .config('spark.sql.shuffle.partitions', '1')
            .getOrCreate()
        )
    except PySparkRuntimeError as exc:
        pytest.skip(
            'Skipped all @spark tests: PySpark could not start a JVM (install a JDK or fix '
            'JAVA_HOME, then re-run). '
            f'Underlying error: {exc}'
        )
