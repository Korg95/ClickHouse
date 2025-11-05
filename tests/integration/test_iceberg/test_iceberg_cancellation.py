import time
import uuid

import pytest

from helpers.client import QueryRuntimeException
from helpers.iceberg_utils import create_iceberg_table


SNAPSHOT_INSERT_ROUNDS = 120


@pytest.fixture(scope="module")
def iceberg_table_with_many_snapshots(started_cluster_iceberg):
    cluster = started_cluster_iceberg
    node = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"iceberg_cancellation_{uuid.uuid4().hex}"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
            CREATE TABLE {table_name} (
                id INT
            )
            USING iceberg
            OPTIONS('format-version'='2')
        """
    )

    for batch in range(SNAPSHOT_INSERT_ROUNDS):
        spark.sql(
            f"""
                INSERT INTO {table_name}
                SELECT {batch} AS id
            """
        )

    create_iceberg_table("s3", node, table_name, cluster)

    try:
        yield table_name
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_max_execution_time_interrupts_metadata(started_cluster_iceberg, iceberg_table_with_many_snapshots):
    node = started_cluster_iceberg.instances["node1"]
    table_name = iceberg_table_with_many_snapshots

    with pytest.raises(QueryRuntimeException) as exc:
        node.query(
            f"SELECT count() FROM {table_name}",
            settings={"max_execution_time": 3, "max_threads": 1},
            timeout=120,
        )

    assert "TIMEOUT_EXCEEDED" in exc.value.stderr
    assert "Iceberg metadata read" in exc.value.stderr


def test_kill_query_interrupts_metadata(started_cluster_iceberg, iceberg_table_with_many_snapshots):
    node = started_cluster_iceberg.instances["node1"]
    table_name = iceberg_table_with_many_snapshots
    query_id = f"iceberg_kill_{uuid.uuid4().hex}"

    request = node.client.get_query_request(
        f"SELECT count() FROM {table_name}",
        settings={"max_threads": 1},
        query_id=query_id,
        timeout=120,
    )

    for _ in range(60):
        processes = node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        if processes == "1":
            break
        time.sleep(0.5)
    else:
        request.get_answer_and_error()
        pytest.fail("Iceberg query did not appear in system.processes")

    node.query(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
    stdout, stderr = request.get_answer_and_error()

    assert "QUERY_WAS_CANCELLED" in stderr
    assert "Iceberg metadata read" in stderr
