"""Test DAG integrity, syntax, and task dependency graph."""
import os
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag():
    dag_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)


def test_dagbag_import_errors(dag_bag):
    """Verify that there are no syntax or import errors across all DAGs."""
    assert len(dag_bag.import_errors) == 0, f"Import errors detected: {dag_bag.import_errors}"


def test_csv_cleaning_pipeline_exists(dag_bag):
    """Verify that csv_cleaning_pipeline DAG is loaded correctly."""
    dag = dag_bag.get_dag("csv_cleaning_pipeline")
    assert dag is not None, "DAG 'csv_cleaning_pipeline' was not found"
    assert dag.dag_id == "csv_cleaning_pipeline"
    assert dag.catchup is False
    assert dag.schedule_interval is None


def test_csv_cleaning_pipeline_task_dependencies(dag_bag):
    """Verify that the DAG tasks and their dependency chain are correctly configured."""
    dag = dag_bag.get_dag("csv_cleaning_pipeline")
    expected_tasks = {"clean_csv", "validate_data", "load_mysql"}
    actual_tasks = set(dag.task_ids)
    assert expected_tasks == actual_tasks, f"Expected {expected_tasks}, found {actual_tasks}"

    clean_task = dag.get_task("clean_csv")
    validate_task = dag.get_task("validate_data")
    load_task = dag.get_task("load_mysql")

    # Dependency check: clean_csv -> validate_data -> load_mysql
    assert "validate_data" in clean_task.downstream_task_ids
    assert "clean_csv" in validate_task.upstream_task_ids
    assert "load_mysql" in validate_task.downstream_task_ids
    assert "validate_data" in load_task.upstream_task_ids
    assert len(load_task.downstream_task_ids) == 0
