"""Tests for surveyor data DAGs."""

import pytest
import os
from airflow.models import DagBag

# Constants
DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "dags")


def get_dags():
    """Get all DAGs from the dags directory."""
    dag_bag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    
    # Check for import errors
    if dag_bag.import_errors:
        print(f"DAG Import Errors: {dag_bag.import_errors}")
    
    return [
        (dag_id, dag, dag.fileloc)
        for dag_id, dag in dag_bag.dags.items()
    ]


class TestDagIntegrity:
    """Test suite for DAG validation."""
    
    @pytest.mark.parametrize("dag_id,dag,fileloc", get_dags())
    def test_dag_tags(self, dag_id, dag, fileloc):
        """Verify all DAGs have at least one tag."""
        assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    
    @pytest.mark.parametrize("dag_id,dag,fileloc", get_dags())
    def test_dag_retries(self, dag_id, dag, fileloc):
        """Verify all DAGs have retries >= 2."""
        retries = dag.default_args.get("retries", 0)
        assert retries >= 2, f"{dag_id} in {fileloc} has less than 2 retries"
    
    @pytest.mark.parametrize("dag_id,dag,fileloc", get_dags())
    def test_dag_owner(self, dag_id, dag, fileloc):
        """Verify all DAGs have an owner specified."""
        owner = dag.default_args.get("owner", "")
        assert owner, f"{dag_id} in {fileloc} has no owner specified"
    
    @pytest.mark.parametrize("dag_id,dag,fileloc", get_dags())
    def test_dag_description(self, dag_id, dag, fileloc):
        """Verify all DAGs have a description."""
        assert dag.description, f"{dag_id} in {fileloc} has no description"
    
    @pytest.mark.parametrize("dag_id,dag,fileloc", get_dags())
    def test_dag_schedule(self, dag_id, dag, fileloc):
        """Verify all DAGs have a valid schedule."""
        assert dag.schedule_interval is not None, f"{dag_id} in {fileloc} has no schedule"


class TestSurveyorDag:
    """Test suite for Surveyor ETL DAG specific tests."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.dag_bag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
        self.dag_id = "surveyor_data_etl"
    
    def test_surveyor_dag_exists(self):
        """Verify the surveyor ETL DAG exists."""
        assert self.dag_id in self.dag_bag.dags, f"DAG {self.dag_id} not found"
    
    def test_surveyor_dag_has_correct_tasks(self):
        """Verify the DAG has all expected tasks."""
        dag = self.dag_bag.get_dag(self.dag_id)
        expected_tasks = [
            "generate_surveyor_data",
            "transform_surveyor_data",
            "create_surveyor_table",
            "load_to_neon"
        ]
        
        for task_id in expected_tasks:
            assert task_id in dag.task_dict, f"Task {task_id} not found in {self.dag_id}"
    
    def test_surveyor_dag_task_count(self):
        """Verify the DAG has the expected number of tasks."""
        dag = self.dag_bag.get_dag(self.dag_id)
        assert len(dag.tasks) == 4, f"Expected 4 tasks, found {len(dag.tasks)}"
