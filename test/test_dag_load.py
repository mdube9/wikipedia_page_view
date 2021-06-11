
from dags import wikipiedia_report_generator_dag
import unittest


class TestDagLoad(unittest.TestCase):
    """ Test to check if DAG is loaded correctly"""
    def test_dag_load(self):
        dag = vars(wikipiedia_report_generator_dag).get('dag')
        task = dag.tasks
        self.assertEqual(len(task), 7)


