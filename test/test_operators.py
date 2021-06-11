from unittest import TestCase
from unittest import mock
import gzip
import os
import datetime

from airflow import DAG

from operators.file_unzip_operator import UnZipOperator
from operators.download_operator import DownloadOperator


class TestOperators(TestCase):

    @mock.patch('glob.glob', return_value=['/tmp/test.gz'])
    def test_unzip_operator(self,_):

        with gzip.open('/tmp/test.gz', 'wb') as f:
            f.write(b'foo\nbar\n')

        UnZipOperator(task_id='bar',
                      start_date=datetime.datetime.now(),
                      dag=DAG('foo')).execute()

        self.assertTrue(os.path.isfile('/tmp/test'))
        self.assertTrue(not os.path.isfile('/tmp/test.gz'))

    class MockRequest:
        def get(self):
            return MockRequest
        content= b'\x1f\x8b\x08\x08A\xf6\xb6`\x02\xfftest\x00K\xcb\xcf\xe7JJ,\xe2\x02\x00\x82\x83\xac\x98\x08\x00\x00\x00'
        status_code = 200
    mock_request = MockRequest
    status_code=200

    @mock.patch('requests.get',return_value= mock_request)
    @mock.patch('os.path.join', return_value = '/tmp/test.gz')
    @mock.patch('utils.postgres_utils.get_existing_file', retun_value=[])
    def test_download_operator(self,_,path,req):
        dag = DAG(
            dag_id='test_xcom',
            schedule_interval='@monthly',
            start_date=datetime.datetime.now(),
        )

        d = DownloadOperator(task_id='bar',
                             start_date=datetime.datetime.now(),
                             base_url='',
                             dag=dag)

        class MockClass:
            def xcom_pull(self, key, task_ids):
                return {"final_start_date_str": '20210501', "final_end_date_str": '20210501', "final_hour_start": 10, "final_hour_end": 11}

        d.execute(context={'task_instance': MockClass()})
        self.assertTrue(os.path.isfile('/tmp/test.gz'))
        self.assertTrue(os.path.getsize('/tmp/test.gz') != 0)

    def tearDown(self):
        if os.path.isfile('/tmp/test.gz'):
            os.remove('/tmp/test.gz')