""" Module to run ETL for wikipedia pages """

# standard imports
from datetime import datetime, timedelta
import logging
import shutil

# airflow imports
# pylint: disable=E0401
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.exceptions import AirflowException
# pylint: enable=E0401

# Custom imports
from operators.download_operator import DownloadOperator
from operators.file_unzip_operator import UnZipOperator
from utils.postgres_utils import upload_data, create_schema, generate_report


DEFAULT_ARGS = {
    'owner': 'Madhav Dube',
    'start_date': datetime(2021, 1, 1),
}

BASE_URL = "https://dumps.wikimedia.org/other/pageviews/"


def verify_input(**kwargs):
    """ Verify the input date and hour and push the usable params using XCOM

        Args:
             **kwargs: Arbitrary keyword arguments.
    """

    current_date_hour = datetime.now().replace(second=0, minute=0, microsecond=0)
    previous_date_hour = current_date_hour - timedelta(hours=24)

    start_date_input = kwargs['dag_run'].conf.get('start_date', None)
    end_date_input = kwargs['dag_run'].conf.get('end_date', None)

    start_hour_input = kwargs['dag_run'].conf.get('start_hour', None)
    end_hour_input = kwargs['dag_run'].conf.get('end_hour', None)

    any_input_not_null = start_date_input is not None or \
        end_date_input is not None or start_hour_input is not None or \
        end_hour_input is not None

    if any_input_not_null and not all(v is not None for v in [start_date_input, end_date_input, start_hour_input,
                                                              end_hour_input]):
        raise AirflowException('One or more arguments missing.')

    if start_date_input and end_date_input:
        try:
            _input_start_date = datetime.strptime(str(start_date_input), '%Y%m%d').date()
            _input_end_date = datetime.strptime(str(end_date_input), '%Y%m%d').date()
        except ValueError:
            raise ValueError("Incorrect date format, should be YYYYMMDD")
    else:
        _input_start_date = previous_date_hour.date()
        _input_end_date = current_date_hour.date()

    if start_hour_input and end_hour_input:
        try:
            start_hour_input = datetime.strptime(str(start_hour_input), '%H')
            end_hour_input = datetime.strptime(str(end_hour_input), '%H')
        except ValueError:
            raise ValueError("Incorrect hour format, should hh")

        _input_hour_start = start_hour_input.hour
        _input_hour_end = end_hour_input.hour

    else:
        _input_hour_start = previous_date_hour.hour
        _input_hour_end = current_date_hour.hour

    kwargs['ti'].xcom_push(key='input_date_hour_dict',
                           value={'final_hour_start': _input_hour_start,
                                  'final_hour_end': _input_hour_end,
                                  'final_start_date_str': _input_start_date.strftime("%Y%m%d"),
                                  'final_end_date_str': _input_end_date.strftime("%Y%m%d")
                                  })

    logging.info('Reports will run for date(s) %s : %s  and %s : %s', _input_start_date, _input_hour_start,
                 _input_end_date, _input_hour_end)


def dir_clean_up():
    """ Will clean up the directory structure

       Args:
            **kwargs: Arbitrary keyword arguments.
    """
    shutil.rmtree('/opt/airflow/data/tmp/')


with DAG('report_generator_dag',
         default_args=DEFAULT_ARGS,
         schedule_interval=None
         ) as dag:

    download_data_task = DownloadOperator(
        task_id='download_data_task',
        base_url=BASE_URL,
        dag=dag,
    )

    unzip_data = UnZipOperator(
        task_id='unzip_data',
        dag=dag,
    )

    upload_data_task = PythonOperator(
        task_id='upload_data',
        provide_context=True,
        python_callable=upload_data,
        dag=dag,
    )

    generate_report = PythonOperator(
        task_id='generate_report',
        provide_context=True,
        python_callable=generate_report,
        dag=dag,
    )
    #
    create_schema = PythonOperator(
        task_id='create_schema',
        provide_context=True,
        python_callable=create_schema,
        dag=dag,
    )

    dir_clean_up = PythonOperator(
        task_id='dir_clean_up',
        python_callable=dir_clean_up,
        trigger_rule='all_done',
        dag=dag,
    )

    verify_input = PythonOperator(
        task_id='verify_input',
        provide_context=True,
        python_callable=verify_input,
        dag=dag,
    )

    verify_input >> create_schema >> download_data_task >> unzip_data >> upload_data_task >> \
    dir_clean_up >> generate_report
