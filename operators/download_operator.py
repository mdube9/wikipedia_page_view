"""Module to download files"""

from pathlib import Path
from multiprocessing.pool import ThreadPool
from datetime import datetime, timedelta
import os
import time
import requests


from airflow.models.baseoperator import BaseOperator  # pylint: disable=E0401

from utils import postgres_utils


class DownloadOperator(BaseOperator):
    """" Operator to download files
    """
    def __init__(
            self,
            base_url: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.failed_downloads = []
        self.base_url = base_url

    def execute(self, context):
        """Method which will be called to execute task"""

        def _download_helper(url):
            """ Utility function to download a file

               Args:
                    url(str): File URL to download
            """
            filename = url.split("/")[-1]
            retries = 3
            retry = 1
            while retry < retries:
                file = os.path.join('/opt/airflow/data/tmp', filename)
                with open(file, "wb") as f:
                    r = requests.get(url)
                    f.write(r.content)
                    f.flush()
                if r.status_code != 200:
                    retry += 1
                    os.remove(file)
                    if retry == 3:
                        self.failed_downloads.append(url)
                        self.log.info('Unable to download file %s in 3 attempts. Marking it as failed download')
                        break
                    time.sleep(15)
                    self.log.error('Error Occurred. Will try again %s, retry_attempt %s', r.text, retry)
                else:
                    break
            self.log.info('Finished downloading file %s' % filename)

        Path("/opt/airflow/data/tmp").mkdir(parents=True, exist_ok=True)  # to make sure path is present.

        input_date_dict = context['task_instance'].xcom_pull(key="input_date_hour_dict", task_ids='verify_input')

        start_date_hour = datetime.strptime(input_date_dict['final_start_date_str'], '%Y%m%d').replace(
            hour=input_date_dict['final_hour_start'])
        end_date_hour = datetime.strptime(input_date_dict['final_end_date_str'], '%Y%m%d').replace(
            hour=input_date_dict['final_hour_end'])

        delta = end_date_hour - start_date_hour  # as timedelta
        days = ','.join([(start_date_hour + timedelta(days=i)).strftime('%Y%m%d') for i in range(delta.days + 1)])

        urls = []
        get_existing_files = []
        if days:
            get_existing_files = postgres_utils.get_existing_file(days)

        d = start_date_hour
        while d < end_date_hour:
            curr_date_hour_str = d.strftime('%Y%m%d')
            file_name_to_download = f'pageviews-{curr_date_hour_str}-{d.hour:02d}0000.gz'
            if file_name_to_download not in get_existing_files:
                url = f"{self.base_url}/{d.year}/{d.year}-{d.month:02d}/{file_name_to_download}"
                urls.append(url)
            d = d + timedelta(hours=1)

        self.log.info('Total files to download: %s ', len(urls))

        pool = ThreadPool(4)
        pool.map(_download_helper, urls)
        pool.close()
        pool.join()

        # If files were in failed status we have to download them again
        if self.failed_downloads:
            failed_pool = ThreadPool(4)
            failed_pool.map(_download_helper, self.failed_downloads)
            failed_pool.close()
            failed_pool.join()
