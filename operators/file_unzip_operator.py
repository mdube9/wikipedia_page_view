"""Module to Unzip files"""

from multiprocessing.pool import ThreadPool
import shutil
import os
import gzip
import glob

from airflow.models.baseoperator import BaseOperator #pylint: disable=E0401


class UnZipOperator(BaseOperator):
    """ Class to unzip files"""

    def execute(self, context=None): #pylint: disable=W0613,R0201
        """Method which will be called to execute task"""

        def unzip_file_helper(file):
            """ Will unzip the compressed files which were just downloaded

               Args:
                    file (str) : File to un-zip
            """
            with gzip.open(file, "rb") as f_in:
                with open(file.replace('.gz', ''), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file)

        file_list = glob.glob("/opt/airflow/data/tmp/*.gz")
        pool = ThreadPool(4)
        pool.map(unzip_file_helper, file_list)
        pool.close()
        pool.join()
