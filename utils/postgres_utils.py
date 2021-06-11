"""Module to interact with Postgres Database"""


import os
import glob
from datetime import datetime, timedelta
import logging
import psycopg2 #pylint: disable=E0401

from airflow.hooks.postgres_hook import PostgresHook #pylint: disable=E0401

hook = PostgresHook('POSTGRES_DATADOG')
conn = hook.get_connection(conn_id='POSTGRES_DATADOG')


def get_connection():
    """ Get postgres connection

        Returns: psycopg2.connect object

    """
    return  psycopg2.connect(user=conn.login,
                             password=conn.password,
                             host=conn.host,
                             port=conn.port,
                             database=conn.schema)


def create_schema(**kwargs):  # pylint: disable=E0401, W0613
    """ Creates schema for postgres database"""

    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(open("/opt/airflow/scripts/schema.sql", "r").read())

        cursor.execute("""
            SELECT 
                CASE WHEN EXISTS (SELECT * FROM blacklist_domains LIMIT 1) THEN 1
                    ELSE 0 
                END
            ;
        """)
        result = cursor.fetchone()[0]

        if result != 1:
            logging.info('Inserting data in back list table')
            cursor.execute(r"""
                COPY blacklist_domains(domain, page)
                FROM '/opt/airflow/data/blacklist_domains_and_pages.tsv'
                DELIMITER E' '
                ;
            """)

        connection.commit()


def upload_data(**kwargs):
    """ Upload data into the database tables

        Args:
             **kwargs: Arbitrary keyword arguments.
    """
    try:
        connection = get_connection()

        connection.autocommit = False
        for file in glob.glob("/opt/airflow/data/tmp/*"):

            if file.endswith('.gz') or file.endswith('.tmp'):
                continue

            with connection.cursor() as cursor:
                logging.info('Loading file %s ', file)

                # create temp table to load raw data
                cursor.execute("""create temp table page_view(
                    domain varchar(20),
                    page text,
                    page_view_count integer,
                    bytes integer
                )""")

                # use COPY command to load data into temp table
                cursor.execute(r"""
                    COPY page_view(domain,page, page_view_count, bytes)
                    FROM PROGRAM 'sed ''s/\\/\\\\/g'' < %s'
                    DELIMITER E' '
                    ;
                """ % file)

                # extract date from the file name
                file_part = file.split('-')
                date_extracted_from_file = int(file_part[1])
                hour = int(file_part[2][:2])

                # Insert data into main table along with date
                # Will do left join with blacklist_domains to avoid inserting any
                # page present in blacklist_domains
                cursor.execute(r"""
                        insert into main_page_view
                            select 
                                %s,
                                %s,
                                main.domain,
                                main.page,
                                main.page_view_count
                            from page_view main
                            left join blacklist_domains blacklist
                                on main.page = blacklist.page
                            where blacklist.page is null
                        """ % (date_extracted_from_file, hour))

                cursor.execute("""
                        insert into file_track
                        values (%s, %s, '%s.gz')
                """ % (date_extracted_from_file, hour, os.path.basename(file)))

                cursor.execute(r"drop table page_view ")
                connection.commit()
                os.rename(file, file + '.tmp')

            logging.info('File %s loaded successfully', file)
    except Exception:
        connection.rollback()
        raise
    finally:
        if connection is not None:
            connection.close()


def generate_report(**kwargs):
    """ Generates the report locally under `reports/` directory

        Args:
             **kwargs: Arbitrary keyword arguments.
    """

    sql = """
        COPY
            (
                select page, domain, total_page_count
                from (
                    select page, domain, sum(page_view_count) as total_page_count
                    from main_page_view
                    where date = {date}
                    and hour = {hour}
                    group by page, domain
                    order by 2 desc
                    limit 25
                    )a
                    order by domain
            )
            TO
            '/opt/airflow/reports/{report_name}.csv'
            csv header
    """
    input_date_dict = kwargs['ti'].xcom_pull(
        key="input_date_hour_dict", task_ids='verify_input')
    connection = get_connection()

    with connection.cursor() as cursor:

        start_date_hour = datetime.strptime(input_date_dict['final_start_date_str'], '%Y%m%d').replace(
            hour=input_date_dict['final_hour_start'])
        end_date_hour = datetime.strptime(input_date_dict['final_end_date_str'], '%Y%m%d').replace(
            hour=input_date_dict['final_hour_end'])

        d = start_date_hour
        while d < end_date_hour:
            curr_date_hour_str = d.strftime('%Y%m%d')
            report_name = curr_date_hour_str + '_' + str(d.hour)
            if os.path.exists(f'/opt/airflow/reports/{report_name}.csv'):
                # check if the report is already present
                d = d + timedelta(hours=1)
                logging.info('Report %s is already present. Skipping generation of new report', report_name)
                continue

            logging.info('Generating report %s.csv', report_name)
            cursor.execute(sql.format(**{'report_name': report_name,
                                         'date': int(curr_date_hour_str),
                                         'hour': int(d.hour)}))
            d = d + timedelta(hours=1)


def get_existing_file(date):
    """ Utility function to get existing files which were already loaded successfully.

       Args:
            date(str): Date string filter

        Returns:
            Existing file list
   """
    existing_files = """select file_name from file_track where date in ( {date} )"""

    connection = get_connection()
    existing_file_list = []
    with connection.cursor() as cursor:
        logging.info(date)
        cursor.execute(existing_files.format(**{'date': date}))

        for row in cursor:
            existing_file_list.append(row[0])

    logging.info('Existing file list %s', str(existing_file_list))
    return existing_file_list
