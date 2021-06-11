# Wikipedia Page View report generator

### Key requirements
Following are the key requirements:
* The application should be able to accept input of `date` and `hour`
* The application should download the data for the defined input only if it has not been loaded previously
* Generate the report for input dates to find out 25 most viewed pages for wikipedia sub-domains

### Application architecture
While developing the data pipeline for above requirements I have designed the application using existing workflow management tool [Airflow](https://airflow.apache.org/).

Key benefits of Airflow (from their website):
* Pure Python
* Useful UI
* Robust integration
* Easy to use
* Open source

I have also used `docker` to start up different services required -
- Postgres database
- Airflow scheduler
- Airflow web server

The overall DAG for the application (data pipeline) will look like below
![DAG](./images/dag.png)

Description of each task:

- **verify_input**: Will verify provided input of date and hour from `conf` argument. This step will format the input in useful format and push the data for the tasks to use using [Xcom]( https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html).

- **create_schema**: will create required tables and load `backlist` data in tables (will check if we have already loaded data before)

- **download_data_task** : will download the files from the remote repository only if we have not loaded the files previously. Since this is an I/O bound operations I have decided to use python [multithreading](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.ThreadPool) to speed up the process.

- **unzip_data**: Since the newly downloaded files will be compressed, this step will uncompress those files and prepare file for the data load.

- **upload_data_task**: will load the data into Postgres database `wiki`. This will not load any data with blacklisted pages.

- **dir_clean_up**; This will clean up the directory where we had downloaded the files to free up the space.

- **generate_report**: Once the data is properly loaded we can generate the report. The report will be available under `reports` directory of the repository



## How to run the DAG
Please follow the below process to start the application. You need to have Docker installed on your machine for the application to run.

- Kickstart the services. Run the below command from within the repository
```
docker-compose up
```
- Check if all the services are up
```
docker ps
```
You should see 3 services running. If not, please kill the previous process and try starting the services again and wait for a few minutes 

- If all the three services are up and running you should be able to see the DAG on [localhost](http://localhost:8080/admin/)

- Turn `ON` the toggle for DAG to make to it schedulable

- There are two options to execute the DAG
	- **Using the command line**
	 Run below command to kick off dag using command line
	 ```shell script
	 docker exec -it wikipiedia_scheduler_1 airflow dags trigger --conf '{}' report_generator_dag 
	 ```
    With arguments:
    ```shell script
    docker exec -it wikipiedia_scheduler_1 airflow dags trigger --conf '{
          "start_date": 20210601,
          "end_date": 20210601,
          "start_hour": 1,
          "end_hour": 2
        }' report_generator_dag 
    ```
 
	- ** UI **

      Click on Trigger Dag button and pass in configuration as JSON like
      ```json
        {
          "start_date": 20210501,
          "end_date": 20210501,
          "start_hour": 1,
          "end_hour": 2
        }
      ``` 
      
 NOTE : If you do not pass in the arguments it will take previous 24 hours as default argument. Also, default timezone
 is 'UTC'
 
 - To gracefully stop the services please run below command 
 ```shell script
docker-compose down --volumes
```
 * On completion of the DAG you should see the new reports under `/reports` directory

## Run tests
There are a few tests included in the repo to test out the basic functionality:
* If services are getting up correctly using `docker-compose.yml` file. Run the following commands (in order)
```shell script
python3 -m venv my_test
source my_test/bin/activate
pip install testcontainers pytest
pytest test/test_docker_compose.py -v
```

* To test functionality of custom operators. This is actually a two step tests (well suited for CI along with Unit tests)

Step 1:  Run below command in terminal
```shell script
docker-compose up webserver
```
Step 2:  Run below command to run the tests against the container
```shell script
docker exec -it wikipiedia_webserver_1 bash -c "cd /opt/airflow  && pip install pytest && pytest test/test_dag_load.py test/test_operators.py"

```

Cool things about the data pipeline:
* It is fully automated data pipeline which just needs input at the run time.
* Tasks which can fail have set with retry parameters(download files) so we need not to run the DAG again
* It has by default logging enabled so we can have a look at each task logs.
* It should never ingest duplicated data in the database so we can trust the data as well as reports
* In order to have code quality I have used `pylint` and have defined custom `pylintrc` file. We can check by running
* should clean up the directories after loading into the database

```shell script
pylint --rcfile .pylintrc utils/
pylint --rcfile .pylintrc dags/
pylint --rcfile .pylintrc operators/
```

You should see all the test cases passing if executed correctly 
 
 
## Improvements possible
- Some code is hard to test without running a service. We can test these in CI enviornment to kick off service and run tests
- We can set it to run at the schedule interval to generate reports
- Integrate airflow with DataDog to have logs accessible. Also each DAG can send events after completion and then we can set up Datadog monitors for alerts.
- Run on a cluster and have a backend to be a data warehouse
- Upload data is sequential which often becomes a bottleneck. Depending on the system/warehouse we can parallelize this and even upload several files in a single step
- Currently, it is generating report locally but we can configure to generate and ship the file directly in S3 

## Assumption
- Have used Mac for development. All the code examples are based on mac environment.

