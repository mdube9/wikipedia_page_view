from unittest import TestCase
from testcontainers.compose import DockerCompose
import requests
import time


class TestDockerCompose(TestCase):

    def test_services_up(self):
        with DockerCompose(filepath='.') as compose:

            time.sleep(10)
            postgres_host = compose.get_service_host("postgres", 5432)
            postgres_port = compose.get_service_port("postgres", 5432)
            assert postgres_host == "0.0.0.0"
            assert postgres_port == "5432"

            airflow_webserver_host = compose.get_service_host("webserver", 8080)
            airflow_webserver_port = compose.get_service_port("webserver", 8080)
            assert airflow_webserver_host == "0.0.0.0"
            assert airflow_webserver_port == "8080"
            assert requests.get('http://localhost:8080/').status_code == 200



