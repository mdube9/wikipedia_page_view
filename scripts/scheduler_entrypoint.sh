#!/bin/sh

pip install -r requirements.txt
export PYTHONPATH="${PYTHONPATH}:/opt/airflow"
airflow scheduler