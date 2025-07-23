#!/bin/bash

airflow db migrate

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

airflow scheduler &
airflow webserver