#!/usr/bin/env bash
# 1. Run ```docker-compose up airflow-init``` to initialize the db.
echo "======="
echo "Run ... docker-compose up airflow-init ... to initialize the db."
echo "======="

docker-compose up airflow-init

echo "======="
echo "# Run ... ./day-1 ... to set the time to day 1. Open up: http://localhost:8080 (pwd and users: airflow/airflow)."
echo "======="

./day-1

# 2. Run ```docker-compose up``` to open up airflow (on start up, jupyter_1 will dump a token, copy it for the next step!)
# 3. Run ```./day-1``` to set the time to day 1. Open up: http://localhost:8080 (pwd and users: airflow/airflow).
echo "======="
echo "Run ... docker-compose up ... to open up airflow (on start up, jupyter_1 will dump a token, copy it for the next step!)"
echo "======="
docker-compose up