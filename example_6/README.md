mkdir dags logs plugins processed_data raw_data
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
docker-compose up

http://localhost:8080
airflow/airflow.