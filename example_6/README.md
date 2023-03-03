echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
docker-compose up

http://localhost:8080
airflow/airflow.


Plan:
1. Full import Users
2. Incremental import Orders

Do three things for FDE:
1. Handle time stamped order stuff (before just append, then partition)
2. Handle full import of users (partition)
3. Add a view on top..

For second tut:
1. Handle late arriving facts
2. Use tax example to keep logic in data.
