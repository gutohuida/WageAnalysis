#!/bin/bash 

echo "Runing Postgres server..."
docker compose -f Postgres/docker-compose.yaml up &
sleep 15
source ./.venv/bin/activate
echo "Starting prefect server..."
prefect server start &
sleep 20
echo "Deploying code..."
python source/prefect/scrapper_prefect.py &
python source/prefect/get_exchange.py &
