#!/bin/bash 

echo "Runing Postgres server..."
docker compose -f Postgres/docker-compose.yaml up &
sleep 15
source ./.venv/bin/activate
echo "Starting prefect server..."
prefect server start &
sleep 20
cd source
echo "Deploying code..."
python -m prefect_dags.glassdoor.scrapper_prefect &
python -m prefect_dags.exchange.get_exchange &
python -m prefect_dags.numbeo.numbeo_scrapper &