#!/bin/bash 

echo "Shutting prefect down..."
kill $(ps aux | grep '[p]ython -m prefect_dags.glassdoor.scrapper_prefect' | awk '{print $2}')
kill $(ps aux | grep '[p]ython -m prefect_dags.exchange.get_exchange' | awk '{print $2}')
kill $(ps aux | grep '[p]ython -m prefect_dags.numbeo.numbeo_scrapper' | awk '{print $2}')
kill $(ps aux | grep '[p]refect server start' | awk '{print $2}')
sleep 10
echo "Shutting Postgres down..."
docker compose -f Postgres/docker-compose.yaml down