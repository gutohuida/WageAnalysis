#!/bin/bash 

echo "Shutting prefect down..."
kill $(ps aux | grep '[p]ython source/prefect/scrapper_prefect.py' | awk '{print $2}')
kill $(ps aux | grep '[p]ython source/prefect/get_exchange.py' | awk '{print $2}')
kill $(ps aux | grep '[p]refect server start' | awk '{print $2}')
sleep 10
echo "Shutting Postgres down..."
docker compose -f Postgres/docker-compose.yaml down