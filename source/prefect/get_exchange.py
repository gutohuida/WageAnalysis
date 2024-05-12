import requests
import ast
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
from datetime import datetime, timezone

CONNECTION_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/wageanalysis"

EXCHANGE_CURRENCY = ["USD", "BRL"]
EXCHANGE_API = [(f"https://open.er-api.com/v6/latest/{x}", x) for x in EXCHANGE_CURRENCY]

@task
def creat_db_connection():
    postgresql_url = CONNECTION_URL
    engine = create_engine(postgresql_url)
    
    return engine

@flow
def get_exchange():

    logging = get_run_logger()
    logging.info('Getting currency exchange for the day')
    engine = creat_db_connection()
    for api in EXCHANGE_API:
        try:
            usd_exchange = requests.get(api[0])
        except Exception as e:
            logging.error(f"Could not get exchange rate USD: {e}")
            return
            
        currency_exchange = pd.DataFrame()
        result_dict = ast.literal_eval(usd_exchange.content.decode('utf-8'))

        for key in result_dict["rates"]:
            line = {
                "currency_from": key,
                "currency_to": api[1],
                "exchange_rate": result_dict["rates"][key],
                "insert_date": datetime.now().astimezone(timezone.utc)
            }
            currency_exchange = pd.concat([currency_exchange, pd.DataFrame(line, index=[0])], ignore_index=True)

        currency_exchange.to_sql('currency_exchange', engine, schema='raw', if_exists='append', index=False)


if __name__ == "__main__":
    get_exchange.serve(
        name="get-exchange",
        cron="30 23 * * 0-4",
        tags=["exchange", "wage-analysis"],
        description="Get the exchange value for BRL and USD daily.",
        version="wage-analysis/deployment",
    )