import logging
from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator, SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json

from flask import Response

logger = logging.getLogger(__name__)


def _next_page(response: Response) -> dict:
    next_page: int = response.json().get('next_page')
    logger.info(f'------------------------------------------ {next_page}')
    if next_page:
        return json.dumps({
            "url": "https://www.google.com/search?q=site%3Ainstagram.com+AND+Watches+AND+%22%40gmail.com%22",
            "page": next_page,
            "num": 20
        })


with DAG(
    "send_url_to_scrap",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    is_scrapper_available = HttpSensor(
        task_id="is_scrapper_available", http_conn_id="scrapper_api", endpoint="/docs"
    )

    # send_url_to_scrap = SimpleHttpOperator(
    #     task_id="send_url_to_scrap",
    #     http_conn_id="scrapper_api",
    #     endpoint="influencers/from_url",
    #     method="POST",
    #     data=json.dumps({
    #         "url": "https://www.google.com/search?q=site%3Ainstagram.com+AND+Watches+AND+%22%40gmail.com%22",
    #         "page": "1",
    #         "num": 20
    #     }),
    #     response_filter=lambda response: json.loads(response.text),
    #     dag=dag,
    #     log_response=True,
    # )

    send_url_to_scrap = HttpOperator(
        task_id="send_url_to_scrap",
        http_conn_id="scrapper_api",
        method="POST",
        endpoint="influencers/from_url",
        data=json.dumps({
            "url": "https://www.google.com/search?q=site%3Ainstagram.com+AND+Watches+AND+%22%40gmail.com%22",
            "page": 0,
            "num": 20
        }),
        pagination_function=_next_page,
    )

    is_scrapper_available >> send_url_to_scrap
