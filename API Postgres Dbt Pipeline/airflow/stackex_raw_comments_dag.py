from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.models import Variable # type: ignore
from psycopg2.extras import Json
from datetime import datetime
import requests
import time

POSTGRES_CONN_ID = Variable.get("POSTGRES_RAW_CONN_ID")
RAW_SCHEMA = Variable.get("RAW_SCHEMA")
API_KEY = Variable.get("STACKEX_API_KEY")

SITE = Variable.get("STACKEX_SITE", default_var="stackoverflow")
PAGE_SIZE = int(Variable.get("STACKEX_PAGE_SIZE", default_var=100))
FROM_DATE = int(datetime(2026, 1, 1).timestamp())

API_URL = "https://api.stackexchange.com/2.3/comments"

def extract_comments():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    page = 1
    has_more = True

    while has_more:
        params = {
            "site": SITE,
            "pagesize": PAGE_SIZE,
            "page": page,
            "fromdate": FROM_DATE,
            "key": API_KEY
        }

        resp = requests.get(API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            cur.execute(
                f"""
                INSERT INTO {RAW_SCHEMA}.comments
                (comment_id, post_id, site, data, creation_date, extracted_at)
                VALUES (%s, %s, %s, %s, to_timestamp(%s), now())
                ON CONFLICT (comment_id, site) DO NOTHING
                """,
                (
                    item["comment_id"],
                    item["post_id"],
                    SITE,
                    Json(item),
                    item["creation_date"]
                )
            )

        conn.commit()

        has_more = data.get("has_more", False)
        page += 1
        time.sleep(0.3)

with DAG(
    dag_id="stackex_raw_comments",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stackexchange", "raw", "api"],
) as dag:

    extract = PythonOperator(
        task_id="extract_comments",
        python_callable=extract_comments
    )

    extract
