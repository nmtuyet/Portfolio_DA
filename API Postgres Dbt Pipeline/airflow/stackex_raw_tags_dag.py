from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.models import Variable     # type: ignore
from datetime import datetime, timezone
from psycopg2.extras import Json
import requests
import time

POSTGRES_CONN_ID = Variable.get("POSTGRES_RAW_CONN_ID")
RAW_SCHEMA = Variable.get("RAW_SCHEMA")
API_KEY = Variable.get("STACKEX_API_KEY")

SITE = Variable.get("STACKEX_SITE", default_var="stackoverflow")
PAGE_SIZE = int(Variable.get("STACKEX_PAGE_SIZE", default_var=100))

API_URL = "https://api.stackexchange.com/2.3/tags"

def extract_tags(**context):
    page = 1
    has_more = True
    items = []

    while has_more:
        params = {
            "site": SITE,
            "key": API_KEY,
            "pagesize": PAGE_SIZE,
            "page": page,
            "order": "desc",
            "sort": "popular"
        }

        r = requests.get(API_URL, params=params, timeout=30)
        r.raise_for_status()
        res = r.json()

        items.extend(res.get("items", []))
        has_more = res.get("has_more", False)
        page += 1
        time.sleep(0.2)

    context["ti"].xcom_push(key="tags", value=items)

def load_tags(**context):
    items = context["ti"].xcom_pull(key="tags")
    if not items:
        return

    extracted_at = datetime.now(timezone.utc)
    hook = PostgresHook(POSTGRES_CONN_ID)

    # 1️⃣ Deduplicate theo tag_name
    unique_tags = {}
    for item in items:
        unique_tags[item["name"]] = item

    hook.run(f"TRUNCATE TABLE {RAW_SCHEMA}.tags")

    rows = [
        (
            tag_name,
            SITE,
            Json(tag_obj),
            extracted_at
        )
        for tag_name, tag_obj in unique_tags.items()
    ]

    hook.insert_rows(
        table=f"{RAW_SCHEMA}.tags",
        rows=rows,
        target_fields=[
            "tag_name",
            "site",
            "data",
            "extracted_at"
        ]
    )

with DAG(
    dag_id="stackex_raw_tags",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["stackexchange", "raw", "api"]
) as dag:

    extract = PythonOperator(
        task_id="extract_tags",
        python_callable=extract_tags
    )

    load = PythonOperator(
        task_id="load_tags",
        python_callable=load_tags
    )

    extract >> load
