from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta, timezone
import requests
import json
import time

# =====================
# Airflow Variables
# =====================
POSTGRES_CONN_ID = Variable.get("POSTGRES_RAW_CONN_ID")
RAW_SCHEMA = Variable.get("RAW_SCHEMA")
API_KEY = Variable.get("STACKEX_API_KEY")

SITE = Variable.get("STACKEX_SITE", default_var="stackoverflow")
PAGE_SIZE = int(Variable.get("STACKEX_PAGE_SIZE", default_var=100))
MAX_PAGE = int(Variable.get("STACKEX_MAX_PAGE", default_var=25))

DEFAULT_FROMDATE = datetime.fromisoformat(
    Variable.get("STACKEX_DEFAULT_FROMDATE", default_var="2026-01-01")
).replace(tzinfo=timezone.utc)

API_NAME = "questions"
API_URL = "https://api.stackexchange.com/2.3/questions"

DAG_ID = "stackex_raw_questions"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# =====================
# Task
# =====================
def extract_questions():
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    # 1️⃣ Load checkpoint
    cur.execute(f"""
        SELECT last_to_date
        FROM {RAW_SCHEMA}.api_checkpoint
        WHERE api_name = %s
    """, (API_NAME,))
    row = cur.fetchone()

    from_date = row[0] if row and row[0] else DEFAULT_FROMDATE
    to_date = datetime.now(tz=timezone.utc)

    from_ts = int(from_date.timestamp())
    to_ts = int(to_date.timestamp())

    print(f"👉 Fetch {API_NAME} from {from_date} → {to_date}")

    page = 1
    has_more = True
    max_creation_ts = from_ts

    while has_more and page <= MAX_PAGE:
        params = {
            "site": SITE,
            "pagesize": PAGE_SIZE,
            "page": page,
            "fromdate": from_ts,
            "todate": to_ts,
            "order": "asc",
            "sort": "creation",
            "key": API_KEY,
        }

        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        has_more = data.get("has_more", False)

        if not items:
            break

        for q in items:
            creation_ts = q["creation_date"]
            max_creation_ts = max(max_creation_ts, creation_ts)

            cur.execute(f"""
                INSERT INTO {RAW_SCHEMA}.questions (
                    question_id,
                    site,
                    data,
                    creation_date,
                    extracted_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    to_timestamp(%s),
                    now()
                )
                ON CONFLICT (question_id, site)
                DO UPDATE SET
                    data = EXCLUDED.data,
                    extracted_at = now()
            """, (
                q["question_id"],
                SITE,
                json.dumps(q),
                creation_ts,
            ))

        conn.commit()
        page += 1

        if "backoff" in data:
            time.sleep(data["backoff"])

    # 2️⃣ Update checkpoint
    cur.execute(f"""
        INSERT INTO {RAW_SCHEMA}.api_checkpoint (
            api_name,
            last_to_date,
            updated_at
        )
        VALUES (
            %s,
            to_timestamp(%s),
            now()
        )
        ON CONFLICT (api_name)
        DO UPDATE SET
            last_to_date = EXCLUDED.last_to_date,
            updated_at   = now()
    """, (API_NAME, max_creation_ts))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Extract questions completed")

# =====================
# DAG
# =====================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["stackexchange", "raw", "api"],
) as dag:

    extract_questions_task = PythonOperator(
        task_id="extract_questions",
        python_callable=extract_questions,
    )

    extract_questions_task
