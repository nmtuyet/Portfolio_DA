from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta, timezone
import requests
import json
import time

# =====================
# Variables
# =====================
POSTGRES_CONN_ID = Variable.get("POSTGRES_RAW_CONN_ID")
RAW_SCHEMA = Variable.get("RAW_SCHEMA")
API_KEY = Variable.get("STACKEX_API_KEY")

SITE = Variable.get("STACKEX_SITE", default_var="stackoverflow")
PAGE_SIZE = int(Variable.get("STACKEX_PAGE_SIZE", default_var=100))
MAX_Q_BATCH = int(Variable.get("STACKEX_MAX_QUESTION_BATCH", default_var=100))

DEFAULT_FROMDATE = datetime.fromisoformat(
    Variable.get("STACKEX_DEFAULT_FROMDATE", default_var="2026-01-01")
).replace(tzinfo=timezone.utc)

API_NAME = "answers"
API_URL_TMPL = "https://api.stackexchange.com/2.3/questions/{ids}/answers"

# =====================
# DAG config
# =====================
DAG_ID = "stackex_raw_answers"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# =====================
# Task logic
# =====================
def extract_answers():
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    # 1️⃣ Get checkpoint (theo question creation)
    cur.execute(f"""
        SELECT last_to_date
        FROM {RAW_SCHEMA}.api_checkpoint
        WHERE api_name = %s
    """, (API_NAME,))
    row = cur.fetchone()

    from_date = row[0] if row and row[0] else DEFAULT_FROMDATE
    to_date = datetime.now(tz=timezone.utc)

    print(f"👉 Fetch answers for questions from {from_date} → {to_date}")

    # 2️⃣ Lấy question_id cần crawl answers
    cur.execute(f"""
        SELECT question_id
        FROM {RAW_SCHEMA}.questions
        WHERE creation_date >= %s
          AND creation_date < %s
        ORDER BY creation_date
    """, (from_date, to_date))

    question_ids = [str(r[0]) for r in cur.fetchall()]

    if not question_ids:
        print("⚠️ No new questions → skip")
        return

    # 3️⃣ Batch question_id (100 ids / call)
    for i in range(0, len(question_ids), MAX_Q_BATCH):
        batch_ids = question_ids[i:i + MAX_Q_BATCH]
        ids_str = ";".join(batch_ids)

        page = 1
        has_more = True

        while has_more:
            params = {
                "site": SITE,
                "pagesize": PAGE_SIZE,
                "page": page,
                "order": "asc",
                "sort": "creation",
                "key": API_KEY,
            }

            url = API_URL_TMPL.format(ids=ids_str)
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            has_more = data.get("has_more", False)

            for a in items:
                cur.execute(f"""
                    INSERT INTO {RAW_SCHEMA}.answers (
                        answer_id,
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
                        %s,
                        to_timestamp(%s),
                        now()
                    )
                    ON CONFLICT (answer_id, site) DO NOTHING
                """, (
                    a["answer_id"],
                    a["question_id"],
                    SITE,
                    json.dumps(a),
                    a["creation_date"],
                ))

            conn.commit()
            page += 1

            # Rate limit
            if "backoff" in data:
                time.sleep(data["backoff"])

    # 4️⃣ Update checkpoint
    cur.execute(f"""
        INSERT INTO {RAW_SCHEMA}.api_checkpoint (
            api_name,
            last_to_date,
            updated_at
        )
        VALUES (%s, %s, now())
        ON CONFLICT (api_name)
        DO UPDATE SET
            last_to_date = EXCLUDED.last_to_date,
            updated_at   = now()
    """, (API_NAME, to_date))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Extract answers completed")

# =====================
# DAG definition
# =====================
with DAG(
    dag_id=DAG_ID,
    start_date=DEFAULT_FROMDATE,
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["stackexchange", "raw", "api"],
) as dag:

    extract_answers_task = PythonOperator(
        task_id="extract_answers",
        python_callable=extract_answers,
    )

    extract_answers_task
