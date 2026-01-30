from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.hooks.base import BaseHook # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta
from vnstock import Company
from sqlalchemy import create_engine
import pandas as pd
import time, warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

# --- cấu hình chung ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --- đọc config từ Airflow ---
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_stockdb")
TABLE_NAME = Variable.get("DIM_SHARE_STRUCTURE_TABLE", default_var="dim_share_structure")
SLEEP_SECONDS = int(Variable.get("VNSTOCK_SLEEP_SECONDS", default_var=1))


# --- hàm tiện ích ---
def safe_get(data, key):
    val = data.get(key)
    if isinstance(val, (list, pd.Series)):
        return val[0] if len(val) > 0 else None
    return val


def parse_float(val):
    try:
        if val is None or pd.isna(val):
            return None
        return float(str(val).replace(",", "").strip())
    except Exception:
        return None


def get_postgres_engine(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    return create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )


# --- hàm chính ---
def load_dim_share_structure_to_db(**kwargs):
    engine = get_postgres_engine(POSTGRES_CONN_ID)

    dim_stock = pd.read_sql(
        "SELECT stock_id, symbol FROM dim_stock",
        engine
    )
    print(f"Đã đọc {len(dim_stock)} mã cổ phiếu từ dim_stock")

    records = []

    for i, row in dim_stock.iterrows():
        sym = row["symbol"]
        stock_id = row["stock_id"]

        # --- TCBS ---
        try:
            tcb_info = Company(symbol=sym, source="TCBS").overview()
            issue_share = parse_float(safe_get(tcb_info, "issueShare"))
        except Exception:
            issue_share = None

        # --- VCI ---
        try:
            time.sleep(SLEEP_SECONDS)
            vci_info = Company(symbol=sym, source="VCI").overview()
            charter_capital = parse_float(safe_get(vci_info, "charter_capital"))
            vci_issue_share = parse_float(safe_get(vci_info, "issue_share"))
            if issue_share is None:
                issue_share = vci_issue_share
            financial_ratio = parse_float(
                safe_get(vci_info, "financial_ratio_issue_share")
            )
        except Exception:
            charter_capital = financial_ratio = None

        records.append({
            "share_id": i + 1,
            "stock_id": stock_id,
            "issue_share": issue_share,
            "charter_capital": charter_capital,
            "financial_ratio_issue_share": financial_ratio,
        })

        if i % 50 == 0:
            print(f"Đã xử lý {i}/{len(dim_stock)} mã...")

    df = pd.DataFrame(records)

    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
    print(f"✅ Đã ghi {len(df)} bản ghi vào bảng '{TABLE_NAME}'")


# --- DAG ---
with DAG(
    dag_id="vnstock_el_weekly_dim_share_structure",
    default_args=default_args,
    description="Weekly update dim_share_structure from vnstock to PostgreSQL",
    schedule_interval="@weekly",
    start_date=datetime(2025, 10, 25),
    catchup=False,
    max_active_runs=1,
    tags=["EL", "weekly"],
) as dag:

    update_data = PythonOperator(
        task_id="extract_and_load_share_structure",
        python_callable=load_dim_share_structure_to_db,
    )

    update_data
