from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.hooks.base import BaseHook # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta
import pandas as pd
import time
from vnstock import Listing, Company
from sqlalchemy import create_engine


# --- cấu hình chung ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --- đọc config từ Airflow ---
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_stockdb")
DIM_STOCK_TABLE = Variable.get("DIM_STOCK_TABLE", default_var="dim_stock")
SLEEP_SECONDS = int(Variable.get("VNSTOCK_SLEEP_SECONDS", default_var=1))


# --- hàm tiện ích ---
def safe_get(data, key):
    val = data.get(key)
    if isinstance(val, (list, pd.Series)):
        return val[0] if len(val) > 0 else None
    return val


def get_postgres_engine(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    return create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )


# --- hàm chính ---
def load_dim_stock_to_db(**kwargs):
    listing = Listing()
    symbols_df = (
        listing.all_symbols()
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    records = []

    for i, row in symbols_df.iterrows():
        sym = row["symbol"]
        organ_name = row["organ_name"]

        # --- TCBS ---
        try:
            tcb_info = Company(symbol=sym, source="TCBS").overview()
            exchange = safe_get(tcb_info, "exchange")
        except Exception:
            exchange = None

        # --- VCI ---
        try:
            time.sleep(SLEEP_SECONDS)
            vci_info = Company(symbol=sym, source="VCI").overview()
            icb2 = safe_get(vci_info, "icb_name2")
            icb3 = safe_get(vci_info, "icb_name3")
            icb4 = safe_get(vci_info, "icb_name4")
        except Exception:
            icb2 = icb3 = icb4 = None

        records.append({
            "symbol": sym,
            "organ_name": organ_name,
            "exchange": exchange,
            "icb_name2": icb2,
            "icb_name3": icb3,
            "icb_name4": icb4,
        })

        if i % 50 == 0:
            print(f"Đã xử lý {i}/{len(symbols_df)} mã...")

    df = pd.DataFrame(records)
    df.insert(0, "stock_id", range(1, len(df) + 1))

    engine = get_postgres_engine(POSTGRES_CONN_ID)
    df.to_sql(DIM_STOCK_TABLE, engine, if_exists="replace", index=False)

    print(f"✅ Đã ghi {len(df)} bản ghi vào bảng '{DIM_STOCK_TABLE}'")


# --- DAG ---
with DAG(
    dag_id="vnstock_el_weekly_dim_stock",
    default_args=default_args,
    description="Weekly update dim_stock data from vnstock to PostgreSQL",
    schedule_interval="@weekly",
    start_date=datetime(2025, 10, 25),
    catchup=False,
    max_active_runs=1,
    tags=["EL", "weekly"],
) as dag:

    update_data = PythonOperator(
        task_id="extract_and_load_dim_stock",
        python_callable=load_dim_stock_to_db,
    )

    update_data
