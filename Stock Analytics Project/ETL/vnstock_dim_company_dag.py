from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import time
import pandas as pd
import warnings
import re
from sqlalchemy import create_engine
from vnstock import Company
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore


warnings.simplefilter(action='ignore', category=FutureWarning)

# --- Cấu hình DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

TABLE_NAME = "dim_company"


# ==============================
#   HÀM XỬ LÝ DỮ LIỆU
# ==============================

def get_value_safe(data, key):
    """Trích xuất giá trị an toàn từ dict"""
    val = data.get(key, None)
    if isinstance(val, pd.Series):
        return val.iloc[0]
    elif isinstance(val, list):
        return val[0] if len(val) > 0 else None
    return val


def format_history(history):
    """Làm sạch chuỗi lịch sử công ty"""
    if history is None or pd.isna(history):
        return None
    text = str(history).strip()
    text = re.sub(r'([.!])\s*(-\s*)', r'\1\n-', text)
    text = text.replace('- ', '\n- ')
    return '\n'.join(line.strip() for line in text.split('\n')).strip()


def format_capital(cap):
    """Chuyển vốn điều lệ sang kiểu số"""
    if cap is None or pd.isna(cap):
        return None
    try:
        return int(float(str(cap).replace(',', '').strip()))
    except Exception:
        return None


def load_dim_company_to_db(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_stockdb")
    engine = postgres_hook.get_sqlalchemy_engine()

    dim_stock = pd.read_sql("SELECT * FROM dim_stock", engine)
    print(f"Đọc {len(dim_stock)} mã cổ phiếu từ dim_stock")

    records = []
    for i, row in dim_stock.iterrows():
        sym, organ_name, stock_id = row['symbol'], row['organ_name'], row['stock_id']

        # --- TCBS ---
        try:
            tcb_info = Company(symbol=sym, source='TCBS').overview()
            website = get_value_safe(tcb_info, 'website')
            if isinstance(website, (list, pd.Series)):
                website = website[0] if len(website) > 0 else None
        except Exception:
            website = None

        # --- VCI ---
        try:
            time.sleep(1.5)
            vci_info = Company(symbol=sym, source='VCI').overview()
            vci_id = get_value_safe(vci_info, 'id')
            history = get_value_safe(vci_info, 'history')
            charter_capital = get_value_safe(vci_info, 'charter_capital')
        except Exception:
            vci_id = history = charter_capital = None

        records.append({
            'company_id': i + 1,
            'stock_id': stock_id,
            'company_profile': organ_name,
            'history': format_history(history),
            'charter_capital': format_capital(charter_capital),
            'website': website,
            'vci_internal_id': vci_id,
        })

        if i % 20 == 0:
            print(f"Đã xử lý {i}/{len(dim_stock)} mã...")

    df = pd.DataFrame(records)
    df.to_sql("dim_company", engine, if_exists='replace', index=False)

    print(f"Đã ghi {len(df)} bản ghi vào bảng dim_company")


# ==============================
#   ĐỊNH NGHĨA DAG
# ==============================

with DAG(
    dag_id="vnstock_el_weekly_dim_company",
    default_args=default_args,
    description="Weekly update dim_company from vnstock to PostgreSQL",
    schedule_interval="@weekly",
    start_date=datetime(2025, 10, 25),
    catchup=False,
    max_active_runs=1,
    tags=["EL", "weekly"],
) as dag:

    update_data = PythonOperator(
        task_id="extract_and_load_company",
        python_callable=load_dim_company_to_db,
    )

    update_data
