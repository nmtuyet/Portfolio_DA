from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.hooks.base import BaseHook # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock
from sqlalchemy import create_engine, text


# ======================
# AIRFLOW CONFIG
# ======================
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_stockdb")
STOCK_PRICE_TABLE = Variable.get("STOCK_PRICE_TABLE", default_var="stock_prices")
VNSTOCK_SOURCE = Variable.get("VNSTOCK_SOURCE", default_var="VCI")
DEFAULT_START_DATE = Variable.get("VNSTOCK_DEFAULT_START_DATE", default_var="2024-01-01")
SYMBOL_LIMIT = int(Variable.get("STOCK_SYMBOL_LIMIT", default_var="10"))


# ======================
# UTILS
# ======================
def get_postgres_engine(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    return create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )


# ======================
# BUSINESS FUNCTIONS
# ======================
def update_stock_price_nearest(symbol, table_name, engine):
    """Cập nhật dữ liệu cổ phiếu mới nhất cho 1 mã."""
    try:
        query = text(f"SELECT * FROM {table_name} WHERE symbol = :symbol")
        df_old = pd.read_sql(query, engine, params={"symbol": symbol})

        if not df_old.empty and "time" in df_old.columns:
            df_old["time"] = pd.to_datetime(df_old["time"])
            last_date = df_old["time"].max()
            start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = DEFAULT_START_DATE

        stock = Vnstock().stock(symbol=symbol, source=VNSTOCK_SOURCE)
        df_new = stock.quote.history(
            start=start_date,
            end=datetime.today().strftime("%Y-%m-%d")
        )

        if df_new.empty:
            print(f"✅ {symbol}: không có dữ liệu mới.")
            return

        df_new["symbol"] = symbol
        df_new.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"✅ {symbol}: thêm {len(df_new)} dòng.")

    except Exception as e:
        print(f"⚠️ Bỏ qua {symbol}: {e}")


def run_update_all_symbols():
    """Hàm chính Airflow."""
    engine = get_postgres_engine(POSTGRES_CONN_ID)

    try:
        df_symbols = pd.read_sql(
            f"""
            SELECT DISTINCT symbol
            FROM {STOCK_PRICE_TABLE}
            ORDER BY symbol
            LIMIT {SYMBOL_LIMIT}
            """,
            engine,
        )

        symbols = df_symbols["symbol"].tolist()
        print(f"🚀 Cập nhật {len(symbols)} mã.")

        for symbol in symbols:
            update_stock_price_nearest(symbol, STOCK_PRICE_TABLE, engine)

        print("🎯 Hoàn tất cập nhật.")

    except Exception as e:
        raise RuntimeError(f"Lỗi tổng: {e}")


# ======================
# DAG
# ======================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="vnstock_el_daily_update_stock_prices",
    default_args=default_args,
    description="Daily update stock_prices from vnstock",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 23),
    catchup=False,
    tags=["EL", "daily"],
) as dag:

    update_data = PythonOperator(
        task_id="extract_and_load_data",
        python_callable=run_update_all_symbols,
    )

    update_data
