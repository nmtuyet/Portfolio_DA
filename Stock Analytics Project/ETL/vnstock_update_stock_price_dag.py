from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.hooks.base import BaseHook # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock, Listing
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError



# ======================
# AIRFLOW CONFIG
# ======================
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_stockdb")
STOCK_PRICE_TABLE = Variable.get("STOCK_PRICE_TABLE", default_var="stock_prices")
VNSTOCK_SOURCE = Variable.get("VNSTOCK_SOURCE", default_var="VCI")
DEFAULT_START_DATE = Variable.get("VNSTOCK_DEFAULT_START_DATE", default_var="2024-01-01")


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
def get_all_symbols_today():
    """
    Lấy danh sách mã cổ phiếu đang giao dịch (VCI).
    """
    try:
        lst = Listing(source=VNSTOCK_SOURCE.lower())
        df_listed = lst.all_symbols(to_df=True)
        symbols = df_listed["symbol"].dropna().unique().tolist()
        print(f"📈 Phát hiện {len(symbols)} mã cổ phiếu.")
        return symbols
    except Exception as e:
        raise RuntimeError(f"Lỗi quét danh sách mã: {e}")

def update_stock_price_nearest(symbol, table_name, engine):
    try:
        # 1️⃣ Try đọc dữ liệu cũ
        try:
            query = text(f"SELECT time FROM {table_name} WHERE symbol = :symbol")
            df_old = pd.read_sql(query, engine, params={"symbol": symbol})
        except ProgrammingError:
            df_old = pd.DataFrame()

        # 2️⃣ Xác định start_date
        if not df_old.empty:
            df_old["time"] = pd.to_datetime(df_old["time"])
            start_date = (df_old["time"].max() + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = DEFAULT_START_DATE

        # 3️⃣ Fetch dữ liệu mới
        stock = Vnstock().stock(symbol=symbol, source=VNSTOCK_SOURCE)
        df_new = stock.quote.history(
            start=start_date,
            end=datetime.today().strftime("%Y-%m-%d")
        )

        if df_new.empty:
            print(f"✅ {symbol}: không có dữ liệu mới.")
            return

        # 4️⃣ Insert → tự tạo bảng nếu chưa có
        df_new["symbol"] = symbol
        df_new.to_sql(table_name, engine, if_exists="append", index=False)

        print(f"✅ {symbol}: thêm {len(df_new)} dòng.")

    except Exception as e:
        print(f"❌ {symbol}: lỗi {e}")
        raise

def run_update_all_symbols():
    """
    - Quét danh sách mã mới
    - So sánh DB
    - Cập nhật toàn bộ
    """
    engine = get_postgres_engine(POSTGRES_CONN_ID)

    # 1️⃣ Quét thị trường
    all_symbols = get_all_symbols_today()

    # 2️⃣ Mã đã có trong DB
    try:
        df_existing = pd.read_sql(
            f"SELECT DISTINCT symbol FROM {STOCK_PRICE_TABLE}", engine
        )
        existing_symbols = df_existing["symbol"].tolist()
    except Exception:
        existing_symbols = []
        print("⚠️ Bảng chưa tồn tại hoặc trống.")

    # 3️⃣ Tìm mã mới
    new_symbols = [s for s in all_symbols if s not in existing_symbols]
    print(f"🆕 {len(new_symbols)} mã mới.")

    # 4️⃣ Cập nhật tất cả
    all_to_update = sorted(set(existing_symbols + new_symbols))
    print(f"🚀 Cập nhật {len(all_to_update)} mã.")

    for symbol in all_to_update:
        update_stock_price_nearest(symbol, STOCK_PRICE_TABLE, engine)

    print("🎯 Hoàn tất cập nhật.")


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
    dag_id="vnstock_el_daily_update_stock_price",
    default_args=default_args,
    description="Daily EL stock price (auto detect new symbols)",
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
