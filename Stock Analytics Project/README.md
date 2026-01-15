# 📈 Stock Analytics Project

## 🔍 Description
Dự án tập trung vào **dự báo giá cổ phiếu** dựa trên dữ liệu lịch sử của thị trường chứng khoán Việt Nam.  
Một **pipeline Machine Learning end-to-end** được xây dựng nhằm tự động hóa toàn bộ quy trình từ thu thập dữ liệu hằng ngày, làm sạch – xử lý dữ liệu chuỗi thời gian, đến huấn luyện và so sánh các mô hình dự báo để lựa chọn mô hình có **độ chính xác cao nhất**.

Tập dữ liệu được thu thập trong giai đoạn từ **01/01/2020 đến 14/01/2026**, giúp đánh giá hiệu quả mô hình trên nhiều trạng thái và xu hướng thị trường khác nhau.

---

## 📊 Data
- **Source**: `vnstock` API  
- **Frequency**: Daily  
- **Features**: Open, High, Low, Close, Volume  

---

## ⚙️ Pipeline
- **Docker Compose**: Khởi tạo database & Airflow local  
- **Apache Airflow (DAG)**: Trích xuất dữ liệu hàng ngày từ API  
- **Data Processing**: Làm sạch và chuẩn hóa dữ liệu time series  
- **Modeling**: Huấn luyện và so sánh các mô hình dự báo  

**Flow:**
vnstock API → Airflow → Database → Cleaning → Modeling → Evaluation

---

## 🤖 Models
- ARIMA  
- LSTM  
- Ensemble  

---

## 📈 Evaluation
- **Metrics**: MSE, RMSE  
- **Result**: **LSTM đạt hiệu quả tốt nhất**, với MSE và RMSE thấp nhất so với các mô hình còn lại.

---

## 🛠️ Tech Stack
Python · Docker · Apache Airflow · vnstock · Pandas · Scikit-learn · TensorFlow / Keras  

---

## 🚀 Outcome
- Xây dựng **ML pipeline end-to-end**  
- Tự động hóa thu thập dữ liệu bằng **Airflow**  
- Ứng dụng **Deep Learning (LSTM)** cho bài toán tài chính  

---

## 📇Instruction of project
### ⚙️Setup
- Docker & Docker Compose
- Python ≥ 3.9
- Git

### 📂 Project Structure
| Thư mục   | File                             | Mô tả                                                                                          |
| --------- | -------------------------------- | ---------------------------------------------------------------------------------------------- |
| **ETL**   | `function_update_stock_price.py` | Gọi API từ thư viện **vnstock**, trích xuất dữ liệu giá cổ phiếu theo ngày và lưu vào database |
|           | `once_time_stock_dag.py`         | Định nghĩa **Airflow DAG** để điều phối pipeline ETL và trigger quá trình trích xuất dữ liệu   |
|           | `stock_price_cleaning.ipynb`     | Làm sạch dữ liệu, chuẩn hóa time series và chuẩn bị dữ liệu đầu vào cho mô hình                |
| **infra** | `.env`                           | Lưu biến môi trường (database, Airflow, cấu hình ETL)                                          |
|           | `docker-compose.yml`             | Khởi tạo **Airflow** và **database** local bằng Docker Compose                                 |
| **Model** | `ARIMA-LSTM.ipynb`               | Huấn luyện và so sánh các mô hình **ARIMA, LSTM, Ensemble** bằng MSE, RMSE                     |

### ▶️ Quy trình chạy dự án
| Bước | Thực hiện              | Mô tả                                                               |
| ---- | ---------------------- | ------------------------------------------------------------------- |
| 1    | `docker-compose up -d` | Khởi tạo Airflow và database local                                  |
| 2    | Mở Airflow UI          | Truy cập `http://localhost:8080`                                    |
| 3    | Trigger DAG            | Chạy DAG `once_time_stock_dag` để trích xuất dữ liệu từ vnstock API |
| 4    | Chạy notebook cleaning | Mở `stock_price_cleaning.ipynb` để làm sạch và xử lý dữ liệu        |
| 5    | Train & evaluate model | Chạy `ARIMA-LSTM.ipynb` để huấn luyện và đánh giá mô hình           |

### 🔄 Luồng xử lý dữ liệu
| Thứ tự | Thành phần    | Vai trò                                 |
| ------ | ------------- | --------------------------------------- |
| 1      | vnstock API   | Cung cấp dữ liệu giá cổ phiếu           |
| 2      | Airflow DAG   | Điều phối và tự động hóa quá trình ETL  |
| 3      | Database      | Lưu trữ dữ liệu thô và dữ liệu đã xử lý |
| 4      | Data Cleaning | Chuẩn hóa dữ liệu chuỗi thời gian       |
| 5      | Modeling      | Huấn luyện và đánh giá mô hình dự báo   |
