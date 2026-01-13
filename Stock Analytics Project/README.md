# 📈 Stock Analytics Project

## 🔍 Problem
Dự báo **giá cổ phiếu** từ dữ liệu lịch sử thị trường chứng khoán Việt Nam bằng các mô hình **time series forecasting**, với mục tiêu lựa chọn mô hình có **độ chính xác cao nhất**.

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

## 📂 Project Structure
Dataset/

ETL/

Model/