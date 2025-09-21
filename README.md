# Hệ thống Data Warehouse cho Doanh nghiệp Thương mại Điện tử

## **Giới thiệu**
Hệ thống này được thiết kế để giải quyết các vấn đề về dữ liệu phân tán và không đồng nhất của một doanh nghiệp thương mại điện tử chuyên bán các sản phẩm công nghệ như điện thoại, laptop, phụ kiện,... trên các nền tảng như Shopee, Lazada, Tiki và website riêng. Mục tiêu chính là xây dựng một hệ thống Data Warehouse để hợp nhất dữ liệu, phục vụ phân tích, báo cáo và ra quyết định kinh doanh.

---

## **Kiến trúc tổng thể**

### **1. Tổng quan kiến trúc**
- **Mô hình ELT (Extract - Load - Transform)**: Hệ thống được xây dựng theo kiến trúc ELT hiện đại.
- **Mô hình phân lớp**: Dữ liệu được tổ chức theo các lớp `Raw` → `Staging` → `Cleaned`.
- **Tự động hóa**: Toàn bộ quy trình được tự động hóa bằng Apache Airflow.

### **2. Các lớp chính**
- **Data Sources**: Thu thập dữ liệu từ API của các sàn thương mại điện tử, hệ thống ERP, CRM, và các file CSV.
- **Ingestion Layer (Extract & Load)**: Sử dụng Apache Airflow để thu thập dữ liệu định kỳ và tải vào kho lưu trữ dưới dạng thô.
- **Storage Layer**: Lưu trữ dữ liệu trên MinIO theo các tầng, sử dụng định dạng Parquet cho các tầng đã qua xử lý.
- **Processing Layer (Transform)**: Sử dụng DuckDB để thực hiện các bước làm sạch, chuẩn hóa và biến đổi dữ liệu.
- **Visualization Layer**: Sử dụng Power BI để tạo báo cáo và dashboard phục vụ phân tích kinh doanh.

---

## **Cấu trúc thư mục lưu trữ**
Dữ liệu được tổ chức trong MinIO theo cấu trúc phân vùng để tối ưu hóa truy vấn:

```
datawarehouse/
├── raw/
│   └── {channel}/
│       └── {data_type}/
│           └── year={YYYY}/
│               └── month={MM}/
│                   └── day={DD}/
│                       └── data.json
├── staging/
│   └── {channel}/
│       └── {data_type}/
│           └── year={YYYY}/
│               └── month={MM}/
│                   └── day={DD}/
│                       └── data.parquet
└── cleaned/
    └── {data_type}/
        └── year={YYYY}/
            └── month={MM}/
                └── day={DD}/
                    └── data.parquet
```

---

## **Công nghệ sử dụng**

### **1. Apache Airflow**
- Quản lý và tự động hóa các data pipeline theo mô hình ELT.
- Các DAG được định nghĩa để thu thập, làm sạch và lưu trữ dữ liệu.

### **2. DuckDB**
- Công cụ xử lý dữ liệu nhanh chóng, hỗ trợ truy vấn SQL trực tiếp trên các file Parquet.
- Được sử dụng để thực hiện bước Transform (làm sạch, chuẩn hóa dữ liệu).

### **3. MinIO**
- Đóng vai trò là kho dữ liệu (Data Warehouse), lưu trữ dữ liệu ở các tầng khác nhau.
- Hỗ trợ giao thức S3 để tích hợp dễ dàng với các công cụ khác.

### **4. PostgreSQL**
- Lưu trữ metadata cho Apache Airflow.

### **5. Redis**
- Hỗ trợ hàng đợi tác vụ (message broker) cho Celery Executor của Airflow.

### **6. Power BI**
- Tạo báo cáo và dashboard trực quan để phân tích dữ liệu từ tầng `cleaned`.

---

## **Cách triển khai**

### **1. Yêu cầu hệ thống**
- Docker và Docker Compose được cài đặt trên máy.
- Cấu hình tối thiểu:
  - CPU: 4 cores
  - RAM: 8GB
  - Dung lượng ổ cứng: 50GB

### **2. Cách chạy hệ thống**
1. Clone repository:
   ```bash
   git clone https://github.com/ngoctam033/DWH.git
   cd DWH
   ```

2. Khởi động các dịch vụ:
   ```bash
   docker-compose up -d
   ```

3. Truy cập giao diện Airflow:
   - URL: [http://localhost:8080](http://localhost:8080)
   - Username: `airflow`
   - Password: `airflow`

4. Kiểm tra các DAG trong Airflow và kích hoạt DAG cần thiết.

---

## **Cấu trúc thư mục dự án**
```
DWH/
├── airflow/                # Cấu hình và DAG của Apache Airflow
├── data_source/            # Dữ liệu nguồn và các dịch vụ liên quan
├── duck_db/                # Cấu hình và script xử lý DuckDB
├── extracted_data/         # Dữ liệu đã được trích xuất
├── init-db/                # File SQL khởi tạo cơ sở dữ liệu
├── docker-compose.yml      # File cấu hình Docker Compose
├── README.md               # Tài liệu dự án
```

---

## **Tác giả**
- **Ngọc Tâm**
- Email: nguyenngoctam0332003@gmail.com
- GitHub: [https://github.com/ngoctam033](https://github.com/ngoctam033)

---

## **Ghi chú**
- Đây là phiên bản đầu tiên của hệ thống. Các tính năng mới sẽ được cập nhật trong tương lai.
- Nếu có bất kỳ vấn đề nào, vui lòng liên hệ qua email hoặc tạo issue trên GitHub.