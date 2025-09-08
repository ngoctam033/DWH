# Hệ thống Lakehouse cho Doanh nghiệp Thương mại Điện tử

## **Giới thiệu**
Hệ thống này được thiết kế để giải quyết các vấn đề về dữ liệu phân tán và không đồng nhất của một doanh nghiệp thương mại điện tử chuyên bán các sản phẩm công nghệ như điện thoại, laptop, phụ kiện,... trên các nền tảng như Shopee, Lazada, Tiki và website riêng. Mục tiêu chính là xây dựng một hệ thống hợp nhất dữ liệu, phục vụ phân tích, báo cáo và ra quyết định kinh doanh.

---

## **Kiến trúc tổng thể**

### **1. Tổng quan kiến trúc**
- **Mô hình phân lớp**: Dữ liệu được tổ chức theo các lớp Raw → Bronze → Silver → Gold.
- **Tích hợp batch và near real-time**: Hệ thống hỗ trợ thu thập dữ liệu định kỳ và theo sự kiện.
- **Bảo mật và phân quyền**: Sử dụng cơ chế kiểm soát truy cập dựa trên vai trò (role-based access control) và ghi log hoạt động.

### **2. Các lớp chính**
- **Data Sources**: Thu thập dữ liệu từ API của các sàn thương mại điện tử, hệ thống ERP, CRM, và các file CSV.
- **Ingestion Layer**: Sử dụng Apache Airflow để thu thập dữ liệu định kỳ và theo sự kiện.
- **Storage Layer**: Lưu trữ dữ liệu thô và đã xử lý dưới dạng Parquet trên MinIO.
- **Processing Layer**: Sử dụng DuckDB để làm sạch, chuẩn hóa và biến đổi dữ liệu.
- **Visualization Layer**: Sử dụng Power BI để tạo báo cáo và dashboard phục vụ phân tích kinh doanh.

---

## **Cấu trúc thư mục lưu trữ**
Dữ liệu được tổ chức trong MinIO theo cấu trúc sau:

```
data/
├── raw/
│   └── orders/
│       ├── shopee/
│       │   └── 2025-08-05.csv
│       ├── lazada/
│       │   └── 2025-08-05.csv
│       ├── tiki/
│       │   └── 2025-08-05.csv
│       └── website/
│           └── 2025-08-05.csv
├── processed/
│   └── orders/
│       └── 2025-08-05/
│           └── orders_cleaned.parquet
```

---

## **Công nghệ sử dụng**

### **1. Apache Airflow**
- Quản lý và tự động hóa các pipeline xử lý dữ liệu.
- Các DAG được định nghĩa để thu thập, làm sạch và lưu trữ dữ liệu.

### **2. DuckDB**
- Công cụ xử lý dữ liệu nhanh chóng, hỗ trợ truy vấn SQL trên các file Parquet.
- Được sử dụng để làm sạch và chuẩn hóa dữ liệu.

### **3. MinIO**
- Lưu trữ dữ liệu thô và đã xử lý dưới dạng file Parquet.
- Hỗ trợ giao thức S3 để tích hợp dễ dàng với các công cụ khác.

### **4. PostgreSQL**
- Lưu trữ metadata cho Apache Airflow.

### **5. Redis**
- Hỗ trợ hàng đợi tác vụ cho Apache Airflow.

### **6. Power BI**
- Tạo báo cáo và dashboard trực quan để phân tích dữ liệu.

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
- Email: ngoctam@example.com
- GitHub: [https://github.com/ngoctam033](https://github.com/ngoctam033)

---

## **Ghi chú**
- Đây là phiên bản đầu tiên của hệ thống. Các tính năng mới sẽ được cập nhật trong tương lai.
- Nếu có bất kỳ vấn đề nào, vui lòng liên hệ qua email hoặc tạo issue trên GitHub.
