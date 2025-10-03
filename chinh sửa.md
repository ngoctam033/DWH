Đề tài: Thiết kế và Triển khai Hệ thống Data Warehouse cho Bán hàng Đa Nền tảng
Sinh viên: Nguyễn Ngọc Tâm
Giảng viên hướng dẫn: Nguyễn Văn Chiến
Trường: Đại học Giao thông Vận tải TP.HCM
Giới thiệu
Cơ sở lý thuyết
Thiết kế hệ thống
Triển khai
Kết luận
TỔNG QUAN DỰ ÁN
1. Giới thiệu dự án
Đề tài: Thiết kế và Triển khai Hệ thống Data Warehouse cho Bán hàng Đa Nền tảng
Vấn đề: Doanh nghiệp TMĐT gặp khó khăn với dữ liệu bán hàng rải rác từ nhiều sàn (Shopee, Lazada, Tiki, TikTok Shop, Website)
Mục tiêu: Xây dựng hệ thống tự động hóa thu thập, xử lý và trực quan hóa dữ liệu bán hàng
2. Bối cảnh
Dữ liệu từ nhiều kênh bán hàng không được tổ chức đồng nhất
Thiếu nền tảng chung để tổng hợp và phân tích
Quyết định kinh doanh bị chậm trễ do thiếu thông tin kịp thời
Cần giải pháp tự động thay thế xử lý thủ công
3. Giải pháp:
•	Thách thức dữ liệu TMĐT phân tán, thiếu nền tảng tổng hợp
•	Nhu cầu phân tích dữ liệu bán hàng đa kênh
•	Giải pháp: Xây dựng hệ thống Data Warehouse
KIẾN TRÚC HỆ THỐNG
3. Stack công nghệ
Python 3.10: Ngôn ngữ lập trình chính
Apache Airflow: Workflow orchestration và scheduling
MinIO: Object storage (S3-compatible)
DuckDB: In-memory columnar database để xử lý dữ liệu
FastAPI: Mock APIs cho testing
Power BI: Dashboard và visualization
Docker: Containerization và deployment
4. Kiến trúc ELT Pipeline
•	
•	
•	
•	
Giai đoạn Extract:
•	10+ DAGs riêng biệt cho từng kênh bán hàng
•	Tự động gọi API hàng ngày (00:00)
•	Lưu JSON thô vào MinIO layer "raw"
Giai đoạn Load:
•	Chuyển đổi JSON → Parquet format
•	Lưu vào MinIO layer "staging"
•	Tối ưu hóa với compression và columnar storage
Giai đoạn Transform:
•	DuckDB xử lý dữ liệu trực tiếp từ MinIO
•	Làm sạch, chuẩn hóa, tính toán trường phái sinh
•	Lưu vào MinIO layer "cleaned"
CẤU TRÚC DỮ LIỆU
5. Mô hình dữ liệu
Fact Tables:
•	fact_orders: Thông tin đơn hàng chính
•	fact_order_items: Chi tiết sản phẩm trong đơn hàng
Dimension Tables:
•	dim_customers: Thông tin khách hàng
•	dim_products: Catalog sản phẩm
•	dim_order_channels: Kênh bán hàng
•	dim_dates: Time dimension
•	dim_geo_locations: Địa lý
6. Lưu trữ phân cấp
•	
•	
•	
•	
TÍNH NĂNG CHÍNH
7. Xử lý dữ liệu tự động
•	Chuyển đổi cơ bản: Làm sạch dữ liệu, chống trùng lặp, chuẩn hóa format
•	Chuyển đổi nâng cao: Dẫn xuất business metrics, tổng hợp đa kênh, phân tích cross-channel
•	Data Quality: Validation rules, error handling, audit logging
8. Dashboard & Analytics
Sales Performance Dashboard:
•	Tổng doanh thu, lợi nhuận theo thời gian
•	So sánh performance giữa các kênh
•	Top sản phẩm bán chạy
Inventory & Logistics Dashboard:
•	Theo dõi tồn kho theo sản phẩm/danh mục
•	Thời gian giao hàng trung bình
•	Tỷ lệ giao hàng đúng hạn
KỸ THUẬT TRIỂN KHAI
9. Containerization
•	Docker Compose với nhiều services
•	Airflow với CeleryExecutor
•	PostgreSQL cho metadata
•	Redis làm message broker
•	Dễ dàng scale và deploy
10. Monitoring & Error Handling
•	Retry mechanism trong Airflow DAGs
•	Comprehensive logging và alerting
•	Dead letter queue cho failed jobs
•	Resource monitoring với Prometheus/Grafana
KỐT QUẢ ĐẠT ĐƯỢC
11. Thành tựu chính
•	Tự động hóa 90% quy trình so với xử lý thủ công
•	15+ DAGs xử lý đa kênh dữ liệu
•	3-layer storage architecture với Parquet optimization
•	Real-time dashboards với Power BI
12. Lợi ích kinh doanh
•	Tiết kiệm thời gian và nhân lực đáng kể
•	Quyết định nhanh chóng dựa trên dữ liệu thời gian thực
•	Tăng độ chính xác và nhất quán của báo cáo
•	Scalable architecture cho tương lai
TƯƠNG LAI & MỞ RỘNG
13. Roadmap phát triển
•	Tích hợp AI/ML: Dự báo doanh thu, phân khúc khách hàng, recommendation engine
•	Real-time processing: Streaming data thay vì batch
•	Advanced analytics: Anomaly detection, predictive analytics
•	Cloud migration: AWS/Azure integration
•	Self-service BI: User-friendly query interface
14. Khả năng áp dụng
•	Mở rộng cho các doanh nghiệp TMĐT khác
•	Adaptable cho nhiều ngành nghề
•	Proven architecture với open-source stack
•	Cost-effective solution cho SME
DEMO HIGHLIGHTS
•	Live Airflow DAGs execution
•	MinIO data layers walkthrough
•	Power BI interactive dashboards
•	Performance metrics và monitoring
Slide 01 — Trang bìa
•	Đề tài: Thiết kế & Triển khai Hệ thống Data Warehouse cho Bán hàng Đa Nền tảng
•	Loại báo cáo: Thực tập tốt nghiệp
•	Sinh viên: Nguyễn Ngọc Tâm – MSSV: 2151050051 – Lớp: KM21
•	GVHD: Nguyễn Văn Chiến
•	Khoa: CNTT – Trường ĐH GTVT TP.HCM
•	Thời gian: 2025
•	(Gợi ý hình: logo trường/khoa + nền tối giản)
________________________________________
Slide 02 — Mục lục
•	Lời mở đầu: Bối cảnh, mục tiêu, phạm vi, phương pháp
•	Cơ sở lý thuyết & công nghệ
•	Phân tích & thiết kế hệ thống
•	Triển khai: ELT pipeline, API mô phỏng, Docker
•	Dashboard Power BI
•	Kết luận & hướng phát triển
________________________________________
Slide 03 — Bối cảnh & Lý do chọn đề tài
•	TMĐT tạo ra dữ liệu lớn, đa kênh (Shopee, Lazada, Tiki, TikTok Shop, Website)
•	Dữ liệu rải rác, chuẩn hóa kém ⇒ chậm trễ khi ra quyết định
•	Quy trình thủ công tốn thời gian, dễ sai sót (tồn kho, vận hành, KPI)
•	Nhu cầu: nền tảng dữ liệu tập trung đảm bảo toàn vẹn, mở rộng, phân tích được
________________________________________
Slide 04 — Mục tiêu & Phạm vi
•	Mục tiêu tổng quát: Xây dựng Data Platform theo mô hình ELT tự động, mở rộng
•	Mục tiêu cụ thể: 
o	Thiết kế kiến trúc (Raw–Staging–Cleaned) & mô hình chiều (Fact/Dim)
o	Xây pipeline tự động bằng Airflow + xử lý bằng DuckDB trên Parquet/MinIO
o	Xây dashboard Power BI theo KPI trọng yếu
•	Phạm vi: Dữ liệu bán hàng đa kênh (đơn, sản phẩm, khách, vận chuyển…), dữ liệu mô phỏng qua API
________________________________________
Slide 05 — Phương pháp nghiên cứu
•	Nghiên cứu tài liệu: DW, Lake/Lakehouse, ELT/ETL, công nghệ xử lý & trực quan
•	Phân tích–Thiết kế: Yêu cầu, kiến trúc, mô hình chiều, chiến lược lưu trữ
•	Thực nghiệm–Đánh giá: Triển khai cục bộ (Docker), kiểm thử pipeline & dashboard
________________________________________
Slide 06 — Dữ liệu TMĐT: Bức tranh tổng quan
•	Thực thể chính: 
o	orders, order_items, products, customers
o	order_channel, shipment, payment, discount, brand, category, sub_category
•	Đặc thù: JSON/CSV từ API; khác biệt schema giữa các sàn
•	Yêu cầu: chuẩn hóa và hợp nhất để phân tích chéo kênh/thời gian/sản phẩm
________________________________________
Slide 07 — ETL vs ELT & Lý do chọn ELT
•	ETL: Transform trước khi Load; phù hợp hệ thống truyền thống; khó mở rộng
•	ELT: Load dữ liệu thô → Transform trong kho/lake; linh hoạt, giữ nguyên dữ liệu gốc
•	Chọn ELT vì: 
o	Khả năng tận dụng sức mạnh DuckDB trên Parquet (I/O theo cột)
o	Dễ đổi logic mà không phải gọi lại nguồn
o	Chi phí thấp, phù hợp doanh nghiệp vừa & nhỏ
________________________________________
Slide 08 — Quy trình ELT tổng quát
•	Extract: Gọi API từng kênh (Shopee, Lazada, Tiki, TikTok, Website) → JSON
•	Load: Lưu raw JSON vào MinIO (phân vùng channel/time) → chuyển Parquet (staging)
•	Transform: Chuẩn hóa/làm sạch/ghép nối bằng DuckDB → cleaned Parquet
•	BI: Power BI kết nối cleaned → mô hình dữ liệu + KPI + dashboard
•	(Gợi ý hình: sơ đồ 4 khối Extract–Load–Transform–BI)
________________________________________
Slide 09 — Công nghệ trọng tâm
•	Apache Airflow: Lập lịch, điều phối DAG, retry, log, giám sát
•	MinIO (S3-compatible): Object storage cho raw/staging/cleaned
•	Parquet: Lưu cột, nén tốt, đọc chọn cột, metadata schema
•	DuckDB: Truy vấn/biến đổi trực tiếp trên Parquet; nhanh với GB–TB nhỏ
•	Power BI: Mô hình + DAX + trực quan + chia sẻ
•	FastAPI: API mô phỏng (Faker), lọc thời gian, phân trang, mô phỏng lỗi
________________________________________
Slide 10 — Kiến trúc tổng thể
•	Thành phần: API mô phỏng ↔ Airflow ↔ MinIO ↔ DuckDB ↔ Power BI
•	Luồng dữ liệu: API → raw/JSON → staging/Parquet → cleaned/Parquet → BI
•	Vận hành: Docker Compose khởi chạy Postgres (Airflow metadata), Redis (broker), Webserver, Scheduler, Worker, MinIO, FastAPI, Flower
•	Thiết kế module hóa, dễ thay API giả bằng API thực tế
________________________________________
Slide 11 — Mô hình dữ liệu (Star Schema)
•	Fact chính: 
o	fact_orders: order_id, order_date, channel_id, status, total_price, shipping_cost, profit…
o	fact_order_items: order_item_id, order_id, product_id, quantity, unit_price, discount_amount, amount, cost_price…
•	Dim chính: 
o	dim_products, dim_customers, dim_order_channels, dim_dates, dim_geo_locations, dim_payment_methods, dim_logistics_partners
•	Lợi ích: Truy vấn KPI nhanh, dễ drill-down (Time/Product/Channel/Region)
________________________________________
Slide 12 — Thiết kế Dimension & Quan hệ
•	dim_dates: Chuẩn hóa Year/Quarter/Month/Week/Day, is_weekend, is_holiday
•	dim_products: SKU, brand, category, sub_category, price vs cost_price
•	dim_customers: code, geo_location_id, phone/email (ẩn danh hóa khi cần)
•	dim_order_channels: Shopee, Lazada, Tiki, TikTok, Website
•	Quan hệ: 1–n từ dim → fact; đảm bảo khoá thay thế (surrogate keys) khi hợp nhất
________________________________________
Slide 13 — Chiến lược lưu trữ & Phân vùng
•	Tầng dữ liệu: 
o	raw: JSON nguyên gốc (backup/traceability)
o	staging: Parquet đã ép kiểu & chuẩn tên cột
o	cleaned: Parquet sạch/chuẩn hoá/ghép nối, Single Source of Truth
•	Phân vùng MinIO: theo data_model/channel/year/month/day
•	Ví dụ đường dẫn: 
o	datawarehouse/cleaned/data_model=orders/channel=shopee/year=2025/month=09/day=20/data.parquet
•	Hiệu ứng: partition pruning + scan ít dữ liệu + truy vấn nhanh
________________________________________
Slide 14 — Môi trường & Docker Compose
•	Services chính: postgres, redis, airflow-webserver, airflow-scheduler, airflow-worker, minio, fastapi-app, flower
•	Lợi ích containerization: 
o	Tính nhất quán môi trường, dễ mở rộng worker
o	Cô lập tài nguyên, quản lý dependency tập trung
o	Khởi động toàn hệ thống bằng một lệnh
•	Bảo mật & cấu hình: biến môi trường, network nội bộ, volume dữ liệu
________________________________________
Slide 15 — Extract (Airflow DAGs)
•	DAG theo kênh & loại dữ liệu: 
o	daily_extract_shopee_order, daily_extract_lazada_order, daily_extract_tiki_order, daily_extract_tiktok_order, daily_extract_website_order
•	Nhiệm vụ chính: 
o	fetch_json_from_api (gọi endpoint, lọc theo thời gian)
o	save_raw_json_to_minio (ghi đúng cấu trúc phân vùng)
•	Logging/Retry: cấu hình retries, retry_delay, lưu log từng task
________________________________________
Slide 16 — Load & Staging (Parquet)
•	Mục tiêu: chuyển JSON → Parquet (ép kiểu, chuẩn tên cột)
•	DAG ví dụ: transform_shopee_order_to_parquet (… tương tự cho kênh khác)
•	Các task tiêu biểu: 
o	get_yesterday_file_paths (xác định input)
o	download_all_json_files (tải batch)
o	parse_json_to_table (nắn schema)
o	convert_to_parquet (ghi staging/Parquet, union_by_name)
•	Kết quả: Parquet staging đồng nhất schema, sẵn sàng cho transform
________________________________________
Slide 17 — Transform & Làm sạch (DuckDB)
•	Đọc Parquet staging trực tiếp (không copy dữ liệu)
•	Biến đổi chính: 
o	Chuẩn hóa ngày/giờ (epoch/string → timestamp)
o	Loại trùng, xử lý null, ép kiểu
o	Tính trường phái sinh: amount, profit, cost_price…
o	Join nhiều nguồn: đơn–dòng đơn–sản phẩm–khách–kênh–địa lý
•	Ghi cleaned: Parquet partitioned, sẵn sàng cho BI
________________________________________
Slide 18 — Đảm bảo chất lượng dữ liệu & Giám sát
•	Validation đa tầng: 
o	Khi Extract: kiểm cấu trúc JSON, số lượng bản ghi
o	Khi Staging: ép kiểu, chuẩn tên, kiểm phạm vi giá trị
o	Khi Cleaned: toàn vẹn tham chiếu, business rules
•	Power Query (trong Power BI): bước lọc null, cờ “Valid”, loại bản ghi lỗi
•	Giám sát: Airflow UI (trạng thái DAG/Task, lịch sử, log), Flower cho Celery
•	Xử lý lỗi: try/except trong task, retry, cảnh báo
________________________________________
Slide 19 — Dashboard Power BI & KPI
•	KPI cốt lõi: 
o	Tổng doanh thu, Lợi nhuận, Tỷ suất lợi nhuận = Profit / Revenue
o	AOV = Doanh thu / Số đơn; Doanh thu theo kênh/sản phẩm/danh mục
•	Visual gợi ý: 
o	Card (Revenue/Profit/Orders), Line (xu hướng theo thời gian)
o	Bar (so sánh theo kênh), Donut (tỷ trọng theo danh mục)
o	Slicers: thời gian, kênh, danh mục
•	Tương tác: drill-down (Year → Quarter → Month → Day), drill-through chi tiết đơn
________________________________________
Slide 20 — Kết quả đạt được & Hướng phát triển
•	Kết quả: 
o	Hoàn thiện kiến trúc ELT đa tầng; pipeline tự động bằng Airflow
o	Lưu trữ hiệu quả trên MinIO/Parquet, xử lý nhanh bằng DuckDB
o	Dashboard Power BI phản ánh KPI đa chiều, hỗ trợ quyết định
•	Lợi ích: tự động hóa, nhất quán, chi phí thấp, dễ mở rộng
•	Hướng phát triển: 
o	Thêm nguồn (CRM/ERP thực, marketing/social), real-time
o	Lakehouse (Delta/Apache Iceberg), dbt cho quản trị transform & lineage
o	Cảnh báo KPI tự động, self-service BI cho người dùng cuối


