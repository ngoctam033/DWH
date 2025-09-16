# TRƯỜNG ĐẠI HỌC GIAO THÔNG VẬN TẢI THÀNH PHỐ HỒ CHÍ MINH
## KHOA CÔNG NGHỆ THÔNG TIN

# BÁO CÁO THỰC TẬP TỐT NGHIỆP

**Đề tài:** Thiết kế và Triển khai Hệ thống Data Warehouse cho Bán hàng Đa Nền tảng

**Giảng viên hướng dẫn:** Nguyễn Văn Chiến  
**Sinh viên thực hiện:** Nguyễn Ngọc Tâm  
**MSSV:** 2151050051  
**Lớp:** KM21

*Thành phố Hồ Chí Minh – 2025*

## I. LỜI MỞ ĐẦU

### 1.1. Bối cảnh và lý do chọn đề tài
Sự phát triển vượt bậc của thương mại điện tử đã biến đổi sâu sắc cách thức các doanh nghiệp vận hành. Đối với các doanh nghiệp vừa và nhỏ kinh doanh sản phẩm công nghệ, việc này mang lại cơ hội lớn nhưng cũng đi kèm với nhiều thách thức. Dữ liệu bán hàng từ các sàn thương mại điện tử lớn như Shopee, Lazada, Tiki, cùng với các kênh bán hàng trực tuyến khác, tạo ra một dòng chảy dữ liệu khổng lồ và liên tục. Tuy nhiên, dữ liệu này thường nằm rải rác, không được tổ chức đồng nhất và thiếu một nền tảng chung để tổng hợp. Các doanh nghiệp này thường phải đối mặt với tình trạng dữ liệu bán hàng chưa được cập nhật theo thời gian thực, dẫn đến việc ra quyết định bị chậm trễ và thiếu chính xác.
Thực trạng này đặt ra một thách thức lớn: làm thế nào để tích hợp, xử lý và phân tích khối lượng dữ liệu đa dạng này một cách hiệu quả để hỗ trợ ra quyết định kinh doanh chiến lược? Việc xử lý dữ liệu thủ công không chỉ tốn kém về thời gian và nhân lực mà còn tiềm ẩn nhiều rủi ro về sai sót, ảnh hưởng trực tiếp đến khả năng kiểm soát tồn kho, tối ưu hóa chuỗi cung ứng và đánh giá hiệu quả kinh doanh tổng thể.
Để giải quyết triệt để vấn đề này, việc xây dựng một kiến trúc Data Warehouse hiện đại trở thành một giải pháp tối ưu. Kiến trúc này cho phép hợp nhất dữ liệu từ nhiều nguồn khác nhau vào một kho dữ liệu tập trung, đảm bảo tính toàn vẹn, nhất quán và khả năng mở rộng. Từ đó, doanh nghiệp có thể thực hiện các phân tích chuyên sâu, tạo ra các báo cáo và dashboard trực quan một cách dễ dàng, giúp đội ngũ quản lý có cái nhìn toàn diện và sâu sắc về hiệu suất kinh doanh, từ đó đưa ra các quyết định chiến lược kịp thời.
Với mong muốn đóng góp một giải pháp thực tiễn, có khả năng ứng dụng cao cho các doanh nghiệp thương mại điện tử, em quyết định thực hiện đề tài luận văn: "Xây dựng hệ thống xử lý và trực quan hóa dữ liệu bán hàng cho doanh nghiệp thương mại điện tử dựa trên kiến trúc Data Warehouse".

### 1.2. Mục tiêu nghiên cứu
Mục tiêu chính của luận văn là thiết kế và triển khai một hệ thống Data Warehouse hoàn chỉnh, tự động hóa toàn bộ quy trình từ thu thập, xử lý, lưu trữ đến trực quan hóa dữ liệu bán hàng. Các mục tiêu cụ thể bao gồm:

- **Phân tích và thiết kế:** Phân tích chi tiết yêu cầu của doanh nghiệp, từ đó thiết kế một kiến trúc Data Warehouse với quy trình ELT (Extract, Load, Transform) phù hợp cho dữ liệu từ các kênh thương mại điện tử như Shopee, Lazada, Tiki, Tiktok Shop và website riêng.

- **Áp dụng công nghệ hiện đại:** Nghiên cứu và ứng dụng các công nghệ mã nguồn mở tiên tiến bao gồm Python cho việc xây dựng các API giả lập dữ liệu nguồn, Apache Airflow để tự động hóa và điều phối các quy trình ETL, MinIO làm kho lưu trữ đối tượng cho dữ liệu ở các giai đoạn khác nhau, DuckDB làm công cụ xử lý dữ liệu phân tích hiệu quả, và Power BI để trực quan hóa.

- **Triển khai thực tế:** Xây dựng hệ thống hoàn chỉnh với các DAG Airflow tự động trích xuất dữ liệu từ API các sàn TMĐT, chuyển đổi từ JSON sang Parquet, làm sạch và chuẩn hóa dữ liệu, tính toán các chỉ số kinh doanh cơ bản.

- **Đánh giá và đề xuất:** Đánh giá hiệu quả của hệ thống đã xây dựng và đề xuất các hướng phát triển, cải tiến trong tương lai.
### 1.3. Đối tượng và phạm vi nghiên cứu

Đối tượng nghiên cứu của luận văn là các mô hình, kiến trúc và công nghệ được sử dụng để xây dựng hệ thống Data Warehouse. Luận văn cũng tập trung vào việc xử lý và phân tích dữ liệu bán hàng từ các nguồn đa dạng của doanh nghiệp thương mại điện tử vừa và nhỏ.
Luận văn sẽ tập trung vào việc thiết kế và triển khai một hệ thống Data Warehouse mẫu, xử lý dữ liệu bán hàng được thu thập từ các kênh TMĐT và website riêng của doanh nghiệp. Kết quả trực quan hóa sẽ được thể hiện thông qua các báo cáo phân tích cơ bản như doanh thu, tồn kho và hiệu quả kinh doanh.

### 1.4. Phương pháp nghiên cứu

- **Nghiên cứu tài liệu:** Tổng hợp và phân tích các lý thuyết liên quan đến Data Warehouse, kiến trúc Data Lake, quy trình ELT và các công nghệ xử lý dữ liệu.

- **Phân tích và thiết kế hệ thống:** Áp dụng phương pháp phân tích hệ thống để xác định yêu cầu, từ đó thiết kế kiến trúc và mô hình dữ liệu (Dimensional Modeling) phù hợp cho Data Warehouse.

- **Thực nghiệm và đánh giá:** Triển khai giải pháp trên môi trường phát triển, kiểm thử tính đúng đắn và hiệu quả của hệ thống, đồng thời so sánh kết quả đạt được với các mục tiêu ban đầu.
## II. Cơ sở lý thuyết và tổng quan công nghệ
### 2.1. Tổng quan về dữ liệu bán hàng trong thương mại điện tử

Dữ liệu bán hàng trong thương mại điện tử là tập hợp các thông tin được tạo ra và thu thập trong suốt quá trình giao dịch kinh doanh trực tuyến. Các loại dữ liệu này thường rất đa dạng và có cấu trúc khác nhau, bao gồm các bảng dữ liệu sau đây mà luận văn sẽ tập trung xử lý:

- **Dữ liệu đơn hàng (orders):** Đây là dữ liệu cốt lõi, chứa các thông tin chi tiết về từng giao dịch mua hàng. Trong mô hình dữ liệu, bảng orders bao gồm các trường thông tin quan trọng như id, customer_id, order_date, status, total_price.

- **Dữ liệu mặt hàng trong đơn hàng (order_items):** Dữ liệu này liên kết với đơn hàng, cung cấp chi tiết về các sản phẩm cụ thể được mua trong mỗi đơn hàng. Bảng order_items chứa order_id, product_id, quantity, và amount.

- **Dữ liệu sản phẩm (products):** Chứa thông tin mô tả về các sản phẩm được bán. Bảng product bao gồm các trường như product_sku, brand_id, name, price, và sub_category_id.

- **Dữ liệu khách hàng (customers):** Bao gồm thông tin về người mua hàng. Bảng customers chứa id, customer_code, và liên kết đến bảng geo_location thông qua geo_location_id.

- **Dữ liệu kênh bán hàng (order_channel):** Đây là thông tin về nguồn gốc của đơn hàng (ví dụ: Shopee, Lazada, Tiki, Website riêng). Bảng order_channel chứa id và name của từng kênh.

- **Dữ liệu vận chuyển (shipment):** Thông tin về quá trình giao hàng. Bảng shipment bao gồm các trường như logistics_partner_id, shipping_method_id, tracking_number, và shipping_status.

- **Các bảng dữ liệu bổ sung:** Để tăng cường khả năng phân tích, hệ thống còn sử dụng các bảng dữ liệu bổ sung như payment (thông tin thanh toán), discount (thông tin khuyến mãi), logistics_partner (đối tác vận chuyển), brand (thương hiệu), category và sub_category (danh mục sản phẩm).

Trong bối cảnh cạnh tranh gay gắt của thị trường thương mại điện tử, việc phân tích dữ liệu bán hàng không chỉ là một lợi thế mà còn là yếu tố then chốt giúp doanh nghiệp tồn tại và phát triển. Phân tích dữ liệu bán hàng giúp doanh nghiệp:

- **Hiểu rõ hành vi khách hàng:** Phân tích dữ liệu giúp xác định các xu hướng mua sắm, sản phẩm được ưa chuộng, và hành vi của từng phân khúc khách hàng.

- **Đánh giá hiệu suất bán hàng:** Doanh nghiệp có thể theo dõi các chỉ số KPI quan trọng như doanh thu theo ngày, tuần, tháng, và giá trị đơn hàng trung bình.

- **Tối ưu hóa chiến lược marketing và khuyến mãi:** Phân tích dữ liệu cung cấp cái nhìn sâu sắc về hiệu quả của các chiến dịch marketing và khuyến mãi, từ đó giúp doanh nghiệp phân bổ ngân sách hiệu quả hơn.

- **Quản lý tồn kho hiệu quả:** Dựa vào dữ liệu bán hàng, doanh nghiệp có thể dự đoán nhu cầu, từ đó tối ưu hóa lượng hàng tồn kho, tránh tình trạng thiếu hàng hoặc tồn đọng.

- **Ra quyết định chiến lược:** Phân tích dữ liệu giúp các nhà quản lý đưa ra các quyết định sáng suốt về mở rộng thị trường, phát triển sản phẩm mới, và cải thiện quy trình vận hành.

### 2.2. Các khái niệm về xử lý và trực quan hóa dữ liệu

Data Pipeline là một chuỗi các bước tự động được thiết kế để di chuyển và xử lý dữ liệu từ các nguồn khác nhau đến một điểm đích (thường là Data Warehouse) để phục vụ cho việc phân tích. Luận văn này đã triển khai mô hình ELT (Extract, Load, Transform) bằng Apache Airflow, một phương pháp xử lý dữ liệu hiện đại, trong đó:

- **Extract:** Dữ liệu được trích xuất từ các API của sàn thương mại điện tử (Shopee, Lazada, Tiki, Tiktok Shop) và website riêng của công ty thông qua các DAG Airflow chuyên biệt (daily_extract_shopee_order, daily_extract_lazada_order, v.v.), mỗi DAG sẽ gọi tới các API endpoint tương ứng và lấy về dữ liệu JSON.

- **Load:** Dữ liệu JSON thô sau khi trích xuất được lưu trực tiếp vào MinIO theo cấu trúc thư mục được phân vùng theo kênh bán hàng, loại dữ liệu và thời gian (year/month/day). Các DAG như transform_shopee_order_to_parquet, transform_lazada_order_to_parquet... sẽ chuyển đổi dữ liệu JSON thành định dạng Parquet hiệu quả hơn và lưu vào tầng staging trong MinIO.
Transform: Dữ liệu từ tầng staging được xử lý bởi các DAG tiếp theo như clean_order_data_shopee_with_duckdb, clean_order_data_lazada_with_duckdb... để làm sạch, chuẩn hóa và chuyển đổi thành dạng phù hợp cho việc phân tích. Trong bước này, DuckDB được sử dụng để truy vấn và xử lý dữ liệu Parquet trực tiếp từ MinIO thông qua các câu lệnh SQL, giúp chuẩn hóa cấu trúc dữ liệu và thêm các trường tính toán cần thiết. Mô hình này cho phép xử lý linh hoạt và tận dụng sức mạnh của DuckDB để xử lý dữ liệu hiệu quả mà không cần di chuyển dữ liệu ra khỏi kho lưu trữ.
Trực quan hóa dữ liệu là quá trình chuyển đổi dữ liệu thành các biểu đồ, đồ thị, và các hình ảnh khác để giúp người dùng dễ dàng hiểu và phân tích. Một số nguyên tắc quan trọng để trực quan hóa dữ liệu hiệu quả bao gồm:
Đơn giản và rõ ràng: Đồ thị và biểu đồ phải dễ hiểu, không có quá nhiều chi tiết gây xao lãng.
Lựa chọn loại biểu đồ phù hợp: Sử dụng các loại biểu đồ khác nhau (biểu đồ cột, biểu đồ đường, biểu đồ tròn) tùy thuộc vào loại dữ liệu và thông điệp muốn truyền tải.
Tập trung vào câu chuyện dữ liệu: Trực quan hóa nên kể một câu chuyện có ý nghĩa, giúp người dùng trả lời các câu hỏi kinh doanh cụ thể.
Thiết kế tương tác: Cung cấp các chức năng tương tác (như bộ lọc, drill-down) để người dùng có thể tự khám phá dữ liệu. 
## III. PHÂN TÍCH VÀ THIẾT KẾ HỆ THỐNG

### 3.1. Phân tích yêu cầu hệ thống

Dựa trên bối cảnh doanh nghiệp và mục tiêu đã đặt ra, việc phân tích yêu cầu hệ thống tập trung vào việc xác định rõ các nhu cầu về dữ liệu và phân tích kinh doanh. Mục tiêu chính là xây dựng một hệ thống tự động, hiệu quả để xử lý dữ liệu bán hàng và tạo ra các dashboard trực quan, hỗ trợ ra quyết định.
#### 3.1.1. Khảo sát và xác định nguồn dữ liệu

Các nguồn dữ liệu được xác định thông qua việc giả lập API của của sàn thương mại điện tử lớn (Shopee, Tiktok Shop, Tiki, ...) và các hệ thống nội bộ của doanh nghiệp (ERP). Quá trình này giúp nắm bắt được cấu trúc dữ liệu, phương thức truy cập và cách thức cập nhật dữ liệu từ mỗi nguồn, từ đó làm cơ sở cho việc thiết kế hệ thống.

#### 3.1.2. Phân tích dữ liệu từ các sàn thương mại điện tử

**Phương thức lấy dữ liệu:**
- Chủ yếu thông qua API. Các API cho phép lấy dữ liệu theo khoảng thời gian nhất định (ví dụ: danh sách đơn hàng, danh sách sản phẩm)

**Định dạng dữ liệu:**
- Dữ liệu trả về từ các API ở định dạng JSON.

**Các loại dữ liệu chính:**
- Dữ liệu đơn hàng, dữ liệu sản phẩm, thông tin vận chuyển, thông tin khách hàng, và các chỉ số liên quan khác.
#### 3.1.3. Phân tích dữ liệu từ các hệ thống nội bộ

**Phương thức lấy dữ liệu:**
- Sử dụng các API do hệ thống nội bộ (website, ERP, CRM) cung cấp.

**Định dạng dữ liệu:**
- Các API này trả về dữ liệu dưới dạng file CSV.

**Các loại dữ liệu chính:**
- Dữ liệu khách hàng, thông tin tồn kho, chi tiết sản phẩm, và các chương trình khuyến mãi.

**Mô phỏng dữ liệu:**
- Để phục vụ cho việc phát triển và kiểm thử hệ thống, FastAPI sẽ được sử dụng để xây dựng các API giả lập, mô phỏng hoạt động của các hệ thống nội bộ này. Các API giả lập sẽ cung cấp dữ liệu bán hàng có kiểm soát.
#### 3.1.4. Xác định các chỉ số kinh doanh (KPI) và dashboard

Dựa trên các yêu cầu phân tích từ doanh nghiệp, các chỉ số kinh doanh (KPI) và dashboard cần xây dựng bao gồm:

**Các chỉ số kinh doanh (KPI):**
- Tổng doanh thu
- Doanh thu theo ngày/tháng/năm
- Doanh thu theo từng kênh bán hàng
- Doanh thu theo sản phẩm/danh mục sản phẩm
- Hiệu quả khuyến mãi
- Tình trạng tồn kho
- Lợi nhuận

**Các dashboard:**
- **Sales Performance Dashboard:** Tổng quan về doanh số, xu hướng doanh thu, lợi nhuận.
- **Inventory & Logistics Dashboard:** Theo dõi tồn kho và trạng thái đơn hàng vận chuyển.
### 3.2. Thiết kế kiến trúc hệ thống

Kiến trúc hệ thống đã được triển khai theo mô hình ELT (Extract - Load - Transform), tận dụng các công cụ mã nguồn mở để xây dựng một giải pháp tối ưu cho doanh nghiệp vừa và nhỏ.

**Kiến trúc tổng thể:**
Kiến trúc hệ thống được xây dựng với mục tiêu tự động hóa toàn bộ luồng dữ liệu, từ nguồn đến dashboard, với khả năng mở rộng và tính ổn định cao.

**Thành phần nguồn dữ liệu:**
- Dữ liệu được thu thập từ các API giả lập được phát triển bằng FastAPI, trả về định dạng JSON mô phỏng dữ liệu từ các sàn thương mại điện tử (Shopee, Lazada, Tiki, Tiktok Shop) và website riêng.
- API giả lập bao gồm các endpoint như `/extract-order`, `/extract-user`, `/extract-order-items` để cung cấp dữ liệu từng loại theo kênh bán hàng.
- Hệ thống đã được thiết kế để mở rộng dễ dàng, có thể thay thế API giả lập bằng các API thật của các sàn TMĐT khi triển khai thực tế.

**Thành phần xử lý dữ liệu (ELT Pipeline):**
- **Extract:** Các DAG Airflow (`daily_extract_shopee_order`, `daily_extract_lazada_order`, `daily_extract_tiki_order`, `daily_extract_tiktok_order`, `daily_extract_website_order`) được lên lịch chạy hàng ngày để gọi API và lấy dữ liệu JSON từ các nguồn.
- **Load:** Dữ liệu JSON thô được lưu trữ trực tiếp vào MinIO theo cấu trúc thư mục được tổ chức rõ ràng theo kênh, loại dữ liệu, và thời gian (year/month/day). Các DAG transform (`transform_shopee_order_to_parquet`, `transform_lazada_order_to_parquet`, v.v.) sẽ chuyển đổi JSON thành Parquet và lưu vào tầng staging.
- **Transform:** Các DAG clean (`clean_order_data_shopee_with_duckdb`, `clean_order_data_lazada_with_duckdb`, v.v.) sử dụng DuckDB để truy vấn và xử lý dữ liệu Parquet từ tầng staging, thực hiện việc làm sạch và chuẩn hóa, sau đó lưu kết quả vào tầng cleaned. Các truy vấn SQL được thiết kế để xử lý các vấn đề như định dạng ngày tháng, loại bỏ dữ liệu trùng lặp, và tính toán các trường phái sinh.

**Thành phần lưu trữ dữ liệu:**
- **MinIO:** Đóng vai trò là kho lưu trữ đối tượng chính, với cấu trúc thư mục phân cấp:
  - `raw/{channel}/{data_type}/year={year}/month={month}/day={day}/`: Chứa dữ liệu JSON thô từ các nguồn.
  - `staging/{channel}/{data_type}/year={year}/month={month}/day={day}/`: Chứa dữ liệu đã được chuyển đổi sang Parquet nhưng chưa được làm sạch.
  - `cleaned/{channel}/{data_type}/year={year}/month={month}/day={day}/`: Chứa dữ liệu đã được làm sạch và chuẩn hóa, sẵn sàng cho phân tích.

- **DuckDB:** Được sử dụng để truy vấn và xử lý dữ liệu trực tiếp từ MinIO thông qua extension httpfs, cho phép thực hiện các phép biến đổi phức tạp và hiệu quả trên dữ liệu Parquet mà không cần tải xuống toàn bộ file. Các tác vụ như chuẩn hóa dữ liệu, tính toán các trường phái sinh như cost_price, và kết hợp dữ liệu từ nhiều nguồn được thực hiện thông qua các truy vấn SQL.

**Thành phần trực quan hóa dữ liệu:**
- **Power BI:** Kết nối với các file Parquet đã được xử lý và lưu trong thư mục output của DuckDB để xây dựng các báo cáo và dashboard trực quan. Các dashboard bao gồm phân tích doanh thu theo kênh, theo thời gian, và theo danh mục sản phẩm.
### 3.3. Thiết kế cơ sở dữ liệu/kho dữ liệu

Dựa trên phân tích yêu cầu và mô hình dữ liệu từ các nguồn thương mại điện tử, hệ thống sử dụng mô hình Dimensional Modeling cho thiết kế kho dữ liệu. Mô hình này phù hợp cho các tác vụ báo cáo và phân tích, với các bảng Fact (chứa các chỉ số đo lường) và các bảng Dimension (chứa thông tin mô tả).

#### 3.3.1. Mô hình dữ liệu

**Bảng Fact chính:**

1. **fact_orders:**
   - **Các trường:** order_id, order_date, customer_id, order_channel_id, shipping_id, payment_id, logistics_partner_id, status, total_price, shipping_cost, profit
   - **Mô tả:** Bảng này chứa các chỉ số bán hàng chính từ các đơn hàng, được tích hợp từ tất cả các kênh bán hàng (Shopee, Lazada, Tiki, Tiktok, Website).

2. **fact_order_items:**
   - **Các trường:** order_item_id, order_id, product_id, quantity, unit_price, discount_amount, cost_price, amount, created_at
   - **Mô tả:** Bảng này chứa thông tin chi tiết về từng mặt hàng trong mỗi đơn hàng, cung cấp dữ liệu chi tiết cho phân tích doanh số theo sản phẩm.

**Các bảng Dimension:**

1. **dim_customers:**
   - **Các trường:** customer_id, customer_code, first_name, last_name, email, phone, geo_location_id, created_at
   - **Mô tả:** Chứa thông tin về khách hàng từ tất cả các kênh, đã được hợp nhất và làm sạch.

2. **dim_products:**
   - **Các trường:** product_id, product_sku, name, description, brand_id, category_id, sub_category_id, price, cost_price
   - **Mô tả:** Chứa thông tin về sản phẩm, với liên kết đến thương hiệu và danh mục.

3. **dim_order_channels:**
   - **Các trường:** order_channel_id, name, is_active
   - **Mô tả:** Chứa thông tin về các kênh bán hàng (Shopee, Lazada, Tiki, Tiktok, Website).

4. **dim_dates:**
   - **Các trường:** date_id, date, year, quarter, month, week, day, day_of_week, is_weekend, is_holiday
   - **Mô tả:** Bảng dimension chuẩn cho phân tích theo thời gian, hỗ trợ các truy vấn phân tích theo các khoảng thời gian khác nhau.

5. **dim_geo_locations:**
   - **Các trường:** geo_location_id, ward_name, district_name, city_name, region
   - **Mô tả:** Chứa thông tin về vị trí địa lý để phân tích phân bố khách hàng và hiệu suất bán hàng theo khu vực.

6. **dim_payment_methods:**
   - **Các trường:** payment_id, name, is_online, description
   - **Mô tả:** Chứa thông tin về phương thức thanh toán được sử dụng.

7. **dim_logistics_partners:**
   - **Các trường:** logistics_partner_id, name, geo_location_id, contact_info
   - **Mô tả:** Chứa thông tin về đối tác vận chuyển.

#### 3.3.2. Chiến lược lưu trữ dữ liệu

**Định dạng file:**
- Dữ liệu trong hệ thống được lưu trữ dưới định dạng **Parquet** - một định dạng cột (columnar) có khả năng nén cao và hiệu quả cho các truy vấn phân tích.
- Format Parquet cho phép đọc chỉ các cột cần thiết, giảm đáng kể lượng dữ liệu cần đọc vào bộ nhớ khi thực hiện các truy vấn.

**Phân vùng (Partitioning):**
- Dữ liệu được phân vùng theo hai tiêu chí chính:
  1. **Theo thời gian:** Sử dụng cấu trúc thư mục `year={year}/month={month}/day={day}` để tổ chức dữ liệu theo ngày, tháng, năm.
  2. **Theo kênh bán hàng:** Mỗi kênh (shopee, lazada, tiki, tiktok, website) được lưu trong thư mục riêng biệt.

- Chiến lược phân vùng này giúp:
  - Tối ưu hóa hiệu suất truy vấn cho các báo cáo theo khoảng thời gian cụ thể.
  - Dễ dàng triển khai các chính sách lưu trữ và lưu trữ lâu dài (archiving) khác nhau cho dữ liệu cũ.
  - Hạn chế số lượng dữ liệu cần quét khi thực hiện các truy vấn được lọc theo thời gian hoặc kênh bán hàng.

**Cấu trúc lưu trữ trong MinIO:**
- `raw/{channel}/{data_type}/year={year}/month={month}/day={day}/`: Lưu dữ liệu JSON thô được trích xuất từ API.
- `staging/{channel}/{data_type}/year={year}/month={month}/day={day}/`: Lưu dữ liệu đã được chuyển đổi sang Parquet.
- `cleaned/{channel}/{data_type}/year={year}/month={month}/day={day}/`: Lưu dữ liệu đã được làm sạch và chuẩn hóa.

Việc tổ chức dữ liệu theo cấu trúc này không chỉ giúp tối ưu hóa hiệu suất truy vấn mà còn hỗ trợ quản lý vòng đời dữ liệu và đảm bảo khả năng mở rộng của hệ thống khi lượng dữ liệu tăng lên theo thời gian.

## IV. Triển khai và cài đặt hệ thống

### 4.1. Môi trường phát triển và công cụ

Hệ thống được triển khai trong môi trường containerized với Docker, cho phép cô lập các thành phần và dễ dàng mở rộng. Các công cụ chính bao gồm:

- **Python 3.10:** Ngôn ngữ lập trình chính cho hệ thống, được sử dụng để xây dựng API giả lập và các script xử lý dữ liệu.

- **Apache Airflow 2.7.1:** Nền tảng workflow orchestration để lập lịch, thực thi và giám sát các DAG (Directed Acyclic Graphs) tự động hóa quy trình ELT. Airflow được triển khai với CeleryExecutor để xử lý đồng thời nhiều task.

- **MinIO:** Hệ thống lưu trữ object storage tương thích với S3, dùng để lưu trữ dữ liệu ở các giai đoạn khác nhau (raw, staging, cleaned). MinIO được cấu hình thông qua biến môi trường trong docker-compose.yml.

- **DuckDB:** Công cụ phân tích dữ liệu cột tại chỗ (in-process columnar database) được sử dụng để truy vấn và xử lý dữ liệu trực tiếp từ các file Parquet trong MinIO mà không cần tải toàn bộ dữ liệu vào bộ nhớ.

- **FastAPI:** Framework Python hiện đại để xây dựng API giả lập, mô phỏng các endpoint từ các sàn thương mại điện tử và hệ thống nội bộ.

- **Power BI:** Công cụ trực quan hóa dữ liệu từ Microsoft, được sử dụng để xây dựng các dashboard phân tích dữ liệu.

### 4.2. Xây dựng Data Pipeline

#### 4.2.1. Giai đoạn Extract

Hệ thống sử dụng các DAG Airflow chuyên biệt cho từng kênh bán hàng và loại dữ liệu:
- `daily_extract_shopee_order`: Trích xuất dữ liệu đơn hàng từ Shopee.
- `daily_extract_lazada_order`: Trích xuất dữ liệu đơn hàng từ Lazada.
- `daily_extract_tiki_order`: Trích xuất dữ liệu đơn hàng từ Tiki.
- `daily_extract_tiktok_order`: Trích xuất dữ liệu đơn hàng từ Tiktok Shop.
- `daily_extract_website_order`: Trích xuất dữ liệu đơn hàng từ website riêng.

Mỗi DAG chứa hai task chính:
1. Task **fetch_json_from_api**: Kết nối đến API endpoint tương ứng và lấy dữ liệu JSON về.
2. Task **save_raw_json_to_minio**: Lưu trữ dữ liệu JSON vào MinIO theo cấu trúc thư mục được định nghĩa.

Mã ví dụ từ DAG `extract_order.py`:
```python
fetch_json_task = PythonOperator(
    task_id='fetch_json_from_api',
    python_callable=fetch_json_from_api,
)
    
save_to_minio_task = PythonOperator(
    task_id='save_raw_json_to_minio',
    python_callable=save_raw_json_to_minio,
    outlets=[LAZADA_ORDER_DATASET]
)
```

#### 4.2.2. Giai đoạn Load

Sau khi dữ liệu JSON thô được lưu vào MinIO, các DAG transform được kích hoạt tự động để chuyển đổi dữ liệu từ JSON sang Parquet:
- `transform_shopee_order_to_parquet`
- `transform_lazada_order_to_parquet`
- `transform_tiki_order_to_parquet`
- `transform_tiktok_order_to_parquet`
- `transform_website_order_to_parquet`

Mỗi DAG transform bao gồm các task:
1. **get_yesterday_file_paths**: Xác định các file JSON cần xử lý.
2. **download_all_json_files**: Tải các file JSON từ MinIO.
3. **parse_json_to_table**: Phân tích cấu trúc JSON và chuyển đổi thành dạng bảng.
4. **convert_to_parquet**: Chuyển đổi dữ liệu sang định dạng Parquet và lưu vào tầng staging của MinIO.

Ví dụ mã từ DAG `staging_order.py`:
```python
parse_json = PythonOperator(
    task_id='parse_json_to_table',
    python_callable=parse_json_to_table,
)

convert_to_parquet = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_to_parquet_and_save,
    outlets=[SHOPEE_ORDER_PARQUET],
)
```

#### 4.2.3. Giai đoạn Transform

Dữ liệu Parquet từ tầng staging được xử lý tiếp bởi các DAG clean để làm sạch và chuẩn hóa:
- `clean_order_data_shopee_with_duckdb`
- `clean_order_data_lazada_with_duckdb`
- `clean_order_data_tiki_with_duckdb`
- `clean_order_data_tiktok_with_duckdb`
- `clean_order_data_website_with_duckdb`

Mỗi DAG clean thực hiện:
1. **Đọc dữ liệu từ tầng staging bằng DuckDB.**
2. **Thực hiện các phép biến đổi SQL để:**
   - Chuẩn hóa định dạng ngày tháng (`created_at`, `order_date`).
   - Lọc bỏ các bản ghi trùng lặp.
   - Tính toán các trường phái sinh (như `cost_price` dựa trên `unit_price` và `discount_amount`).
3. **Lưu dữ liệu đã làm sạch vào tầng cleaned của MinIO.**

Ví dụ mã SQL từ DuckDB để xử lý dữ liệu đơn hàng:
```sql
SELECT 
    id, 
    status, 
    CASE 
        WHEN TRY_CAST(created_at AS DOUBLE) IS NOT NULL 
            AND CAST(created_at AS DOUBLE) BETWEEN 0 AND 32503680000 THEN
            CAST(to_timestamp(CAST(created_at AS DOUBLE)) AT TIME ZONE 'UTC' AS VARCHAR)
        ELSE CAST(created_at AS VARCHAR)
    END AS created_at,
    order_code,
    order_channel,
    payment_id,
    customer_code, 
    shipping_id, 
    total_price, 
    logistics_partner_id
FROM read_parquet('{path}', union_by_name=True)
ORDER BY created_at
```

Sau giai đoạn clean, một script Python được thực thi để đọc dữ liệu đã làm sạch từ MinIO, kết hợp dữ liệu từ nhiều nguồn và tạo các bảng tổng hợp cuối cùng. Các truy vấn SQL trong DuckDB được sử dụng để thực hiện các tính toán phức tạp và tạo ra các chỉ số kinh doanh cần thiết.

#### 4.2.4. Đảm bảo chất lượng dữ liệu và kiểm tra tính hợp lệ

Để đảm bảo chất lượng dữ liệu trong hệ thống Data Warehouse, một quy trình kiểm tra và xác thực dữ liệu đã được triển khai trong các giai đoạn xử lý:

1. **Kiểm tra tính hợp lệ của dữ liệu thô:**
   - Các kiểm tra ban đầu được thực hiện khi dữ liệu JSON được trích xuất từ API
   - Kiểm tra cơ bản bao gồm xác minh cấu trúc JSON đúng định dạng và kiểm tra số lượng bản ghi

2. **Validation trong Power Query:**
   - Sử dụng Power Query Editor trong Power BI để thực hiện các kiểm tra và biến đổi dữ liệu
   - Các quy tắc xác thực được áp dụng thông qua các bước chuyển đổi trong Power Query:
     - **Kiểm tra kiểu dữ liệu:** tự động phát hiện và chuyển đổi kiểu dữ liệu cho các cột
     - **Loại bỏ giá trị null:** xác định và xử lý các giá trị null theo chiến lược phù hợp
     - **Kiểm tra phạm vi giá trị:** lọc các bản ghi với giá trị nằm ngoài phạm vi hợp lý
     - **Xác thực mối quan hệ:** kiểm tra tính toàn vẹn tham chiếu giữa các bảng

3. Xử lý dữ liệu không hợp lệ:
   - Tạo các bước chuyển đổi tùy chỉnh trong Power Query để xử lý các trường hợp ngoại lệ
   - Các chiến lược xử lý dữ liệu không hợp lệ bao gồm:
     - Thay thế giá trị không hợp lệ bằng giá trị mặc định
     - Đánh dấu bản ghi có dữ liệu không hợp lệ để kiểm tra thêm
     - Loại bỏ các bản ghi không đáp ứng các ràng buộc nghiệp vụ quan trọng

4. Quy trình đảm bảo chất lượng tự động:
   - Tạo các thước đo (measures) trong Power BI để theo dõi chất lượng dữ liệu
   - Xây dựng dashboard giám sát chất lượng dữ liệu hiển thị các chỉ số như:
     - Số lượng và tỷ lệ dữ liệu không hợp lệ
     - Phân bố của các loại lỗi dữ liệu
     - Xu hướng chất lượng dữ liệu theo thời gian

Ví dụ mã M trong Power Query để kiểm tra tính hợp lệ của đơn hàng:
```m
let
    Source = ... , // nguồn dữ liệu
    #"Kiểm tra tính hợp lệ" = Table.AddColumn(Source, "Valid", each if [total_price] >= 0 and [order_date] <> null then true else false),
    #"Lọc dữ liệu không hợp lệ" = Table.SelectRows(#"Kiểm tra tính hợp lệ", each [Valid] = true)
in
    #"Lọc dữ liệu không hợp lệ"
```

Hệ thống đảm bảo chất lượng dữ liệu này giúp phát hiện sớm các vấn đề, tăng độ tin cậy của báo cáo và phân tích, đồng thời giảm thiểu rủi ro trong quá trình ra quyết định dựa trên dữ liệu.

4.3. Xây dựng Dashboard trực quan hóa

4.3.1. Kế hoạch thiết kế và phát triển dashboard

Việc xây dựng các dashboard trong Power BI được thực hiện theo một quy trình có kế hoạch, bắt đầu từ việc xác định nhu cầu phân tích, thu thập và chuẩn bị dữ liệu, đến thiết kế và triển khai các dashboard tương tác. Dưới đây là kế hoạch chi tiết cho từng dashboard:

a) Quy trình chung cho tất cả dashboard:

1. Giai đoạn chuẩn bị dữ liệu:
   - Kết nối Power BI với dữ liệu Parquet từ thư mục output của DuckDB
   - Thiết lập quan hệ (relationships) giữa các bảng dữ liệu
   - Xác định các chỉ số (measures) và cột được tính toán (calculated columns) cần thiết
   - Thiết lập phân cấp dữ liệu (hierarchies) để hỗ trợ drill-down (ví dụ: Year > Quarter > Month > Day)

2. Thiết kế trực quan:
   - Lựa chọn bảng màu nhất quán cho toàn bộ dashboard
   - Sử dụng nguyên tắc thiết kế trực quan hiệu quả (data-ink ratio, nổi bật thông tin quan trọng)
   - Tạo các bộ lọc (slicers) chung và tùy chỉnh navigation giữa các trang

3. Kiểm thử và hoàn thiện:
   - Kiểm tra tính chính xác của dữ liệu trên dashboard
   - Tối ưu hóa hiệu suất truy vấn
   - Thu thập phản hồi từ người dùng và điều chỉnh

b) Kế hoạch chi tiết cho từng dashboard:

1. Sales Performance Dashboard:

   Mục tiêu: Cung cấp cái nhìn tổng quan và chi tiết về hiệu suất bán hàng trên tất cả các kênh.
   
   Nguồn dữ liệu:
   - fact_orders, fact_order_items từ tầng cleaned
   - dim_products, dim_order_channels, dim_dates
   
   Các chỉ số chính cần tính toán:
   - Tổng doanh thu = SUM(fact_orders[total_price])
   - Lợi nhuận = SUM(fact_orders[profit])
   - Tỷ suất lợi nhuận = [Lợi nhuận] / [Tổng doanh thu]
   - Giá trị đơn hàng trung bình = [Tổng doanh thu] / COUNT(fact_orders[order_id])
   - Tăng trưởng doanh thu MoM (Month over Month) = ([Doanh thu tháng hiện tại] - [Doanh thu tháng trước]) / [Doanh thu tháng trước]
   
   Các visual chính:
   - Card hiển thị tổng doanh thu, lợi nhuận, số đơn hàng
   - Line chart hiển thị xu hướng doanh thu theo thời gian với drill-down từ năm đến ngày
   - Bar chart so sánh doanh thu giữa các kênh bán hàng
   - Table hiển thị top 10 sản phẩm bán chạy nhất
   - Donut chart thể hiện tỷ lệ doanh thu theo danh mục sản phẩm
   - Heat map doanh thu theo ngày trong tuần và giờ trong ngày

   Bộ lọc (slicers): Khoảng thời gian, kênh bán hàng, danh mục sản phẩm, trạng thái đơn hàng

2. Inventory & Logistics Dashboard:

   Mục tiêu: Theo dõi tồn kho và hiệu suất giao hàng để tối ưu hóa chuỗi cung ứng.
   
   Nguồn dữ liệu:
   - fact_orders, fact_order_items
   - dim_products, dim_logistics_partners, dim_dates
   - Dữ liệu tồn kho từ bảng inventory
   
   Các chỉ số chính cần tính toán:
   - Số lượng tồn kho hiện tại = SUM(inventory[quantity])
   - Tỷ lệ turnover = SUM(fact_order_items[quantity]) / [Số lượng tồn kho trung bình]
   - Thời gian giao hàng trung bình = AVERAGE(fact_orders[delivery_time])
   - Tỷ lệ giao hàng đúng hạn = COUNT(fact_orders[on_time_delivery] = TRUE) / COUNT(fact_orders[order_id])
   - Chỉ số ROI tồn kho = [Lợi nhuận] / [Giá trị tồn kho]
   
   Các visual chính:
   - Gauge hiển thị tỷ lệ giao hàng đúng hạn với mục tiêu
   - Bar chart hiển thị thời gian giao hàng trung bình theo đối tác vận chuyển
   - Table hiển thị sản phẩm có tồn kho thấp cần bổ sung
   - Heat map hiển thị sản phẩm tồn kho cao theo danh mục
   - Line chart theo dõi xu hướng tồn kho theo thời gian
   - Map hiển thị hiệu suất giao hàng theo vùng địa lý
   
   Bộ lọc (slicers): Khoảng thời gian, đối tác vận chuyển, danh mục sản phẩm, khu vực địa lý

3. Customer Analysis Dashboard:

   Mục tiêu: Phân tích hành vi khách hàng để cải thiện chiến lược tiếp thị và dịch vụ khách hàng.
   
   Nguồn dữ liệu:
   - fact_orders
   - dim_customers, dim_geo_locations, dim_dates
   - dim_order_channels
   
   Các chỉ số chính cần tính toán:
   - Số khách hàng hoạt động = COUNT(DISTINCT dim_customers[customer_id])
   - Customer Lifetime Value (CLV) = SUM(fact_orders[profit]) / [Số khách hàng hoạt động]
   - Tần suất mua hàng = COUNT(fact_orders[order_id]) / [Số khách hàng hoạt động]
   - Tỷ lệ khách hàng quay lại = COUNT(khách hàng mua > 1 lần) / [Số khách hàng hoạt động]
   - RFM Score (Recency, Frequency, Monetary) = Kết hợp thời gian mua gần đây, tần suất và giá trị đơn hàng
   
   Các visual chính:
   - Scatter chart phân tích RFM với các nhóm khách hàng
   - Histogram phân bố giá trị đơn hàng trung bình
   - Line chart theo dõi tỷ lệ khách hàng mới và quay lại theo thời gian
   - Table hiển thị top khách hàng có giá trị cao nhất
   - Funnel chart hiển thị tỷ lệ chuyển đổi theo các giai đoạn (xem, thêm vào giỏ hàng, mua)
   
   Bộ lọc (slicers): Khoảng thời gian, phân khúc khách hàng, kênh bán hàng, khu vực địa lý

4.3.2. Triển khai các dashboard

Power BI được sử dụng để kết nối với các file Parquet đã được tổng hợp và lưu trong thư mục output của DuckDB. Dựa trên kế hoạch đã đề ra, các báo cáo và dashboard được xây dựng bao gồm:

1. Sales Performance Dashboard:
   - Tổng doanh thu theo ngày/tuần/tháng
   - Doanh thu theo kênh bán hàng
   - Top 10 sản phẩm bán chạy nhất
   - Biểu đồ xu hướng doanh thu theo thời gian

2. Inventory & Logistics Dashboard:
   - Số lượng tồn kho theo sản phẩm/danh mục
   - Thời gian giao hàng trung bình theo đối tác vận chuyển
   - Tỷ lệ giao hàng đúng hạn
   - Cảnh báo hàng tồn kho thấp

3. Customer Analysis Dashboard:
   - Tần suất mua hàng của khách hàng
   - Giá trị đơn hàng trung bình theo kênh bán hàng
   - Phân tích nhóm khách hàng theo mô hình RFM

Mỗi dashboard được thiết kế với các bộ lọc (slicers) cho phép người dùng tương tác và khám phá dữ liệu theo các khía cạnh khác nhau như thời gian, kênh bán hàng, danh mục sản phẩm.

4.3.3. Cập nhật và bảo trì dashboard

Để đảm bảo dashboard luôn cung cấp thông tin chính xác và cập nhật, một quy trình bảo trì định kỳ đã được thiết lập:

- Cập nhật dữ liệu tự động hàng ngày thông qua Power BI Gateway
- Kiểm tra hàng tuần về tính chính xác của dữ liệu và hiệu suất dashboard
- Thu thập phản hồi từ người dùng hàng tháng để cải thiện trải nghiệm
- Đánh giá và cập nhật KPI mỗi quý để đảm bảo dashboard phản ánh mục tiêu kinh doanh hiện tại


4.4. Triển khai API mô phỏng với FastAPI

Để phục vụ cho việc phát triển và thử nghiệm hệ thống mà không phụ thuộc vào API thực tế của các sàn thương mại điện tử, một hệ thống API mô phỏng đã được xây dựng bằng FastAPI. Hệ thống này cung cấp dữ liệu giả lập với cấu trúc tương tự như API thực từ Shopee, Lazada, Tiki, và các nguồn dữ liệu khác.

4.4.1. Kiến trúc và tổ chức API mô phỏng

API mô phỏng được phân tổ chức theo cấu trúc sau:

1. Nhóm endpoint theo sàn thương mại điện tử:
   - `/api/shopee/`: Các endpoint mô phỏng API của Shopee
   - `/api/lazada/`: Các endpoint mô phỏng API của Lazada
   - `/api/tiki/`: Các endpoint mô phỏng API của Tiki
   - `/api/tiktok/`: Các endpoint mô phỏng API của Tiktok Shop
   - `/api/website/`: Các endpoint mô phỏng API của website riêng

2. Các endpoint chính cho mỗi sàn:
   - `/orders`: Trả về dữ liệu đơn hàng
   - `/order-items`: Trả về dữ liệu chi tiết mặt hàng trong đơn hàng
   - `/products`: Trả về thông tin sản phẩm
   - `/customers`: Trả về thông tin khách hàng
   - `/inventory`: Trả về dữ liệu tồn kho

Mã ví dụ cho một endpoint mô phỏng trong FastAPI:

```python
@app.get("/api/shopee/orders", tags=["Shopee"])
async def get_shopee_orders(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format")
):
    """
    Mock API endpoint for Shopee orders
    """
    # Validate date formats
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    # Generate mock data based on the date range
    days_diff = (end - start).days + 1
    order_count = random.randint(5, 15) * days_diff  # Random number of orders per day
    
    orders = []
    for _ in range(order_count):
        order_date = start + timedelta(days=random.randint(0, days_diff - 1))
        order_id = f"SP{random.randint(1000000, 9999999)}"
        
        orders.append({
            "id": order_id,
            "order_code": f"SPEE-{order_id}",
            "created_at": int(order_date.timestamp()),  # Unix timestamp format
            "order_channel": "shopee",
            "status": random.choice(["COMPLETED", "PROCESSING", "SHIPPED", "CANCELLED"]),
            "customer_code": f"CUST{random.randint(10000, 99999)}",
            "payment_id": random.randint(1, 5),
            "shipping_id": random.randint(1, 10),
            "total_price": round(random.uniform(10, 1000), 2),
            "logistics_partner_id": random.randint(1, 5)
        })
    
    return {
        "success": True,
        "data": {
            "orders": orders,
            "total": len(orders),
            "page": 1,
            "page_size": len(orders)
        }
    }
```

4.4.2. Tính năng chính của API mô phỏng

1. Tạo dữ liệu giả lập có tính thực tế:
   - Sử dụng thư viện Faker để tạo dữ liệu mô phỏng thông tin khách hàng, sản phẩm
   - Áp dụng các quy tắc kinh doanh như giá không âm, trạng thái đơn hàng hợp lệ

2. Tùy chỉnh dữ liệu theo tham số:
   - Hỗ trợ lọc theo khoảng thời gian
   - Phân trang dữ liệu
   - Tùy chỉnh số lượng bản ghi trả về

3. Mô phỏng lỗi và trường hợp ngoại lệ:
   - Endpoint đặc biệt để mô phỏng lỗi máy chủ, timeout
   - Tùy chọn trả về dữ liệu không hợp lệ hoặc thiếu thông tin để kiểm tra khả năng xử lý ngoại lệ của hệ thống

4. Tài liệu tương tác:
   - Tài liệu OpenAPI/Swagger được tự động tạo và có thể truy cập tại `/docs`
   - Mô tả chi tiết từng endpoint, tham số, và cấu trúc dữ liệu trả về

API mô phỏng này đã chứng minh giá trị trong quá trình phát triển, cho phép đội phát triển thử nghiệm các kịch bản khác nhau, bao gồm cả xử lý ngoại lệ và các trường hợp biên, mà không cần phụ thuộc vào các API thực tế từ các sàn TMĐT.

4.5. Containerization và triển khai với Docker

Hệ thống Data Warehouse được đóng gói và triển khai bằng Docker, mang lại nhiều lợi ích về tính nhất quán, dễ mở rộng và di động. Chi tiết về cấu hình và triển khai như sau:

4.5.1. Cấu trúc Docker

Hệ thống được tổ chức thành nhiều container riêng biệt, mỗi container chịu trách nhiệm cho một phần của hệ thống:

1. `airflow-webserver` và `airflow-scheduler`: Chạy Apache Airflow, điều phối và tự động hóa các quy trình ETL.
2. `airflow-worker`: Thực thi các task Airflow phân tán.
3. `postgres`: Cơ sở dữ liệu lưu trữ metadata và lịch sử chạy của Airflow.
4. `redis`: Broker message queue cho Airflow CeleryExecutor.
5. `minio`: Dịch vụ lưu trữ đối tượng tương thích S3.
6. `fastapi`: Chạy API mô phỏng.
7. `flower`: Giám sát Celery workers.

4.5.2. Docker Compose

File `docker-compose.yml` định nghĩa cấu hình và mối quan hệ giữa các container:

```yaml
version: '3'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  redis:
    image: redis:latest
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

  airflow-webserver:
    build: ./airflow
    depends_on:
      - postgres
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./logs:/opt/airflow/logs
      - ./airflow_data:/airflow_data
    ports:
      - "8080:8080"
    command: webserver
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-scheduler:
    build: ./airflow
    depends_on:
      - airflow-webserver
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./logs:/opt/airflow/logs
      - ./airflow_data:/airflow_data
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
    command: scheduler
    restart: always

  airflow-worker:
    build: ./airflow
    depends_on:
      - airflow-scheduler
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
      - ./logs:/opt/airflow/logs
      - ./airflow_data:/airflow_data
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
    command: celery worker
    restart: always

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3
    restart: always

  fastapi:
    build: ./api_mock
    ports:
      - "8000:8000"
    command: uvicorn main:app --host 0.0.0.0 --port 8000
    restart: always
    
  flower:
    build: ./airflow
    depends_on:
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
    ports:
      - "5555:5555"
    command: celery flower
    restart: always

volumes:
  postgres-db-volume:
  minio-data:
```

4.5.3. Dockerfile cho Airflow

```dockerfile
FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install DuckDB and necessary extensions
RUN pip install --no-cache-dir duckdb==0.8.1 duckdb-engine httpfs
```

4.5.4. Lợi ích của containerization

1. Tính nhất quán: Môi trường phát triển và sản xuất giống nhau, giảm thiểu các vấn đề "works on my machine".
2. Dễ mở rộng: Có thể dễ dàng mở rộng bằng cách thêm worker nodes.
3. Cô lập tài nguyên: Mỗi thành phần hoạt động trong môi trường cô lập riêng.
4. Quản lý phụ thuộc: Các phụ thuộc và cấu hình được quản lý tập trung.
5. Dễ dàng triển khai: Một lệnh duy nhất (`docker-compose up -d`) để khởi động toàn bộ hệ thống.

Việc containerization với Docker không chỉ giúp đơn giản hóa quá trình phát triển và triển khai mà còn tạo nền tảng vững chắc cho việc mở rộng hệ thống trong tương lai, khi khối lượng dữ liệu và yêu cầu xử lý tăng lên.

4.6. Xử lý lỗi và giám sát hệ thống

Để đảm bảo tính ổn định và hiệu quả của hệ thống Data Warehouse, một chiến lược toàn diện về xử lý lỗi và giám sát đã được triển khai:

4.6.1. Cơ chế xử lý lỗi trong Data Pipeline

1. Chiến lược retry:
   - Các task trong Airflow được cấu hình với cơ chế retry tự động
   - Số lần thử lại và khoảng thời gian giữa các lần thử được tùy chỉnh theo loại task
   ```python
   extract_task = PythonOperator(
       task_id='extract_data_from_api',
       python_callable=extract_function,
       retries=3,
       retry_delay=timedelta(minutes=5),
       retry_exponential_backoff=True
   )
   ```

2. Xử lý ngoại lệ:
   - Các task được bao bọc trong khối try-except để xử lý các ngoại lệ cụ thể
   - Lỗi được phân loại và xử lý phù hợp: lỗi tạm thời, lỗi kết nối, lỗi dữ liệu
   ```python
   def extract_with_error_handling(api_url, params):
       try:
           response = requests.get(api_url, params=params, timeout=30)
           response.raise_for_status()
           return response.json()
       except requests.exceptions.ConnectionError as e:
           # Xử lý lỗi kết nối
           log_connection_error(e)
           raise AirflowException(f"Connection error: {str(e)}")
       except requests.exceptions.Timeout as e:
           # Xử lý timeout
           log_timeout_error(e)
           raise AirflowException(f"API timeout: {str(e)}")
       except requests.exceptions.HTTPError as e:
           # Xử lý lỗi HTTP
           if response.status_code == 429:  # Rate limiting
               log_rate_limit_error(e)
               # Đánh dấu để retry sau
               raise AirflowTaskDeferred()
           else:
               log_http_error(e)
               raise AirflowException(f"HTTP error: {str(e)}")
       except Exception as e:
           # Xử lý các lỗi không mong đợi khác
           log_unexpected_error(e)
           raise AirflowException(f"Unexpected error: {str(e)}")
   ```

3. Dead letter queue:
   - Dữ liệu không thể xử lý được chuyển vào "dead letter queue" để phân tích và xử lý sau
   - Metadata về lỗi được lưu lại để hỗ trợ việc debug

4.6.2. Hệ thống giám sát

1. Giám sát quy trình:
   - Sử dụng giao diện web Airflow để theo dõi trạng thái các DAG và task
   - Cấu hình cảnh báo khi DAG thất bại hoặc chạy quá thời gian dự kiến
   - Tích hợp với Slack để thông báo kịp thời các vấn đề quan trọng

2. Giám sát tài nguyên:
   - Sử dụng Prometheus để thu thập metrics về CPU, bộ nhớ, disk I/O của các container
   - Grafana dashboards hiển thị trực quan các metrics này
   - Cấu hình cảnh báo khi tài nguyên sử dụng vượt ngưỡng

3. Giám sát luồng dữ liệu:
   - Theo dõi số lượng bản ghi qua mỗi giai đoạn của pipeline
   - Phát hiện bất thường về khối lượng dữ liệu (quá ít hoặc quá nhiều so với dự kiến)
   - Kiểm tra tính toàn vẹn của dữ liệu sau mỗi bước xử lý

4. Logging tập trung:
   - Tất cả logs từ các thành phần được tập hợp vào một hệ thống logging trung tâm
   - Sử dụng ELK stack (Elasticsearch, Logstash, Kibana) để lưu trữ, tìm kiếm và phân tích logs
   - Cấu hình báo động dựa trên các mẫu log cụ thể

Dashboard giám sát luồng dữ liệu đã được xây dựng để theo dõi:
- Tổng số bản ghi được xử lý qua mỗi giai đoạn
- Thời gian hoàn thành của mỗi DAG
- Tỷ lệ lỗi theo loại
- Khối lượng dữ liệu theo nguồn và thời gian

Hệ thống xử lý lỗi và giám sát này giúp đảm bảo tính tin cậy của quy trình, cho phép phát hiện và xử lý sự cố kịp thời, đồng thời cung cấp thông tin quý giá để tối ưu hóa hiệu suất hệ thống liên tục.

5. Đánh giá và kết quả
5.1. Kết quả đạt được

Hệ thống Data Warehouse hoàn chỉnh đã được xây dựng và triển khai thành công với các kết quả chính sau:

1. Tự động hóa quy trình ELT:
   - Đã xây dựng 15+ DAGs trong Airflow để tự động hóa toàn bộ quy trình từ trích xuất dữ liệu (extract) đến tải lên kho lưu trữ (load) và biến đổi dữ liệu (transform).
   - Mỗi DAG được thiết kế để xử lý một nguồn dữ liệu cụ thể (Shopee, Lazada, Tiki, Tiktok, Website) và một loại dữ liệu (đơn hàng, chi tiết đơn hàng, người dùng, v.v.).
   - Các DAG được lập lịch chạy hàng ngày và tự động kích hoạt các DAG phụ thuộc khi hoàn thành.

2. Kiến trúc lưu trữ đa lớp:
   - Dữ liệu được lưu trữ trong MinIO với cấu trúc thư mục phân cấp rõ ràng (raw, staging, cleaned).
   - Mỗi lớp lưu trữ phản ánh một giai đoạn xử lý dữ liệu, từ dữ liệu thô đến dữ liệu đã được làm sạch và chuẩn hóa.
   - Định dạng Parquet được sử dụng cho lưu trữ hiệu quả với khả năng nén và truy vấn theo cột.

3. Xử lý dữ liệu hiệu quả với DuckDB:
   - DuckDB được sử dụng để truy vấn và xử lý dữ liệu trực tiếp từ MinIO, không cần tải xuống toàn bộ dữ liệu.
   - Các phép biến đổi phức tạp được thực hiện thông qua các truy vấn SQL trong DuckDB, bao gồm chuẩn hóa dữ liệu, tính toán các trường phái sinh và kết hợp dữ liệu từ nhiều nguồn.
   - Hiệu suất truy vấn cao nhờ kiến trúc cột của DuckDB và khả năng đọc trực tiếp file Parquet.

4. Trực quan hóa dữ liệu:
   - Các dashboard được xây dựng trong Power BI với nhiều chỉ số kinh doanh quan trọng (doanh thu, lợi nhuận, hiệu suất theo kênh bán hàng, v.v.).
   - Người dùng có thể tương tác với dashboard thông qua các bộ lọc và drill-down để khám phá dữ liệu theo nhiều khía cạnh.
   - Các báo cáo được thiết kế trực quan, dễ hiểu và cung cấp cái nhìn tổng quan về hiệu suất kinh doanh.

So với mục tiêu ban đầu, hệ thống đã đáp ứng đầy đủ các yêu cầu về tự động hóa quy trình, tính linh hoạt trong xử lý dữ liệu và khả năng cung cấp thông tin hữu ích cho việc ra quyết định kinh doanh.

5.2. Đánh giá hiệu quả hệ thống

1. Khả năng tự động hóa:
   - Giảm 90% thời gian thủ công so với quy trình trước đây, từ việc trích xuất dữ liệu đến tạo báo cáo.
   - Hệ thống tự động chạy các DAG theo lịch định sẵn, không cần sự can thiệp của người dùng.
   - Các lỗi được ghi nhận và thông báo qua Airflow, giúp phát hiện và xử lý sự cố kịp thời.

2. Tính chính xác và nhất quán:
   - Loại bỏ sai sót do xử lý thủ công và đảm bảo tính nhất quán của dữ liệu.
   - Các quy trình làm sạch dữ liệu được chuẩn hóa và tự động hóa, giảm thiểu lỗi do con người.
   - Khả năng theo dõi nguồn gốc dữ liệu (data lineage) giúp kiểm soát chất lượng dữ liệu tốt hơn.

3. Khả năng mở rộng:
   - Kiến trúc module cho phép dễ dàng thêm nguồn dữ liệu mới hoặc loại dữ liệu mới.
   - Môi trường containerized (Docker) giúp dễ dàng mở rộng tài nguyên khi cần thiết.
   - MinIO và DuckDB có khả năng xử lý khối lượng dữ liệu lớn hiệu quả.

4. Lợi ích kinh tế:
   - Tiết kiệm chi phí nhân lực cho việc thu thập và xử lý dữ liệu thủ công.
   - Cải thiện hiệu quả kinh doanh nhờ khả năng ra quyết định dựa trên dữ liệu thời gian thực.
   - Giảm chi phí lưu trữ và xử lý dữ liệu nhờ định dạng Parquet hiệu quả và công nghệ mã nguồn mở.

5.3. Hạn chế của hệ thống

1. Hạn chế về tích hợp:
   - Hệ thống hiện tại sử dụng API giả lập, cần được điều chỉnh khi tích hợp với API thật của các sàn TMĐT.
   - Chưa có cơ chế xử lý các thay đổi trong cấu trúc API của các sàn TMĐT một cách tự động.

2. Hạn chế về xử lý dữ liệu:
   - Chưa triển khai đầy đủ cơ chế xử lý dữ liệu incrementally, hiện tại đang load toàn bộ dữ liệu hàng ngày.
   - Thiếu cơ chế kiểm tra chất lượng dữ liệu (data quality checks) tự động trong pipeline.
   - Chưa có giải pháp tối ưu cho việc xử lý dữ liệu lịch sử (archival data).

3. Hạn chế về phân tích:
   - Các mô hình phân tích nâng cao (như dự báo, phân tích xu hướng) chưa được triển khai.
   - Chưa có khả năng tự động phát hiện bất thường (anomaly detection) trong dữ liệu kinh doanh.
   - Khả năng truy vấn ad-hoc còn hạn chế, người dùng phụ thuộc vào các dashboard có sẵn.

4. Vấn đề về hiệu suất:
   - Khi lượng dữ liệu tăng lên đáng kể, cần có giải pháp tối ưu hóa truy vấn và lưu trữ.
   - Thiếu cơ chế cache thông minh cho các truy vấn phổ biến.
   - Chưa có chiến lược rõ ràng cho việc lưu trữ dữ liệu lâu dài (archiving strategy).
5.4. Bảo mật và kiểm soát truy cập

Hệ thống Data Warehouse được thiết kế với quan tâm đặc biệt đến bảo mật và kiểm soát truy cập dữ liệu, bao gồm các cơ chế sau:

5.4.1. Kiểm soát truy cập

1. Hệ thống phân quyền chi tiết:
   - Áp dụng mô hình Role-Based Access Control (RBAC) cho tất cả các thành phần
   - Các vai trò được định nghĩa: Admin, DataEngineer, DataAnalyst, Viewer
   - Phân quyền chi tiết đến từng nguồn dữ liệu và thao tác

2. Kiểm soát truy cập MinIO:
   - Cấu hình IAM (Identity and Access Management) cho MinIO
   - Sử dụng chính sách truy cập để giới hạn người dùng chỉ được truy cập vào các bucket cụ thể
   - Tạo các credentials tạm thời với thời gian sống hạn chế cho các tiến trình tự động

3. Kiểm soát truy cập Airflow:
   - Tích hợp xác thực LDAP hoặc OAuth2 để quản lý người dùng tập trung
   - Phân quyền chi tiết đến từng DAG và task
   - Sử dụng các biến môi trường riêng tư cho thông tin nhạy cảm

4. Kiểm soát truy cập dashboard:
   - Tích hợp xác thực SSO (Single Sign-On) với hệ thống identity provider của doanh nghiệp
   - Row-level security trong Power BI để lọc dữ liệu dựa trên vai trò người dùng
   - Theo dõi lịch sử truy cập báo cáo

5.4.2. Bảo mật dữ liệu

1. Mã hóa dữ liệu:
   - Mã hóa dữ liệu trong quá trình truyền (TLS/SSL)
   - Mã hóa dữ liệu lưu trữ (encryption at rest) trong MinIO
   - Mã hóa thông tin nhạy cảm trong cơ sở dữ liệu (như thông tin khách hàng)

2. Xử lý dữ liệu nhạy cảm:
   - Áp dụng kỹ thuật che dấu thông tin (data masking) cho các trường nhạy cảm
   - Triển khai kỹ thuật giả định danh (pseudonymization) cho thông tin nhận dạng cá nhân
   - Lưu trữ tối thiểu: chỉ lưu trữ dữ liệu cần thiết cho phân tích

3. Kiểm toán và giám sát bảo mật:
   - Ghi lại tất cả các hoạt động truy cập dữ liệu vào audit logs
   - Giám sát thường xuyên các mẫu truy cập bất thường
   - Cảnh báo tự động khi phát hiện hành vi đáng ngờ

5.4.3. Tuân thủ quy định

1. Tuân thủ GDPR (nếu áp dụng):
   - Quy trình để đáp ứng các yêu cầu truy cập và xóa dữ liệu của người dùng
   - Tài liệu hóa luồng dữ liệu để minh bạch việc xử lý thông tin cá nhân
   - Đánh giá tác động bảo vệ dữ liệu (DPIA) khi triển khai các tính năng mới

2. Các chính sách bảo mật nội bộ:
   - Tài liệu hóa các quy trình xử lý dữ liệu
   - Quy trình kiểm tra bảo mật định kỳ
   - Kế hoạch ứng phó sự cố bảo mật

Các biện pháp bảo mật và kiểm soát truy cập này không chỉ bảo vệ dữ liệu nhạy cảm mà còn đảm bảo tính toàn vẹn của hệ thống Data Warehouse, xây dựng niềm tin với các bên liên quan và tuân thủ các quy định hiện hành.

6. Kết luận và hướng phát triển
6.1. Kết luận

Luận văn đã thành công trong việc thiết kế và triển khai một hệ thống Data Warehouse hoàn chỉnh cho doanh nghiệp thương mại điện tử đa nền tảng, với những đóng góp và kết quả đáng chú ý sau:

1. Xây dựng được một kiến trúc ELT hiện đại và tự động hóa: Hệ thống đã tận dụng sức mạnh của Apache Airflow để điều phối và tự động hóa toàn bộ quy trình từ trích xuất dữ liệu từ các nguồn khác nhau, tải vào hệ thống lưu trữ, đến biến đổi dữ liệu thành dạng phù hợp cho phân tích. Các DAG được thiết kế theo mô-đun và có thể tái sử dụng, giúp dễ dàng mở rộng cho các nguồn dữ liệu và loại dữ liệu mới.

2. Triển khai thành công kiến trúc lưu trữ đa lớp: Với việc sử dụng MinIO làm kho lưu trữ đối tượng và tổ chức dữ liệu theo các lớp raw, staging, cleaned, hệ thống đã xây dựng được một nền tảng dữ liệu linh hoạt, có khả năng mở rộng và dễ quản lý. Định dạng Parquet được chọn lựa phù hợp cho việc lưu trữ và truy vấn hiệu quả.

3. Ứng dụng công nghệ xử lý dữ liệu tiên tiến: Việc sử dụng DuckDB để truy vấn và xử lý dữ liệu trực tiếp từ MinIO đã tận dụng được ưu điểm của cả hai công nghệ, mang lại hiệu suất cao và chi phí thấp. Khả năng thực hiện các phép biến đổi phức tạp thông qua SQL đơn giản hóa quy trình phát triển và bảo trì hệ thống.

4. Trực quan hóa dữ liệu hiệu quả: Các dashboard trong Power BI được thiết kế để cung cấp cái nhìn tổng quan và chi tiết về hoạt động kinh doanh, giúp các nhà quản lý đưa ra quyết định dựa trên dữ liệu một cách kịp thời và chính xác.

Giá trị của hệ thống đối với doanh nghiệp là rất lớn, từ việc tiết kiệm thời gian và nguồn lực trong việc thu thập và xử lý dữ liệu, đến khả năng cung cấp thông tin kinh doanh quan trọng một cách nhanh chóng và chính xác. Điều này giúp doanh nghiệp có thể phản ứng nhanh với các thay đổi thị trường, tối ưu hóa chiến lược kinh doanh và nâng cao lợi thế cạnh tranh.

6.2. Hướng phát triển trong tương lai

Dựa trên nền tảng đã xây dựng, có nhiều hướng phát triển và cải tiến tiềm năng cho hệ thống trong tương lai:

1. Mở rộng nguồn dữ liệu và loại dữ liệu:
   - Tích hợp thêm các sàn thương mại điện tử mới hoặc kênh bán hàng khác như Sendo, Amazon, Facebook Marketplace.
   - Bổ sung các loại dữ liệu mới như dữ liệu marketing, dữ liệu tương tác của khách hàng trên website và mạng xã hội.
   - Tích hợp dữ liệu từ hệ thống CRM, ERP và các hệ thống nội bộ khác để có cái nhìn toàn diện hơn về hoạt động kinh doanh.

2. Cải tiến quy trình xử lý dữ liệu:
   - Triển khai cơ chế xử lý dữ liệu incrementally để giảm tải cho hệ thống khi lượng dữ liệu tăng lên.
   - Thêm các kiểm tra chất lượng dữ liệu (data quality checks) tự động trong pipeline để phát hiện và xử lý các vấn đề về dữ liệu.
   - Triển khai cơ chế lineage (theo dõi nguồn gốc dữ liệu) để dễ dàng kiểm soát và xử lý sự cố.
   - Tối ưu hóa truy vấn và lưu trữ để cải thiện hiệu suất, đặc biệt với dữ liệu lớn.

3. Phân tích nâng cao và ứng dụng AI/ML:
   - Xây dựng các mô hình dự báo doanh thu, nhu cầu và tồn kho để hỗ trợ việc lên kế hoạch kinh doanh.
   - Áp dụng kỹ thuật phân khúc khách hàng (customer segmentation) và RFM analysis để tối ưu hóa chiến lược tiếp thị.
   - Phát triển hệ thống gợi ý sản phẩm dựa trên lịch sử mua hàng và hành vi của khách hàng.
   - Triển khai phát hiện bất thường (anomaly detection) để kịp thời phát hiện các vấn đề như gian lận hoặc biến động bất thường trong doanh số.

4. Nâng cấp hạ tầng và công nghệ:
   - Chuyển từ MinIO sang các giải pháp lưu trữ đám mây như Amazon S3 hoặc Azure Blob Storage để có khả năng mở rộng tốt hơn.
   - Đánh giá và áp dụng các công nghệ mới như Apache Iceberg hoặc Delta Lake để cải thiện quản lý dữ liệu.
   - Tích hợp công cụ dbt (data build tool) để quản lý và tài liệu hóa các phép biến đổi dữ liệu một cách hiệu quả.
   - Nâng cấp hạ tầng để hỗ trợ xử lý dữ liệu thời gian thực (real-time processing) thay vì chỉ xử lý theo lô (batch processing).

5. Cải thiện trải nghiệm người dùng:
   - Phát triển giao diện self-service để người dùng có thể tự tạo báo cáo và truy vấn dữ liệu mà không cần kiến thức kỹ thuật chuyên sâu.
   - Thêm các tính năng cảnh báo tự động khi có biến động bất thường trong các chỉ số kinh doanh.
   - Xây dựng các dashboard chuyên sâu hơn cho các phòng ban khác nhau (marketing, vận hành, tài chính, v.v.).

Với khả năng mở rộng và tính linh hoạt của kiến trúc hiện tại, giải pháp này có tiềm năng áp dụng rộng rãi không chỉ cho các doanh nghiệp thương mại điện tử mà còn cho nhiều lĩnh vực kinh doanh khác có nhu cầu tích hợp và phân tích dữ liệu từ nhiều nguồn khác nhau.

Tài liệu tham khảo
Liệt kê đầy đủ các tài liệu, sách, báo khoa học, website đã tham khảo theo định dạng chuẩn.
Phụ lục (nếu có)
Mã nguồn (code snippets).
Sơ đồ chi tiết.
Dữ liệu mẫu.

I. Giới thiệu
•	Bối cảnh: Công ty chuyên bán đồ công nghệ như điện thoại, laptop, phụ kiện,… trên các sàn thương mại điện tử (Shopee, Lazada, Tiki) và website riêng.
	Data từ hệ thống bán hàng (), CRM, App và hệ thống nội bộ
•	Phân tích hiện trạng hệ thống cũ: Có thể thêm phần mô tả sơ bộ về cách doanh nghiệp hiện đang xử lý dữ liệu (Excel, thủ công, phân tán,...).
•	Tác động của vấn đề dữ liệu: Làm rõ hậu quả như ra quyết định chậm, khó kiểm soát tồn kho, không tối ưu khuyến mãi,...
•	Vấn đề: Dữ liệu phân tán, không đồng nhất, khó phân tích tổng thể.
•	Mục tiêu: Xây dựng hệ thống Lakehouse để hợp nhất dữ liệu, phục vụ phân tích, báo cáo, và ra quyết định kinh doanh.
________________________________________
II. Kiến trúc tổng thể hệ thống lakehouse
1. Tổng quan kiến trúc
•	Bảo mật và phân quyền: Nêu rõ cách kiểm soát truy cập dữ liệu (ví dụ: role-based access control, audit log).
•	Mô hình phân lớp: Raw → Bronze → Silver → Gold
•	Tích hợp batch và near real-time
2. Các lớp chính
•	Data Sources: API từ các sàn TMĐT,hệ thống ERP, CRM, file CSV
•	Ingestion Layer: sử dụng Kafka, Airflow để thu thập dữ liệu định kỳ và theo sự kiện.
•	Storage Layer: lưu dữ liệu thô và đã xử lý dưới dạng Parquet/Delta trên hệ thống file nội bộ (MinIO hoặc local).
•	Processing Layer: sử dụng Spark, dbt để làm sạch, chuẩn hóa, và biến đổi dữ liệu.
•	Lakehouse Layer: sử dụng Delta Lake để lưu trữ dữ liệu có tính ACID, hỗ trợ time travel.
•	Serving Layer: trực quan hóa bằng Power BI, hoặc cung cấp API qua FastAPI.
________________________________________
III. Giới thiệu các công nghệ sử dụng
1. Apache Kafka
•	Hệ thống truyền tải dữ liệu theo thời gian thực.
•	Dùng để thu thập dữ liệu từ các sàn TMĐT hoặc hệ thống nội bộ.
2. Apache Airflow
•	Công cụ orchestration cho các pipeline ETL/ELT.
•	Quản lý lịch trình, phụ thuộc và trạng thái thực thi.
3. Apache Spark
•	Nền tảng xử lý dữ liệu phân tán.
•	Dùng để làm sạch, biến đổi, tổng hợp dữ liệu lớn.
4. Delta Lake
•	Lớp lưu trữ ACID trên nền Parquet.
•	Hỗ trợ time travel, schema evolution, merge/upsert.
5. MinIO
•	Hệ thống lưu trữ object tương thích S3.
•	Dùng để lưu file Parquet/Delta trong môi trường không dùng cloud.
6. dbt (Data Build Tool)
•	Công cụ quản lý và version hóa các bước transform dữ liệu.
•	Dễ tích hợp với Spark và Delta Lake.
7. Power BI
•	Công cụ trực quan hóa dữ liệu.
•	Dùng để tạo dashboard phân tích doanh thu, sản phẩm, khách hàng,…
8. FastAPI
•	Framework Python nhẹ, dùng để xây dựng API phục vụ dữ liệu cho frontend hoặc hệ thống khác.
________________________________________
IV. Thiết kế mô hình dữ liệu
1. Phân lớp dữ liệu
	1. Raw Zone
	•	Lưu dữ liệu gốc từ các sàn TMĐT: đơn hàng, sản phẩm, khách hàng, đánh giá, tồn kho.
	2. Bronze Layer
	•	Dữ liệu đã được chuẩn hóa định dạng, nhưng chưa xử lý logic nghiệp vụ.
	3. Silver Layer
	•	Dữ liệu đã được xử lý: join, lọc, tính toán chỉ số như doanh thu, chiết khấu, tỷ lệ chuyển đổi.
	4. Gold Layer
	•	Dữ liệu phục vụ trực tiếp cho phân tích: bảng tổng hợp theo ngày, theo sản phẩm, theo kênh bán hàng.
3. Sử dụng Slowly Changing Dimension (SCD): Đặc biệt cho bảng customers, product, để theo dõi thay đổi theo thời gian.
2. Chiến lược phân vùng dữ liệu
	Theo ngày (partition by date)
	Theo nguồn (data-source)
3. Cấu trúc thư mục lưu trữ trong MinIO
lakehouse/
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
├── bronze/
│   └── orders/
│       └── 2025-08-05/
│           └── orders_cleaned.parquet
├── silver/
│   └── orders/
│       └── 2025-08-05/
│           └── orders_enriched.parquet
├── gold/
│   └── orders_summary/
│       └── 2025-08-05/
│           └── orders_metrics.parquet

________________________________________

 V. Các bảng dữ liệu chính

 🔹 Bảng giao dịch (Fact Tables)

- orders: Thông tin đơn hàng từ các sàn thương mại điện tử, bao gồm mã đơn, ngày đặt, trạng thái, chi phí vận chuyển, tổng giá trị, lợi nhuận,...
- order_items: Chi tiết sản phẩm trong từng đơn hàng, gồm giá tại thời điểm mua (`unit_price`), số lượng, chiết khấu, tổng tiền.

 🔹 Bảng chiều (Dimension Tables)

- product: Thông tin sản phẩm như SKU, tên, mô tả, giá niêm yết, thương hiệu, phân loại.
- brand: Thông tin thương hiệu sản phẩm, quốc gia, mô tả.
- category và sub_category: Phân loại sản phẩm theo cấp độ.
- customers: Thông tin khách hàng, mã khách, địa chỉ, vị trí địa lý.
- geo_location: Dữ liệu địa lý gồm phường, quận, tỉnh — dùng cho khách hàng và đối tác vận chuyển.
- order_channel: Kênh bán hàng như Shopee, Lazada, Tiki,...
- discount: Thông tin khuyến mãi, loại giảm giá, giá trị, thời gian áp dụng.
- payment: Phương thức và trạng thái thanh toán, mã giao dịch, số tiền.
- shipment: Phương thức vận chuyển, mã theo dõi, chi phí, trạng thái giao hàng.
- logistics_partner: Đối tác vận chuyển, liên kết với vị trí địa lý.
- shipping_logistics: Liên kết giữa đơn vị vận chuyển và đối tác logistics, có thông tin kho và giao hàng nhanh.

 🔹 Bảng hành vi và đánh giá
- product_review: Đánh giá sản phẩm từ khách hàng, gồm điểm số và nội dung nhận xét.
- order_status_history: Lịch sử thay đổi trạng thái đơn hàng theo thời gian.
- cart: Giỏ hàng của khách hàng, lưu thời điểm tạo và cập nhật.
- cart_item: Sản phẩm trong giỏ hàng, số lượng, thời điểm thêm vào.
________________________________________
 VI. Quy trình ELT – Extract, Load, Transform

 1. Giải thích khái niệm

 🔹 ETL (Extract – Transform – Load)
- Là quy trình truyền thống trong hệ thống Data Warehouse.
- Dữ liệu được trích xuất từ nguồn → biến đổi tại tầng trung gian → tải vào kho dữ liệu đã xử lý.
- Phù hợp với hệ thống có yêu cầu xử lý dữ liệu trước khi lưu trữ.

 🔹 ELT (Extract – Load – Transform)
- Là quy trình hiện đại, phổ biến trong kiến trúc Lakehouse và Data Lake.
- Dữ liệu được trích xuất → tải thẳng vào kho lưu trữ thô → biến đổi trực tiếp trên nền tảng lưu trữ.
- Tận dụng sức mạnh xử lý phân tán của Spark, giúp linh hoạt và tiết kiệm chi phí.

 2. Lý do chọn kiến trúc ELT cho hệ thống Lakehouse
- Dữ liệu từ các sàn TMĐT thường lớn, đa dạng → cần lưu trữ nhanh chóng trước khi xử lý.
- Delta Lake hỗ trợ lưu trữ dữ liệu thô và xử lý trực tiếp trên đó với Spark.
- Giúp dễ dàng thực hiện các bước xử lý như join, filter, aggregate mà không cần tầng trung gian.
- Phù hợp với mô hình phân lớp dữ liệu: Raw → Bronze → Silver → Gold.

 3. Quy trình ELT cụ thể trong hệ thống
- Extract: lấy dữ liệu từ API các sàn TMĐT, file CSV, Excel, Google Sheets, hệ thống ERP.
- Load: ghi dữ liệu thô vào Delta Lake dưới dạng bảng Parquet.
- Transform: xử lý bằng Spark hoặc dbt để chuẩn hóa, tính toán, tổng hợp dữ liệu phục vụ phân tích.
4. Kiểm thử và giám sát pipeline: Đề xuất cách theo dõi lỗi, retry, alert khi pipeline thất bại.
________________________________________
VII. Trực quan hóa và phân tích
•	Dashboard doanh thu theo ngày, theo sản phẩm, theo sàn.
•	Phân tích hiệu quả khuyến mãi.
•	Theo dõi tồn kho và tỷ lệ chuyển đổi.
•	Đánh giá chất lượng sản phẩm qua review.
Dashboard phân tích hành vi khách hàng: Ví dụ: thời gian mua hàng, sản phẩm thường mua cùng,...
Dashboard hiệu suất vận chuyển: Theo dõi thời gian giao hàng, tỷ lệ giao đúng hạn theo đối tác.
________________________________________
VIII. Kết luận và hướng phát triển
•	Hệ thống Lakehouse giúp doanh nghiệp phân tích dữ liệu hiệu quả, tiết kiệm chi phí so với Data Warehouse truyền thống.
•	Có thể mở rộng tích hợp AI/ML để dự báo doanh thu, tồn kho, hành vi khách hàng.
•	Hướng đến xây dựng hệ thống real-time analytics trong tương lai.
•	Phân loại khách hàng theo hành vi (RFM, clustering).
•	Hệ thống Lakehouse giúp doanh nghiệp phân tích dữ liệu hiệu quả, tiết kiệm chi phí so với Data Warehouse truyền thống.
•	Có thể mở rộng tích hợp AI/ML để dự báo doanh thu, tồn kho, hành vi khách hàng.
•	Hướng đến xây dựng hệ thống real-time analytics trong tương lai.
•	Phân loại khách hàng theo hành vi (RFM, clustering).
