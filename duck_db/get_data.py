import duckdb
import logging
import os
import argparse
import re
from datetime import datetime
from config import S3_CONFIG, DEFAULT_BUCKET_NAME
# Hàm thuần túy để tạo chuỗi cấu hình cho DuckDB
def format_config_value(value):
    """Định dạng giá trị config cho DuckDB"""
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)

# Hàm tạo connection với side effect rõ ràng
def create_s3_connection(s3_config):
    """Tạo kết nối DuckDB với cấu hình S3"""
    conn = duckdb.connect(database=':memory:')
    conn.install_extension('httpfs')
    conn.load_extension('httpfs')
    
    # Áp dụng các cấu hình
    for key, value in s3_config.items():
        config_value = format_config_value(value)
        conn.execute(f"SET {key}='{config_value}';")
    
    return conn

# Hàm thuần túy để tạo đường dẫn S3
def build_s3_path(bucket_name, path, specific_date=None):
    """Tạo đường dẫn S3 đầy đủ, có thể lọc theo ngày cụ thể"""
    base_path = f"s3://{bucket_name}/{path}"
    
    # Nếu không có ngày cụ thể, trả về đường dẫn đầy đủ với wildcard
    if not specific_date:
        return base_path
    
    # Nếu có ngày cụ thể, thay thế các wildcard bằng giá trị cụ thể
    year = specific_date.year
    # Đảm bảo tháng và ngày có 2 chữ số (zero-padded)
    month = f"{specific_date.month:02d}"
    day = f"{specific_date.day:02d}"
    
    # Thay thế wildcard với giá trị cụ thể
    path_with_date = base_path.replace("year=*/month=*/day=*", f"year={year}/month={month}/day={day}")
    path_with_date = path_with_date.replace("year=*", f"year={year}")
    path_with_date = path_with_date.replace("month=*", f"month={month}")
    path_with_date = path_with_date.replace("day=*", f"day={day}")

    return path_with_date

# Hàm thuần túy để tạo truy vấn SQL
def build_parquet_query(path):
    """Tạo câu truy vấn SQL để đọc parquet"""
    if "/data_model=orders/" in path:
        return f"""
            SELECT 
                id, 
                status, 
                CASE 
                    WHEN TRY_CAST(created_at AS DOUBLE) IS NOT NULL 
                        AND CAST(created_at AS DOUBLE) BETWEEN 0 AND 32503680000 THEN  -- Phạm vi hợp lệ
                        CAST(to_timestamp(CAST(created_at AS DOUBLE)) AT TIME ZONE 'UTC' AS VARCHAR)
                    ELSE CAST(created_at AS VARCHAR)  -- Ép kiểu giá trị gốc thành VARCHAR
                END AS created_at,  -- Chuyển đổi timestamp hợp lệ
                CASE 
                    WHEN TRY_CAST(order_date AS DOUBLE) IS NOT NULL 
                        AND CAST(order_date AS DOUBLE) BETWEEN 0 AND 32503680000 THEN  -- Phạm vi hợp lệ
                        CAST(to_timestamp(CAST(order_date AS DOUBLE)) AT TIME ZONE 'UTC' AS VARCHAR)
                    ELSE CAST(order_date AS VARCHAR)  -- Ép kiểu giá trị gốc thành VARCHAR
                END AS order_date,  -- Chuyển đổi timestamp hợp lệ
                order_code,
                order_channel,
                payment_id,
                customer_code, 
                shipping_id, 
                total_price, 
                --shipping_cost, 
                logistics_partner_id
            FROM read_parquet('{path}', union_by_name=True)
            ORDER BY created_at
        """
    if "/data_model=order_channel/" in path:
        return f"""
                    SELECT DISTINCT id, name, is_active
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/data_model=order_items/" in path:
        return f"""
                    SELECT 
                            *,
                            CASE 
                                WHEN TRY_CAST(unit_price AS DOUBLE) IS NULL OR TRY_CAST(discount_amount AS DOUBLE) IS NULL THEN NULL
                                ELSE GREATEST(
                                    ROUND((CAST(COALESCE(TRY_CAST(unit_price AS DOUBLE), 0) AS DOUBLE) - 
                                        CAST(COALESCE(TRY_CAST(discount_amount AS DOUBLE), 0) AS DOUBLE)) * 
                                        (1 - (0.2 + random() * 0.1))),
                                    0
                                )
                            END AS cost_price
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/data_model=users/" in path:
        return f"""
                    SELECT * 
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/data_model=product/" in path:
        return f"""
                    SELECT * 
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/data_model=geo_location/" in path:
        return f"""
                    SELECT * 
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/data_model=payment/" in path:
        return f"""
                    SELECT * 
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/data_model=sub_category/" in path:
        return f"""
                    SELECT * 
                    FROM read_parquet('{path}', union_by_name=True)
                """
    raise ValueError("Đường dẫn không hợp lệ hoặc không được hỗ trợ")

# Hàm với side effect rõ ràng để đọc dữ liệu
def read_parquet_from_s3(conn, query):
    """Đọc dữ liệu parquet từ S3 thông qua DuckDB"""
    # logging.info(f"Chạy query: {query}")
    return conn.execute(query).fetchdf()

# Hàm với side effect rõ ràng để lưu dữ liệu
def save_to_parquet(df, output_path, specific_date=None):
    """Lưu DataFrame vào file parquet theo cấu trúc thư mục mới:
    output/
     ├─ users/
     │   ├─ 2025_09_19.parquet
     │   ├─ 2025_09_20.parquet
     ├─ orders/
     │   ├─ 2025_09_19.parquet
     │   ├─ 2025_09_20.parquet
    
    Args:
        df: DataFrame cần lưu
        output_path: Đường dẫn gốc để lưu file parquet
        specific_date: Đối tượng datetime để đặt tên file, nếu None thì dùng ngày hiện tại
    """
    # Nếu không có specific_date, sử dụng ngày hiện tại
    if specific_date is None:
        specific_date = datetime.now()
        logging.info(f"Không có specific_date, sử dụng ngày hiện tại: {specific_date.strftime('%Y-%m-%d')}")
    
    # Tạo tên file với định dạng YYYY_MM_DD.parquet
    date_str = specific_date.strftime('%Y_%m_%d')
    
    # Lấy thư mục gốc từ output_path
    base_dir = os.path.dirname(os.path.abspath(output_path))
    
    # Lấy tên model từ tên file gốc (không bao gồm đuôi .parquet)
    model_name = os.path.basename(output_path).split('.')[0]
    
    # Tạo đường dẫn thư mục cho model
    model_dir = os.path.join(base_dir, model_name)
    
    # Tạo tên file mới với định dạng YYYY_MM_DD.parquet
    new_filename = f"{date_str}.parquet"
    
    # Tạo đường dẫn đầy đủ mới
    output_path = os.path.join(model_dir, new_filename)
    
    logging.info(f"Lưu dữ liệu vào thư mục model '{model_name}' với tên file '{new_filename}'")
    
    # Đảm bảo thư mục tồn tại
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    df.to_parquet(output_path)
    logging.info(f"Đã lưu file parquet sạch về: {output_path}")
    return output_path

# Hàm pipeline để kết hợp các hàm nhỏ
def pipeline(functions, initial_input):
    """Thực hiện tuần tự các hàm, output của hàm trước là input của hàm sau"""
    result = initial_input
    for func in functions:
        result = func(result)
    return result

# Hàm chính sử dụng functional composition
def clean_and_save_parquet(
    parquet_in,
    parquet_out_local,
    bucket_name=DEFAULT_BUCKET_NAME,
    specific_date=None
):
    """Đọc dữ liệu parquet từ S3, làm sạch và lưu xuống local
    
    Args:
        parquet_in: Đường dẫn S3 input với wildcards
        parquet_out_local: Đường dẫn local để lưu file parquet
        bucket_name: Tên bucket S3
        specific_date: Ngày cụ thể để lọc dữ liệu (datetime object)
    """
    try:
        # Tạo kết nối
        conn = create_s3_connection(S3_CONFIG)
        
        # Xây dựng và thực thi truy vấn
        s3_path = build_s3_path(bucket_name, parquet_in, specific_date)
        
        # Log đường dẫn được sử dụng
        logging.info(f"Đọc dữ liệu từ đường dẫn: {s3_path}")
        
        query = build_parquet_query(s3_path)
        df = read_parquet_from_s3(conn, query)
        
        # Log kết quả trung gian
        logging.info(f"Số dòng sau khi đọc: {len(df)}")
        
        # Không cần thêm ngày vào tên file nữa vì sẽ lưu vào thư mục riêng theo ngày
        # Lưu kết quả (truyền thêm specific_date để tạo thư mục theo ngày)
        save_to_parquet(df, parquet_out_local, specific_date=specific_date)
        
        return df
    except Exception as e:
        logging.error(f"Lỗi xảy ra khi xử lý file {parquet_in}: {e}")
        return None

def parse_date(date_str):
    """Parse date string to datetime object"""
    # Thử nhiều format date khác nhau
    formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d.%m.%Y']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Không thể parse date từ chuỗi '{date_str}'. Sử dụng định dạng DD/MM/YYYY.")

if __name__ == "__main__":
    # Thiết lập logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Tải dữ liệu từ S3 và chuyển đổi thành parquet")
    parser.add_argument('--date', type=str, help="Ngày để lọc dữ liệu (định dạng DD/MM/YYYY)")
    parser.add_argument('--all', action='store_true', help="Lấy tất cả dữ liệu (bỏ qua tham số date)")
    parser.add_argument('--source', type=str, help="Chỉ lấy dữ liệu từ nguồn cụ thể (users, orders, v.v.)")
    args = parser.parse_args()
    
    # Xử lý tham số date
    specific_date = None
    if args.date and not args.all:
        try:
            specific_date = parse_date(args.date)
            logging.info(f"Đang lọc dữ liệu cho ngày: {specific_date.strftime('%d/%m/%Y')}")
        except ValueError as e:
            logging.error(str(e))
            exit(1)
    
    data_sources = [
        {
            "name": "users",
            "parquet_in": "layer=staging/data_model=users/channel=*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/users.parquet"
        },
        {
            "name": "orders",
            "parquet_in": "layer=staging/data_model=orders/channel=*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/orders.parquet"
        },
        {
            "name": "orderchannels",
            "parquet_in": "layer=cleaned/data_model=order_channel/channel=*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/order_channel.parquet"
        },
        {
            "name": "orderitems",
            "parquet_in": "layer=staging/data_model=order_items/channel=*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/order_items.parquet"
        },
        {
            "name": "product",
            "parquet_in": "layer=cleaned/data_model=product/*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/product.parquet"
        },
        {
            "name": "geo_location",
            "parquet_in": "layer=cleaned/data_model=geo_location/channel=*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/geo_location.parquet"
        },
        {
            "name": "payment",
            "parquet_in": "layer=cleaned/data_model=payment/channel=*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/payment.parquet"
        },
        {
            "name": "sub_category",
            "parquet_in": "layer=cleaned/data_model=sub_category/channel=*/year=*/month=*/day=*/*.parquet",
            "parquet_out_local": "output/sub_category.parquet"
        }
    ]
    
    # Lọc nguồn dữ liệu nếu chỉ định cụ thể
    if args.source:
        data_sources = [source for source in data_sources if source["name"] == args.source]
        if not data_sources:
            logging.error(f"Không tìm thấy nguồn dữ liệu với tên: {args.source}")
            logging.info("Các nguồn có sẵn: users, orders, orderchannels, orderitems, product, geo_location, payment, sub_category")
            exit(1)
    
    # Hiện trạng thái
    status_msg = f"Đang lấy dữ liệu cho {'tất cả ngày' if not specific_date else specific_date.strftime('%d/%m/%Y')}"
    if args.source:
        status_msg += f" từ nguồn: {args.source}"
    else:
        status_msg += " từ tất cả nguồn"
    
    logging.info(status_msg)
    
    # Sử dụng phiên bản thông thường
    for source in data_sources:
        clean_and_save_parquet(
            parquet_in=source["parquet_in"],
            parquet_out_local=source["parquet_out_local"],
            specific_date=specific_date
        )