import duckdb
import logging
import os
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
def build_s3_path(bucket_name, path):
    """Tạo đường dẫn S3 đầy đủ"""
    return f"s3://{bucket_name}/{path}"

# Hàm thuần túy để tạo truy vấn SQL
def build_parquet_query(path):
    """Tạo câu truy vấn SQL để đọc parquet"""
    if "/orders/" in path:
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
                        CAST(to_timestamp(CAST(order_date AS DOUBLE)) AS VARCHAR)
                    ELSE CAST(order_date AS VARCHAR)  -- Ép kiểu giá trị gốc thành VARCHAR
                END AS order_date,  -- Chuyển đổi timestamp hợp lệ
                order_code, 
                order_channel,
                payment_id,
                customer_code, 
                shipping_id, 
                total_price, 
                shipping_cost, 
                logistics_partner_id
            FROM read_parquet('{path}', union_by_name=True)
            ORDER BY created_at
        """
    if "/order_channel/" in path:
        return f"""
                    SELECT DISTINCT id, name, is_active
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/order_items/" in path:
        return f"""
                    SELECT * 
                    FROM read_parquet('{path}', union_by_name=True)
                """
    if "/users/" in path:
        return f"""
                    SELECT * 
                    FROM read_parquet('{path}', union_by_name=True)
                """
    raise ValueError("Đường dẫn không hợp lệ hoặc không được hỗ trợ")

# Hàm với side effect rõ ràng để đọc dữ liệu
def read_parquet_from_s3(conn, query):
    """Đọc dữ liệu parquet từ S3 thông qua DuckDB"""
    logging.info(f"Chạy query: {query}")
    return conn.execute(query).fetchdf()

# Hàm với side effect rõ ràng để lưu dữ liệu
def save_to_parquet(df, output_path):
    """Lưu DataFrame vào file parquet local"""
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
    bucket_name=DEFAULT_BUCKET_NAME
):
    """Đọc dữ liệu parquet từ S3, làm sạch và lưu xuống local"""
    try:
        # Tạo kết nối
        conn = create_s3_connection(S3_CONFIG)
        
        # Xây dựng và thực thi truy vấn
        s3_path = build_s3_path(bucket_name, parquet_in)
        query = build_parquet_query(s3_path)
        df = read_parquet_from_s3(conn, query)
        
        # Log kết quả trung gian
        logging.info(f"Số dòng sau khi đọc: {len(df)}")
        
        # Lưu kết quả
        save_to_parquet(df, parquet_out_local)
        
        return df
    except Exception as e:
        logging.error(f"Lỗi xảy ra khi xử lý file {parquet_in}: {e}")
        return None

if __name__ == "__main__":
    # Thiết lập logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    data_sources = [
        {
            "name": "users",
            "parquet_in": "cleaned/*/users/**/*.parquet",
            "parquet_out_local": "output/cleaned_users.parquet"
        },
        {
            "name": "orders",
            "parquet_in": "cleaned/*/orders/**/*.parquet",
            "parquet_out_local": "output/cleaned_orders.parquet"
        },
        {
            "name": "orderchannels",
            "parquet_in": "cleaned/*/order_channel/**/*.parquet",
            "parquet_out_local": "output/cleaned_order_channel.parquet"
        },
        {
            "name": "orderitems",
            "parquet_in": "cleaned/*/order_items/**/*.parquet",
            "parquet_out_local": "output/cleaned_order_items.parquet"
        }
    ]
    # Sử dụng phiên bản thông thường
    for source in data_sources:
        clean_and_save_parquet(
            parquet_in=source["parquet_in"],
            parquet_out_local=source["parquet_out_local"]
        )