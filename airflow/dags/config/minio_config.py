"""
Cấu hình kết nối MinIO cho các DAG của Airflow.
Module này cung cấp các hàm tiện ích để kết nối và tương tác với MinIO.
"""
import logging
import io
from datetime import datetime
from minio import Minio
import json

# Thông tin kết nối MinIO
MINIO_ENDPOINT = "minio1"  # Sử dụng tên service trong docker-compose
MINIO_PORT = 9000
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_SECURE = False  # Sử dụng HTTP thay vì HTTPS

# Thông tin về bucket
DEFAULT_BUCKET = "datawarehouse"
JSON_FOLDER = "json-data"

# Cấu hình định dạng tên file
# FILE_NAME_FORMAT = "{dag_id}/{logical_date}_{task_id}.json"

FOLDER_STRUCTURE = {
    "raw": {
        "shopee": ["users", "orders", "orders_items", "product_review", "discount"],
        "lazada": ["users", "orders", "products"],
        "tiki": ["users", "orders", "products"],
        "website": ["users", "orders", "products"],
        "erp": ["product"]
    },
    "staging": {},
    "cleaned": {},
    "aggregated": {}
}

def upload_file_to_minio(file_bytes, object_name, bucket_name=DEFAULT_BUCKET, content_type="application/octet-stream"):
    """
    Upload file bytes (Parquet, CSV, v.v.) lên MinIO
    
    Parameters:
    -----------
    file_bytes : bytes
        Dữ liệu file dạng bytes (ví dụ: từ buffer.getvalue().to_pybytes())
    object_name : str
        Tên đối tượng trên MinIO (đường dẫn đầy đủ)
    bucket_name : str, optional
        Tên bucket, mặc định là bucket được cấu hình trong module
    content_type : str, optional
        Loại nội dung file, mặc định là "application/octet-stream"
    
    Returns:
    --------
    str
        Đường dẫn đầy đủ của đối tượng đã upload theo định dạng s3://{bucket}/{object}
    """
    try:
        file_buffer = io.BytesIO(file_bytes)
        file_length = len(file_bytes)
        client = get_minio_client()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=file_buffer,
            length=file_length,
            content_type=content_type
        )
        logging.info(f"Đã upload thành công file lên MinIO: s3://{bucket_name}/{object_name}")
        return f"s3://{bucket_name}/{object_name}"
    except Exception as e:
        logging.error(f"Lỗi khi upload file lên MinIO: {e}")
        raise

def get_minio_client():
    """
    Tạo và trả về một kết nối client đến MinIO server.
    
    Returns:
    --------
    minio.Minio
        Instance của Minio client đã được kết nối
    """
    try:        
        # Tạo client MinIO
        client = Minio(
            f"{MINIO_ENDPOINT}:{MINIO_PORT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        
        return client
    except Exception as e:
        logging.error(f"Lỗi khi tạo kết nối MinIO: {e}")
        raise

def get_object_name(layer, channel, data_model, logical_date, task_id=None, file_type="json"):
    """
    Tạo tên file chuẩn cho đối tượng trong MinIO theo cấu trúc phân vùng (partition)
    Lưu ý: Ngày trong đường dẫn sẽ là ngày chạy DAG - 1 ngày (hôm qua)
    
    Parameters:
    -----------
    layer : str
        Tầng dữ liệu (raw, staging, cleaned, aggregated)
    channel : str
        Tên của kênh (shopee, lazada, tiki, website, erp)
    data_model : str
        Loại dữ liệu (orders, users, products, v.v.)
    logical_date : datetime hoặc str
        Ngày thực thi của DAG, có thể là đối tượng datetime hoặc chuỗi định dạng YYYY-MM-DD
    task_id : str, optional
        ID của task, mặc định là None
    file_type : str, optional
        Loại file, mặc định là "json"
        
    Returns:
    --------
    str
        Tên đầy đủ của đối tượng trong MinIO theo cấu trúc phân vùng
    """
    from datetime import datetime, timedelta
    
    # Chuyển đổi logical_date thành datetime và tính ngày hôm qua
    if hasattr(logical_date, 'strftime'):
        # Nếu logical_date đã là datetime object
        yesterday = logical_date - timedelta(days=1)
    else:
        # Nếu logical_date là chuỗi 'YYYY-MM-DD', chuyển thành datetime rồi trừ 1 ngày
        try:
            exec_date = datetime.strptime(logical_date, '%Y-%m-%d')
            yesterday = exec_date - timedelta(days=1)
        except ValueError:
            # Xử lý trường hợp format không đúng
            raise ValueError(f"Định dạng ngày không hợp lệ: {logical_date}. Phải có định dạng 'YYYY-MM-DD'")
    
    # Lấy các thành phần của yesterday
    year = yesterday.strftime('%Y')
    month = yesterday.strftime('%m')
    day = yesterday.strftime('%d')
    date_str = yesterday.strftime('%Y-%m-%d')
    
    # Tạo tên file
    file_name = f"{date_str}_data.{file_type}"

    # Tạo đường dẫn đầy đủ theo cấu trúc thư mục phân vùng
    object_path = f"{layer}/{channel}/{data_model}/year={year}/month={month}/day={day}/{file_name}"
    
    # Log đường dẫn trả về
    logging.info(f"Đường dẫn đối tượng trong MinIO: {object_path}")

    return object_path

def list_files_in_minio_dir(object_dir):
    client = get_minio_client()
    bucket_name=DEFAULT_BUCKET
    # object_dir phải kết thúc bằng dấu "/"
    if not object_dir.endswith('/'):
        object_dir += '/'
    objects = client.list_objects(bucket_name, prefix=object_dir, recursive=True)
    file_paths = [obj.object_name for obj in objects]
    return file_paths

def upload_json_to_minio(json_data, object_name, bucket_name=DEFAULT_BUCKET):
    """
    Upload dữ liệu JSON lên MinIO. Nếu object_name đã tồn tại thì tự động thêm hậu tố để tránh ghi đè.
    
    Parameters:
    -----------
    json_data : dict hoặc list
        Dữ liệu JSON cần upload
    object_name : str
        Tên đối tượng trên MinIO (đường dẫn đầy đủ)
    bucket_name : str, optional
        Tên bucket, mặc định là bucket được cấu hình trong module
        
    Returns:
    --------
    str
        Đường dẫn đầy đủ của đối tượng đã upload theo định dạng s3://{bucket}/{object}
    """
    try:
        # Chuyển đổi dữ liệu JSON thành bytes
        json_bytes = json.dumps(json_data, indent=2).encode('utf-8')
        json_buffer = io.BytesIO(json_bytes)
        json_buffer_length = len(json_bytes)
        
        client = get_minio_client()

        # Kiểm tra object đã tồn tại chưa, nếu có thì thêm hậu tố _1, _2, ...
        base_name = object_name
        suffix = 1
        while True:
            try:
                client.stat_object(bucket_name, object_name)
                # Nếu không lỗi, tức là object đã tồn tại
                dot_idx = base_name.rfind('.')
                if dot_idx != -1:
                    object_name = f"{base_name[:dot_idx]}_{suffix}{base_name[dot_idx:]}"
                else:
                    object_name = f"{base_name}_{suffix}"
                suffix += 1
            except Exception:
                # Nếu lỗi (object chưa tồn tại) thì break
                break

        # Upload file
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=json_buffer,
            length=json_buffer_length,
            content_type="application/json"
        )
        
        logging.info(f"Đã upload thành công dữ liệu JSON lên MinIO: s3://{bucket_name}/{object_name}")
        return f"s3://{bucket_name}/{object_name}"
    except Exception as e:
        logging.error(f"Lỗi khi upload dữ liệu JSON lên MinIO: {e}")
        raise

def download_json_from_minio(object_name, bucket_name=DEFAULT_BUCKET):
    """
    Download và trả về dữ liệu JSON từ MinIO
    
    Parameters:
    -----------
    object_name : str
        Tên đối tượng trên MinIO (đường dẫn đầy đủ)
    bucket_name : str, optional
        Tên bucket, mặc định là bucket được cấu hình trong module
        
    Returns:
    --------
    dict hoặc list
        Dữ liệu JSON đã được parse từ file
    """
    
    # Log thông tin chi tiết trước khi download
    logging.info(f"Chuẩn bị download file từ MinIO với:")
    logging.info(f"  - bucket_name: '{bucket_name}'")
    logging.info(f"  - object_name: '{object_name}'")
    logging.info(f"  - đường dẫn đầy đủ: s3://{bucket_name}/{object_name}")
    
    try:
        # Kết nối đến MinIO và download file
        client = get_minio_client()
        
        # Log thêm thông tin về endpoint MinIO
        logging.info(f"  - MinIO endpoint: {MINIO_ENDPOINT}:{MINIO_PORT}")
        
        # Kiểm tra bucket tồn tại không
        bucket_exists = client.bucket_exists(bucket_name)
        logging.info(f"  - Bucket '{bucket_name}' tồn tại: {bucket_exists}")
        
        # Thử kiểm tra object có tồn tại không
        try:
            stat = client.stat_object(bucket_name, object_name)
            logging.info(f"  - Object tồn tại, kích thước: {stat.size} bytes")
        except Exception as stat_error:
            logging.warning(f"  - Không thể kiểm tra object: {stat_error}")
        
        response = client.get_object(
            bucket_name=bucket_name,
            object_name=object_name
        )
        
        # Đọc và parse dữ liệu JSON
        content_bytes = response.read()
        logging.info(f"  - Đã đọc {len(content_bytes)} bytes từ object")
        
        json_data = json.loads(content_bytes.decode('utf-8'))
        response.close()
        
        logging.info(f"Đã download thành công dữ liệu JSON từ MinIO: s3://{bucket_name}/{object_name}")
        return json_data
    except Exception as e:
        logging.error(f"Lỗi khi download dữ liệu JSON từ MinIO: {e}")
        logging.error(f"Chi tiết: bucket_name={bucket_name}, object_name={object_name}")
        raise
