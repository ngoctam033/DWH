"""
MinIO connection configuration for Airflow DAGs.
This module provides utility functions to connect and interact with MinIO.
"""
import logging
import io
from datetime import datetime
from minio import Minio
import json

# MinIO connection information
MINIO_ENDPOINT = "minio1"  # Use service name in docker-compose
MINIO_PORT = 9000
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_SECURE = False  # Use HTTP instead of HTTPS

# Bucket information
DEFAULT_BUCKET = "datawarehouse"
JSON_FOLDER = "json-data"

# File name format configuration
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
    Upload file bytes (Parquet, CSV, etc.) to MinIO
    
    Parameters:
    -----------
    file_bytes : bytes
        File data in bytes (e.g., from buffer.getvalue().to_pybytes())
    object_name : str
        Object name on MinIO (full path)
    bucket_name : str, optional
        Bucket name, default is the bucket configured in the module
    content_type : str, optional
        File content type, default is "application/octet-stream"
    
    Returns:
    --------
    str
        Full path of the uploaded object in the format s3://{bucket}/{object}
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
        logging.info(f"Successfully uploaded file to MinIO: s3://{bucket_name}/{object_name}")
        return f"s3://{bucket_name}/{object_name}"
    except Exception as e:
        logging.error(f"Error uploading file to MinIO: {e}")
        raise

def get_minio_client():
    """
    Create and return a client connection to the MinIO server.
    
    Returns:
    --------
    minio.Minio
        Instance of connected Minio client
    """
    try:        
        # Create MinIO client
        client = Minio(
            f"{MINIO_ENDPOINT}:{MINIO_PORT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        
        return client
    except Exception as e:
        logging.error(f"Error creating MinIO connection: {e}")
        raise

def get_object_name(layer, channel, data_model, logical_date, task_id=None, file_type="json"):
    """
    Generate a standard file name for the object in MinIO according to partition structure.
    Note: The date in the path will be DAG run date - 1 day (yesterday)
    
    Parameters:
    -----------
    layer : str
        Data layer (raw, staging, cleaned, aggregated)
    channel : str
        Channel name (shopee, lazada, tiki, website, erp)
    data_model : str
        Data type (orders, users, products, etc.)
    logical_date : datetime or str
        DAG execution date, can be a datetime object or string in YYYY-MM-DD format
    task_id : str, optional
        Task ID, default is None
    file_type : str, optional
        File type, default is "json"
        
    Returns:
    --------
    str
        Full object name in MinIO according to partition structure
    """
    from datetime import datetime, timedelta
    
    # Convert logical_date to datetime and get yesterday
    if hasattr(logical_date, 'strftime'):
        # If logical_date is already a datetime object
        yesterday = logical_date - timedelta(days=1)
    else:
        # If logical_date is a string 'YYYY-MM-DD', convert to datetime then subtract 1 day
        try:
            exec_date = datetime.strptime(logical_date, '%Y-%m-%d')
            yesterday = exec_date - timedelta(days=1)
        except ValueError:
            # Handle invalid format
            raise ValueError(f"Invalid date format: {logical_date}. Must be in 'YYYY-MM-DD' format")
    
    # Get components of yesterday
    year = yesterday.strftime('%Y')
    month = yesterday.strftime('%m')
    day = yesterday.strftime('%d')
    date_str = yesterday.strftime('%Y-%m-%d')
    
    # Create file name
    file_name = f"{date_str}_data.{file_type}"

    # Create full path according to partitioned folder structure
    object_path = f"layer={layer}/year={year}/month={month}/day={day}/channel={channel}/data_model={data_model}/{file_name}"
    
    # Log returned path
    logging.info(f"Object path in MinIO: {object_path}")

    return object_path

def list_files_in_minio_dir(object_dir):
    client = get_minio_client()
    bucket_name=DEFAULT_BUCKET
    # object_dir must end with "/"
    if not object_dir.endswith('/'):
        object_dir += '/'
    objects = client.list_objects(bucket_name, prefix=object_dir, recursive=True)
    file_paths = [obj.object_name for obj in objects]
    return file_paths

def upload_json_to_minio(json_data, object_name, bucket_name=DEFAULT_BUCKET):
    """
    Upload JSON data to MinIO. If object_name already exists, automatically add a suffix to avoid overwriting.
    
    Parameters:
    -----------
    json_data : dict or list
        JSON data to upload
    object_name : str
        Object name on MinIO (full path)
    bucket_name : str, optional
        Bucket name, default is the bucket configured in the module
        
    Returns:
    --------
    str
        Full path of the uploaded object in the format s3://{bucket}/{object}
    """
    try:
        # Convert JSON data to bytes
        json_bytes = json.dumps(json_data, indent=2).encode('utf-8')
        json_buffer = io.BytesIO(json_bytes)
        json_buffer_length = len(json_bytes)
        
        client = get_minio_client()

        # Check if object exists, if so add suffix _1, _2, ...
        base_name = object_name
        suffix = 1
        while True:
            try:
                client.stat_object(bucket_name, object_name)
                # If no error, object exists
                dot_idx = base_name.rfind('.')
                if dot_idx != -1:
                    object_name = f"{base_name[:dot_idx]}_{suffix}{base_name[dot_idx:]}"
                else:
                    object_name = f"{base_name}_{suffix}"
                suffix += 1
            except Exception:
                # If error (object does not exist), break
                break

        # Upload file
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=json_buffer,
            length=json_buffer_length,
            content_type="application/json"
        )
        
        logging.info(f"Successfully uploaded JSON data to MinIO: s3://{bucket_name}/{object_name}")
        return f"s3://{bucket_name}/{object_name}"
    except Exception as e:
        logging.error(f"Error uploading JSON data to MinIO: {e}")
        raise

def download_json_from_minio(object_name, bucket_name=DEFAULT_BUCKET):
    """
    Download and return JSON data from MinIO
    
    Parameters:
    -----------
    object_name : str
        Object name on MinIO (full path)
    bucket_name : str, optional
        Bucket name, default is the bucket configured in the module
        
    Returns:
    --------
    dict or list
        JSON data parsed from file
    """
    
    # Log detailed information before download
    logging.info(f"Preparing to download file from MinIO with:")
    logging.info(f"  - bucket_name: '{bucket_name}'")
    logging.info(f"  - object_name: '{object_name}'")
    logging.info(f"  - full path: s3://{bucket_name}/{object_name}")
    
    try:
        # Connect to MinIO and download file
        client = get_minio_client()
        
        # Log more information about MinIO endpoint
        logging.info(f"  - MinIO endpoint: {MINIO_ENDPOINT}:{MINIO_PORT}")
        
        # Check if bucket exists
        bucket_exists = client.bucket_exists(bucket_name)
        logging.info(f"  - Bucket '{bucket_name}' exists: {bucket_exists}")
        
        # Try to check if object exists
        try:
            stat = client.stat_object(bucket_name, object_name)
            logging.info(f"  - Object exists, size: {stat.size} bytes")
        except Exception as stat_error:
            logging.warning(f"  - Cannot check object: {stat_error}")
        
        response = client.get_object(
            bucket_name=bucket_name,
            object_name=object_name
        )
        
        # Read and parse JSON data
        content_bytes = response.read()
        logging.info(f"  - Read {len(content_bytes)} bytes from object")
        
        json_data = json.loads(content_bytes.decode('utf-8'))
        response.close()
        
        logging.info(f"Successfully downloaded JSON data from MinIO: s3://{bucket_name}/{object_name}")
        return json_data
    except Exception as e:
        logging.error(f"Error downloading JSON data from MinIO: {e}")
        logging.error(f"Details: bucket_name={bucket_name}, object_name={object_name}")
        raise
