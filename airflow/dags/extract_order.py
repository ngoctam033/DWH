
# Chuẩn hệ thống
import logging
from datetime import datetime, timedelta
from io import BytesIO

# Thư viện bên thứ ba
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset

# Nội bộ dự án
from config.minio_config import (
    MINIO_ENDPOINT, MINIO_PORT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
    DEFAULT_BUCKET,
    get_minio_client, get_object_name, upload_json_to_minio
)
from staging_user import get_yesterday_file_paths, download_all_json_files, convert_to_parquet_and_save
from extract_user import fetch_json_from_api, save_raw_json_to_minio

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=1)  # Timeout mặc định là 1 phút
}

def parse_json_to_table(**context):
    """
    Chuyển đổi dữ liệu JSON order_list thành dạng bảng.
    
    Parameters:
    -----------
    **context : dict
        Context từ Airflow DAG, dùng để lấy dữ liệu từ task trước
    
    Returns:
    --------
    Dict
        Dictionary chứa kết quả parse dưới dạng bảng
    """
    # Lấy dữ liệu đã download từ task trước
    downloaded_files = context['ti'].xcom_pull(task_ids='download_all_json_files', key='downloaded_files')
    # Lấy tham số từ context
    params = context.get('params', {})
    order_channel = params.get('channel')
    logging.info(f"Order channel hiện tại: {order_channel}")
    if not downloaded_files:
        error_msg = "Không tìm thấy dữ liệu JSON từ task trước"
        logging.error(error_msg)
        raise ValueError(error_msg)

    order_list = []
    if order_channel == 'shopee':
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
                
                # Trích xuất order_list từ cấu trúc response
                if 'response' in content and 'order_list' in content['response']:
                    order_list = content['response']['order_list']
                else:
                    # Thử tìm order_list ở root level
                    order_list = content.get('order_list')
                    if not order_list:
                        raise ValueError(f"Không tìm thấy 'order_list' trong JSON của file '{file_key}'")
                
                # Kiểm tra order_list có phải là list
                if not isinstance(order_list, list):
                    raise ValueError(f"'order_list' phải là danh sách, không phải {type(order_list)}")
                
                # Sửa 2 dòng log này
                logging.info(f"Đã parse thành công file '{file_key}': {len(order_list)} rows")
                if order_list:
                    columns = list(order_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
                
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    elif order_channel == 'tiktok':
        logging.info("Bắt đầu parse dữ liệu Tiktok")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']

                # Lấy danh sách orders từ cấu trúc JSON mẫu TikTok
                # content['data']['order_list']
                if (
                    isinstance(content, dict)
                    and 'data' in content
                    and isinstance(content['data'], dict)
                    and 'order_list' in content['data']
                ):
                    order_list = content['data']['order_list']
                else:
                    # Thử tìm ở các vị trí khác có thể có
                    order_list = content.get('order_list', content.get('orders'))
                    if not order_list:
                        raise ValueError(f"Không tìm thấy dữ liệu order_list trong JSON của file '{file_key}'")

                # Kiểm tra order_list có phải là list
                if not isinstance(order_list, list):
                    raise ValueError(f"'order_list' phải là danh sách, không phải {type(order_list)}")

                # Log thông tin
                logging.info(f"Đã parse thành công file '{file_key}': {len(order_list)} rows")
                if order_list:
                    columns = list(order_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")

                # Thêm log cho metadata từ TikTok nếu có
                if 'total' in content.get('data', {}):
                    logging.info(f"Tổng số orders: {content['data']['total']}")
                if 'more' in content.get('data', {}):
                    logging.info(f"Còn trang tiếp theo không: {content['data']['more']}")
                if 'created_count' in content:
                    logging.info(f"Số đơn hàng mới tạo: {content['created_count']}")

            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    elif order_channel == 'tiki':
        logging.info("Bắt đầu parse dữ liệu Tiki")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']

                # Trích xuất order_list từ cấu trúc Tiki (nằm trong data.orders)
                if 'data' in content and 'orders' in content['data']:
                    order_list = content['data']['orders']
                else:
                    # Thử tìm ở các vị trí khác có thể có
                    order_list = content.get('orders', content.get('order_list', []))
                    if not order_list and 'data' in content:
                        order_list = content['data']
                    if not order_list:
                        raise ValueError(f"Không tìm thấy dữ liệu orders trong JSON của file '{file_key}'")

                # Kiểm tra order_list có phải là list
                if not isinstance(order_list, list):
                    raise ValueError(f"'orders' phải là danh sách, không phải {type(order_list)}")

                # Log thông tin
                logging.info(f"Đã parse thành công file '{file_key}': {len(order_list)} rows")
                if order_list:
                    columns = list(order_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")

                # Thêm log cho metadata từ Tiki
                if 'summary' in content.get('data', {}):
                    summary = content['data']['summary']
                    logging.info(f"Metadata từ Tiki: Tổng số bản ghi: {summary.get('total_orders', 'không xác định')}")

            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    elif order_channel == 'lazada':
        logging.info("Bắt đầu parse dữ liệu Lazada")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
    
                # Lấy danh sách orders từ cấu trúc JSON mẫu Lazada
                if (
                    isinstance(content, dict)
                    and 'data' in content
                    and isinstance(content['data'], dict)
                    and 'orders' in content['data']
                ):
                    order_list = content['data']['orders']
                else:
                    order_list = content.get('orders', content.get('order_list'))
                    if not order_list:
                        raise ValueError(f"Không tìm thấy dữ liệu orders trong JSON của file '{file_key}'")
    
                # Kiểm tra order_list có phải là list
                if not isinstance(order_list, list):
                    raise ValueError(f"'orders' phải là danh sách, không phải {type(order_list)}")
    
                # Lọc chỉ lấy các trường cần thiết
                required_fields = [
                    'id', 'profit', 'status', 'created_at', 'order_code', 'order_date',
                    'payment_id', 'customer_code', 'shipping_id', 'total_price',
                    'shipping_cost', 'logistics_partner_id'
                ]
                filtered_orders = []
                for order in order_list:
                    filtered_order = {k: order.get(k) for k in required_fields}
                    filtered_orders.append(filtered_order)
                order_list = filtered_orders
    
                logging.info(f"Đã parse thành công file '{file_key}': {len(order_list)} rows")
                if order_list:
                    columns = list(order_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
    
                # Thêm log cho metadata từ Lazada
                if 'count' in content.get('data', {}):
                    logging.info(f"Tổng số orders: {content['data']['count']}")
                if 'has_next' in content.get('data', {}):
                    logging.info(f"Còn trang tiếp theo không: {content['data']['has_next']}")
                if 'created_orders' in content:
                    logging.info(f"Số đơn hàng mới tạo: {content['created_orders']}")
    
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    # ...existing code...
    elif order_channel == 'website':
        logging.info("Bắt đầu parse dữ liệu Website")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
    
                # Lấy danh sách orders từ trường 'orders'
                order_list = content.get('orders', [])
                if not order_list:
                    raise ValueError(f"Không tìm thấy dữ liệu orders trong JSON của file '{file_key}'")
    
                # Kiểm tra order_list có phải là list
                if not isinstance(order_list, list):
                    raise ValueError(f"'orders' phải là danh sách, không phải {type(order_list)}")
    
                # Log thông tin
                logging.info(f"Đã parse thành công file '{file_key}': {len(order_list)} rows")
                if order_list:
                    columns = list(order_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
    
                # Thêm log cho metadata từ Website
                if 'meta' in content:
                    meta = content['meta']
                    logging.info(f"Metadata từ Website: Tổng số bản ghi: {meta.get('count', 'không xác định')}, Số đơn hàng mới tạo: {meta.get('created_count', 'không xác định')}")
    
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    # Lưu kết quả vào XCom
    context['ti'].xcom_push(key='parsed_tables', value=order_list)  

def clean_order_data(**context):
    """
    Đọc dữ liệu Parquet từ MinIO, làm sạch bằng DuckDB, trả về DataFrame sạch.
    """
    conf = context['dag_run'].conf
    params = context.get('params', {})
    channel = params.get('channel')
    data_model = params.get('data_model')
    bucket_name = params.get('bucket_name', 'datawarehouse')
    layer_in = params.get('layer_in')
    logical_date = conf.get('logical_date', context.get('logical_date'))

    logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")

    parquet_in = get_object_name(layer_in, channel, data_model, logical_date, file_type='parquet')
    folder_prefix = parquet_in.rsplit('/', 1)[0]  # Lấy folder chứa file parquet

    logging.info(f"Bắt đầu lọc trùng dữ liệu trong folder: s3://{bucket_name}/{folder_prefix}")

    # Đếm số file parquet trong folder
    client = get_minio_client()
    objects = client.list_objects(bucket_name, prefix=folder_prefix + '/', recursive=True)
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
    logging.info(f"Số lượng file parquet cần lọc trùng: {len(parquet_files)}")
    if parquet_files:
        logging.info(f"Danh sách file: {parquet_files}")

    conn = duckdb.connect(database=':memory:')
    conn.install_extension('httpfs')
    conn.load_extension('httpfs')
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}:{MINIO_PORT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    conn.execute(f"SET s3_url_style='path';")
    conn.execute(f"SET s3_use_ssl={'true' if MINIO_SECURE else 'false'};")

    query = f"""
    SELECT *
    FROM read_parquet('s3://{bucket_name}/{parquet_in}')
    """
    logging.info(f"Chạy query lọc trùng dữ liệu: {query}")
    df_clean = conn.execute(query).fetchdf()
    logging.info(f"Số dòng sau khi lọc trùng: {len(df_clean)}")

    # Đẩy DataFrame đã lọc trùng lên XCom (dùng to_dict để serialize)
    context['ti'].xcom_push(key='cleaned_order_data', value=df_clean.to_dict(orient='records'))
    logging.info(f"Đã đẩy {len(df_clean)} bản ghi đã làm sạch lên XCom.")

def save_cleaned_order_data(**context):
    """
    Nhận DataFrame sạch từ XCom, lưu lại file parquet vào MinIO.
    """
    conf = context['dag_run'].conf
    params = context.get('params', {})
    channel = params.get('channel', 'shopee')
    data_model = params.get('data_model', 'users')
    bucket_name = params.get('bucket_name', 'datawarehouse')
    layer_out = params.get('layer_out', 'cleaned')
    logical_date = conf.get('logical_date', context.get('logical_date'))

    logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")

    parquet_out = get_object_name(layer_out, channel, data_model, logical_date, file_type='parquet')

    # Lấy dữ liệu sạch từ XCom
    cleaned_data = context['ti'].xcom_pull(task_ids='clean_order_data', key='cleaned_order_data')
    if not cleaned_data:
        raise ValueError("Không tìm thấy dữ liệu đã làm sạch từ XCom")

    df_clean = pa.Table.from_pandas(pd.DataFrame(cleaned_data))
    buffer = BytesIO()
    pq.write_table(df_clean, buffer)
    buffer.seek(0)

    client = get_minio_client()
    client.put_object(
        bucket_name=bucket_name,
        object_name=parquet_out,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    logging.info(f"Đã lưu dữ liệu đã làm sạch lên MinIO: s3://{bucket_name}/{parquet_out}")
    context['ti'].xcom_push(key='cleaned_parquet_path', value=parquet_out)
    return parquet_out

# DAG lazada
with DAG(
    dag_id='daily_extract_lazada_order',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-order, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 0 * * *',         # Lịch chạy: 00:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-order',
        'order_channel': 'lazada',
        'channel': 'lazada',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'layer_in': 'staging',
        'bucket_name': 'datawarehouse',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'orders',
        'extra_params': {},  # Các tham số API bổ sung
    }
) as dag:

    # Task 1: Gọi API và lấy dữ liệu JSON
    # Đây là bước extract trong quy trình ETL: trích xuất dữ liệu từ nguồn
    fetch_json_task = PythonOperator(
        task_id='fetch_json_from_api',  # Giữ nguyên task_id để không ảnh hưởng đến các runs trước đó
        python_callable=fetch_json_from_api,  # Gọi hàm fetch_json_from_api đã định nghĩa ở trên
    )
    
    # Task 2: Lưu dữ liệu JSON vào MinIO
    # Đây là bước load trong quy trình ETL: lưu trữ dữ liệu đã xử lý vào data lake
    save_to_minio_task = PythonOperator(
        task_id='save_raw_json_to_minio',
        python_callable=save_raw_json_to_minio,
    )

    # Task 3:
    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        #inlets=[SHOPEE_ORDER_DATASET]
    )

    # Task 4:
    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    # Task 5:
    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    # Task 6: Chuyển đổi dữ liệu sang định dạng Parquet và lưu vào MinIO
    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save
    )

    # Task 7: Làm sạch dữ liệu với DuckDB
    clean_data = PythonOperator(
        task_id='clean_order_data',
        python_callable=clean_order_data,
    )

    # task 8:
    save_data = PythonOperator(
        task_id='save_cleaned_order_data',
        python_callable=save_cleaned_order_data,
        #outlets = [WEBSITE_ORDER_CLEANED_PARQUET]
    )
    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> get_file_path >> download_file >> parse_json >> convert_to_parquet >> clean_data >> save_data

# DAG Shopee
with DAG(
    dag_id='daily_extract_shopee_order',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-order, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 0 * * *',         # Lịch chạy: 00:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-order',
        'order_channel': 'shopee',
        'channel': 'shopee',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'layer_in': 'staging',
        'bucket_name': 'datawarehouse',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'orders',
        'extra_params': {},  # Các tham số API bổ sung
    }
) as dag:

    # Task 1: Gọi API và lấy dữ liệu JSON
    # Đây là bước extract trong quy trình ETL: trích xuất dữ liệu từ nguồn
    fetch_json_task = PythonOperator(
        task_id='fetch_json_from_api',  # Giữ nguyên task_id để không ảnh hưởng đến các runs trước đó
        python_callable=fetch_json_from_api,  # Gọi hàm fetch_json_from_api đã định nghĩa ở trên
    )
    
    # Task 2: Lưu dữ liệu JSON vào MinIO
    # Đây là bước load trong quy trình ETL: lưu trữ dữ liệu đã xử lý vào data lake
    save_to_minio_task = PythonOperator(
        task_id='save_raw_json_to_minio',
        python_callable=save_raw_json_to_minio,
    )

    # Task 3:
    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        #inlets=[SHOPEE_ORDER_DATASET]
    )

    # Task 4:
    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    # Task 5:
    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    # Task 6: Chuyển đổi dữ liệu sang định dạng Parquet và lưu vào MinIO
    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save
    )

    # Task 7: Làm sạch dữ liệu với DuckDB
    clean_data = PythonOperator(
        task_id='clean_order_data',
        python_callable=clean_order_data,
    )

    # task 8:
    save_data = PythonOperator(
        task_id='save_cleaned_order_data',
        python_callable=save_cleaned_order_data,
        #outlets = [WEBSITE_ORDER_CLEANED_PARQUET]
    )
    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> get_file_path >> download_file >> parse_json >> convert_to_parquet >> clean_data >> save_data

# DAG tiki
with DAG(
    dag_id='daily_extract_tiki_order',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-order, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 0 * * *',         # Lịch chạy: 00:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-order',
        'order_channel': 'tiki',
        'channel': 'tiki',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'layer_in': 'staging',
        'bucket_name': 'datawarehouse',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'orders',
        'extra_params': {},  # Các tham số API bổ sung
    }
) as dag:

    # Task 1: Gọi API và lấy dữ liệu JSON
    # Đây là bước extract trong quy trình ETL: trích xuất dữ liệu từ nguồn
    fetch_json_task = PythonOperator(
        task_id='fetch_json_from_api',  # Giữ nguyên task_id để không ảnh hưởng đến các runs trước đó
        python_callable=fetch_json_from_api,  # Gọi hàm fetch_json_from_api đã định nghĩa ở trên
    )
    
    # Task 2: Lưu dữ liệu JSON vào MinIO
    # Đây là bước load trong quy trình ETL: lưu trữ dữ liệu đã xử lý vào data lake
    save_to_minio_task = PythonOperator(
        task_id='save_raw_json_to_minio',
        python_callable=save_raw_json_to_minio,
    )

    # Task 3:
    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        #inlets=[SHOPEE_ORDER_DATASET]
    )

    # Task 4:
    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    # Task 5:
    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    # Task 6: Chuyển đổi dữ liệu sang định dạng Parquet và lưu vào MinIO
    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save
    )

    # Task 7: Làm sạch dữ liệu với DuckDB
    clean_data = PythonOperator(
        task_id='clean_order_data',
        python_callable=clean_order_data,
    )

    # task 8:
    save_data = PythonOperator(
        task_id='save_cleaned_order_data',
        python_callable=save_cleaned_order_data,
        #outlets = [WEBSITE_ORDER_CLEANED_PARQUET]
    )
    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> get_file_path >> download_file >> parse_json >> convert_to_parquet >> clean_data >> save_data

# DAG Tiktok
with DAG(
    dag_id='daily_extract_tiktok_order',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-order, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 0 * * *',         # Lịch chạy: 00:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-order',
        'order_channel': 'tiktok',
        'channel': 'tiktok',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'layer_in': 'staging',
        'bucket_name': 'datawarehouse',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'orders',
        'extra_params': {},  # Các tham số API bổ sung
    }
) as dag:

    # Task 1: Gọi API và lấy dữ liệu JSON
    # Đây là bước extract trong quy trình ETL: trích xuất dữ liệu từ nguồn
    fetch_json_task = PythonOperator(
        task_id='fetch_json_from_api',  # Giữ nguyên task_id để không ảnh hưởng đến các runs trước đó
        python_callable=fetch_json_from_api,  # Gọi hàm fetch_json_from_api đã định nghĩa ở trên
    )
    
    # Task 2: Lưu dữ liệu JSON vào MinIO
    # Đây là bước load trong quy trình ETL: lưu trữ dữ liệu đã xử lý vào data lake
    save_to_minio_task = PythonOperator(
        task_id='save_raw_json_to_minio',
        python_callable=save_raw_json_to_minio,
    )

    # Task 3:
    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        #inlets=[SHOPEE_ORDER_DATASET]
    )

    # Task 4:
    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    # Task 5:
    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    # Task 6: Chuyển đổi dữ liệu sang định dạng Parquet và lưu vào MinIO
    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save
    )

    # Task 7: Làm sạch dữ liệu với DuckDB
    clean_data = PythonOperator(
        task_id='clean_order_data',
        python_callable=clean_order_data,
    )

    # task 8:
    save_data = PythonOperator(
        task_id='save_cleaned_order_data',
        python_callable=save_cleaned_order_data,
        #outlets = [WEBSITE_ORDER_CLEANED_PARQUET]
    )
    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> get_file_path >> download_file >> parse_json >> convert_to_parquet >> clean_data >> save_data

# DAG website
with DAG(
    dag_id='daily_extract_website_order',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-order, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 0 * * *',         # Lịch chạy: 00:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-order',
        'order_channel': 'website',
        'channel': 'website',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'layer_in': 'staging',
        'bucket_name': 'datawarehouse',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'orders',
        'extra_params': {},  # Các tham số API bổ sung
    }
) as dag:

    # Task 1: Gọi API và lấy dữ liệu JSON
    # Đây là bước extract trong quy trình ETL: trích xuất dữ liệu từ nguồn
    fetch_json_task = PythonOperator(
        task_id='fetch_json_from_api',  # Giữ nguyên task_id để không ảnh hưởng đến các runs trước đó
        python_callable=fetch_json_from_api,  # Gọi hàm fetch_json_from_api đã định nghĩa ở trên
    )
    
    # Task 2: Lưu dữ liệu JSON vào MinIO
    # Đây là bước load trong quy trình ETL: lưu trữ dữ liệu đã xử lý vào data lake
    save_to_minio_task = PythonOperator(
        task_id='save_raw_json_to_minio',
        python_callable=save_raw_json_to_minio,
    )

    # Task 3:
    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        #inlets=[SHOPEE_ORDER_DATASET]
    )

    # Task 4:
    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    # Task 5:
    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    # Task 6: Chuyển đổi dữ liệu sang định dạng Parquet và lưu vào MinIO
    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save
    )

    # Task 7: Làm sạch dữ liệu với DuckDB
    clean_data = PythonOperator(
        task_id='clean_order_data',
        python_callable=clean_order_data,
    )

    # task 8:
    save_data = PythonOperator(
        task_id='save_cleaned_order_data',
        python_callable=save_cleaned_order_data,
        #outlets = [WEBSITE_ORDER_CLEANED_PARQUET]
    )
    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> get_file_path >> download_file >> parse_json >> convert_to_parquet >> clean_data >> save_data