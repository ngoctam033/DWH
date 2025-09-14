from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException
import pandas as pd
import duckdb
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq
# Sử dụng BytesIO thay vì file tạm thời
from io import BytesIO
import os
import requests
import time
# Import config MinIO
from config.minio_config import (
    MINIO_ENDPOINT, MINIO_PORT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
    DEFAULT_BUCKET,
    get_minio_client, get_object_name, upload_json_to_minio, list_files_in_minio_dir
)

def get_clean_order_data(**context):
    """
    Đọc dữ liệu Parquet từ MinIO bằng DuckDB, trả về DataFrame sạch.
    """
    conf = context['dag_run'].conf
    params = context.get('params', {})
    channel = params.get('channel')
    data_model = params.get('data_model')
    bucket_name = params.get('bucket_name', 'datawarehouse')
    layer_in = params.get('layer_in')
    logical_date = conf.get('logical_date', context.get('logical_date'))

    parquet_in = get_object_name(layer_in, channel, data_model, logical_date, file_type='parquet')
    folder_prefix = parquet_in.rsplit('/', 1)[0]  # Lấy folder chứa file parquet

    logging.info(f"Bắt đầu lấy các order trong folder: s3://{bucket_name}/{folder_prefix}")

    # Đếm số file parquet trong folder
    client = get_minio_client()
    objects = client.list_objects(bucket_name, prefix=folder_prefix + '/', recursive=True)
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
    logging.info(f"Số lượng file parquet cần lấy: {len(parquet_files)}")
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
    SELECT DISTINCT order_code
    FROM read_parquet('s3://{bucket_name}/{parquet_in}')
    """
    logging.info(f"Chạy query lọc trùng dữ liệu: {query}")
    df_clean = conn.execute(query).fetchdf()
    logging.info(f"Số dòng sau khi lọc trùng: {len(df_clean)}")

    # Đẩy DataFrame đã lọc trùng lên XCom (dùng to_dict để serialize)
    context['ti'].xcom_push(key='cleaned_order_data', value=df_clean.to_dict(orient='records'))
    logging.info(f"Đã đẩy {len(df_clean)} bản ghi đã làm sạch lên XCom.")

def get_order_items(**context):
    """
    Lấy order_items từ API cho danh sách order_code, làm sạch và lưu vào MinIO dưới dạng JSON.
    """
    # Lấy danh sách order từ task trước
    order_list = context['ti'].xcom_pull(task_ids='get_order_data',key='cleaned_order_data')
    if not order_list:
        logging.error("Không có order nào từ task trước.")
        raise AirflowFailException("Danh sách order trống")

    # Lấy các tham số
    conf = context['dag_run'].conf
    params = context.get('params', {})
    channel = params.get('channel')
    bucket_name = params.get('bucket_name', DEFAULT_BUCKET)
    logical_date = conf.get('logical_date', context.get('logical_date'))
    logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")
    # Chuẩn bị các tham số
    api_url = "http://data_source:8000/extract-order-items"  # Thay thế bằng URL thực tế của API
    order_codes = [item['order_code'] for item in order_list if 'order_code' in item]
    
    logging.info(f"Chuẩn bị lấy order items cho {len(order_codes)} orders từ API: {api_url}")
    
    # Chuẩn bị tham số cho request
    all_order_items = []
    
    try:
        # Chuẩn bị tham số cho request
        params = [('order_codes', code) for code in order_codes]
        
        # Thực hiện request
        logging.info(f"Gửi request đến API với {len(order_codes)} mã đơn hàng")
        response = requests.get(api_url, params=params, timeout=60)
        
        # Kiểm tra status code
        if response.status_code == 200:
            data = response.json()
            
            # Kiểm tra kết quả
            if data.get('success') and 'response' in data:
                all_order_items = data['response'].get('order_item_list', [])
                logging.info(f"Lấy thành công {len(all_order_items)} order items từ {len(order_codes)} orders")
            else:
                error_msg = data.get('message', 'Không có dữ liệu trong response')
                logging.warning(f"API trả về success=False: {error_msg}")
        else:
            logging.error(f"API trả về mã lỗi {response.status_code}: {response.text}")
            raise AirflowFailException(f"API trả về mã lỗi {response.status_code}")
    
    except requests.RequestException as e:
        logging.error(f"Lỗi khi gọi API: {str(e)}")
        raise AirflowFailException(f"Lỗi khi gọi API: {str(e)}")
    
    except Exception as e:
        logging.error(f"Lỗi không xác định: {str(e)}")
        raise AirflowFailException(f"Lỗi không xác định: {str(e)}")
    
    logging.info(f"Đã lấy tổng cộng {len(all_order_items)} order items")
    
    # Xử lý dữ liệu và lưu vào MinIO
    if all_order_items:
        # Tạo đường dẫn đích trong MinIO
        target_path = get_object_name('raw', channel, 'order_items', logical_date, file_type='json')
        
        # Chuyển đổi danh sách order_items thành JSON
        json_data = json.dumps(all_order_items)
        
        # Đẩy lên MinIO
        minio_path = upload_json_to_minio(
            json_data=json_data,
            object_name=target_path
        )
        logging.info(f"Đã lưu dữ liệu JSON thành công vào MinIO: {minio_path}")
    
        logging.info(f"Đã lưu order_items vào MinIO dưới dạng JSON: s3://{bucket_name}/{target_path}")
        
        # Lưu đường dẫn file vào XCom để sử dụng sau này nếu cần
        context['ti'].xcom_push(key='order_items', value=minio_path)
    else:
        raise AirflowFailException("Không có order items nào để lưu")

# tạo hàm return về các file mới được tạo
def get_yesterday_file_paths(**context):
    """
    Lấy danh sách các đường dẫn file được tạo trong ngày hôm qua dựa vào context của DAG.
    Đẩy kết quả vào XCom để các task sau có thể sử dụng.
    
    Parameters:
    -----------
    **context : dict
        Context từ Airflow DAG, bao gồm logical_date và params
    
    Returns:
    --------
    None
        Kết quả được lưu vào XCom thay vì return trực tiếp
    """
    try:
        folder_path = context['ti'].xcom_pull(task_ids='get_order_items',key='order_items')
        if folder_path:
            logging.info(f"File path: {folder_path}")
            
            # Lấy tham số từ context
            params = context.get('params', {})
            layer = params.get('layer_inlets')  # Mặc định là 'raw'
            channel = params.get('channel')
            data_model = params.get('data_model')
            file_type = params.get('file_type')

            object_dir = os.path.dirname(folder_path) + '/'
            logging.info(f"Thư mục chứa file: {object_dir}")
            # Lấy danh sách file thực tế trong thư mục object_dir
            client = get_minio_client()
            bucket_name=DEFAULT_BUCKET
            objects = client.list_objects(bucket_name, prefix=object_dir, recursive=True)
            # in log ra list object
            for obj in objects:
                logging.info(f"Found object: {obj.object_name}")
        else:
            raise AirflowFailException("Không tìm thấy file nào để lấy đường dẫn")
    except Exception as e:
        logging.error(f"Lỗi không xác định: {str(e)}")
        raise AirflowFailException(f"Lỗi không xác định: {str(e)}")

# Tham số chung cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # <-- Không retry
    'retry_delay': timedelta(minutes=5)
}

# DAG cho Lazada
with DAG(
    dag_id='daily_extract_order_items_lazada',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Lazada',
    # schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['elt', 'parquet', 'lazada'],
    params={
        'channel': 'lazada',
        'layer_inlets': 'cleaned',
        'layer_in': 'staging',
        'file_type': 'parquet',
        'data_model': 'orders'
    }
) as lazada_dag:
    # truy cập vào trong MiniIO, lấy tất cả các file path .parquet trong thư mục cleaned/lazada/orders
    order_list = PythonOperator(
        task_id='get_order_data',
        python_callable=get_clean_order_data,
    )
    # gọi api để lấy tất cả các order_item tương ứng với order_list lấy được từ task trên
    order_items = PythonOperator(
        task_id='get_order_items',
        python_callable=get_order_items,
    )
    # Task cuối cùng: Trigger DAG transform_lazada_order_items_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_lazada_order_items_to_parquet',
        trigger_dag_id='transform_lazada_order_items_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}',  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    order_list >> order_items >> trigger_transform_dag

# DAG cho Shopee
with DAG(
    dag_id='daily_extract_order_items_shopee',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Shopee',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=['elt', 'parquet', 'shopee'],
    params={
        'channel': 'shopee',
        'layer_inlets': 'cleaned',
        'layer_in': 'cleaned',
        'file_type': 'parquet',
        'data_model': 'orders'
    }
) as shopee_dag:
    # truy cập vào trong MiniIO, lấy tất cả các file path .parquet trong thư mục cleaned/shopee/orders
    order_list = PythonOperator(
        task_id='get_order_data',
        python_callable=get_clean_order_data,
    )
    # gọi api để lấy tất cả các order_item tương ứng với order_list lấy được từ task trên
    order_items = PythonOperator(
        task_id='get_order_items',
        python_callable=get_order_items,
    )
    # Task cuối cùng: Trigger DAG transform_shopee_order_items_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_shopee_order_items_to_parquet',
        trigger_dag_id='transform_shopee_order_items_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}',  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    order_list >> order_items >> trigger_transform_dag

# DAG cho Tiki
with DAG(
    dag_id='daily_extract_order_items_tiki',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiki',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=['elt', 'parquet', 'tiki'],
    params={
        'channel': 'tiki',
        'layer_inlets': 'cleaned',
        'layer_in': 'cleaned',
        'file_type': 'parquet',
        'data_model': 'orders'
    }
) as tiki_dag:
    # truy cập vào trong MiniIO, lấy tất cả các file path .parquet trong thư mục cleaned/tiki/orders
    order_list = PythonOperator(
        task_id='get_order_data',
        python_callable=get_clean_order_data,
    )
    # gọi api để lấy tất cả các order_item tương ứng với order_list lấy được từ task trên
    order_items = PythonOperator(
        task_id='get_order_items',
        python_callable=get_order_items,
    )
    # Task cuối cùng: Trigger DAG transform_tiki_order_items_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_tiki_order_items_to_parquet',
        trigger_dag_id='transform_tiki_order_items_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}',  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    order_list >> order_items >> trigger_transform_dag

# DAG cho Tiktok
with DAG(
    dag_id='daily_extract_order_items_tiktok',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiktok',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=['elt', 'parquet', 'tiktok'],
    params={
        'channel': 'tiktok',
        'layer_inlets': 'cleaned',
        'layer_in': 'cleaned',
        'file_type': 'parquet',
        'data_model': 'orders'
    }
) as tiktok_dag:
    # truy cập vào trong MiniIO, lấy tất cả các file path .parquet trong thư mục cleaned/tiktok/orders
    order_list = PythonOperator(
        task_id='get_order_data',
        python_callable=get_clean_order_data,
    )
    # gọi api để lấy tất cả các order_item tương ứng với order_list lấy được từ task trên
    order_items = PythonOperator(
        task_id='get_order_items',
        python_callable=get_order_items,
    )
    # Task cuối cùng: Trigger DAG transform_tiktok_order_items_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_tiktok_order_items_to_parquet',
        trigger_dag_id='transform_tiktok_order_items_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}',  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    order_list >> order_items >> trigger_transform_dag

# DAG cho Website
with DAG(
    dag_id='daily_extract_order_items_website',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Website',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=['elt', 'parquet', 'website'],
    params={
        'channel': 'website',
        'layer_inlets': 'cleaned',
        'layer_in': 'cleaned',
        'file_type': 'parquet',
        'data_model': 'orders'
    }
) as website_dag:
    # truy cập vào trong MiniIO, lấy tất cả các file path .parquet trong thư mục cleaned/website/orders
    order_list = PythonOperator(
        task_id='get_order_data',
        python_callable=get_clean_order_data,
    )
    # gọi api để lấy tất cả các order_item tương ứng với order_list lấy được từ task trên
    order_items = PythonOperator(
        task_id='get_order_items',
        python_callable=get_order_items,
    )
    # Task cuối cùng: Trigger DAG transform_website_order_items_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_website_order_items_to_parquet',
        trigger_dag_id='transform_website_order_items_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}',  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    order_list >> order_items >> trigger_transform_dag