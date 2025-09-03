from multiprocessing import context
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset
import pandas as pd
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from typing import Dict, List, Any
# Sử dụng BytesIO thay vì file tạm thời
from io import BytesIO

# Import datasets từ DAG extract_order
from extract_order import (
    SHOPEE_ORDER_DATASET, TIKTOK_ORDER_DATASET, 
    TIKI_ORDER_DATASET, WEBSITE_ORDER_DATASET,
    LAZADA_ORDER_DATASET
)

from staging_user import get_yesterday_file_paths, download_all_json_files, convert_to_parquet_and_save
# Import thư viện kết nối MinIO
from config.minio_config import (
    get_object_name, download_json_from_minio
)

# Định nghĩa asset đầu ra - với cấu trúc partition theo year/month/day
SHOPEE_ORDER_PARQUET = Dataset("s3://minio/staging/shopee/orders/year={{execution_date.year}}/month={{execution_date.strftime('%m')}}/day={{execution_date.strftime('%d')}}/{{ds}}_data.parquet")
TIKTOK_ORDER_PARQUET = Dataset("s3://minio/staging/tiktok/orders/year={{execution_date.year}}/month={{execution_date.strftime('%m')}}/day={{execution_date.strftime('%d')}}/{{ds}}_data.parquet")
TIKI_ORDER_PARQUET = Dataset("s3://minio/staging/tiki/orders/year={{execution_date.year}}/month={{execution_date.strftime('%m')}}/day={{execution_date.strftime('%d')}}/{{ds}}_data.parquet")
LAZADA_ORDER_PARQUET = Dataset("s3://minio/staging/lazada/orders/year={{execution_date.year}}/month={{execution_date.strftime('%m')}}/day={{execution_date.strftime('%d')}}/{{ds}}_data.parquet")
WEBSITE_ORDER_PARQUET = Dataset("s3://minio/staging/website/orders/year={{execution_date.year}}/month={{execution_date.strftime('%m')}}/day={{execution_date.strftime('%d')}}/{{ds}}_data.parquet")

# Tham số chung cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
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
                    'payment_id', 'customer_id', 'shipping_id', 'total_price',
                    'shipping_fee', 'shipping_cost', 'logistics_partner_id'
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

# DAG cho Lazada
with DAG(
    dag_id='transform_lazada_order_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Lazada',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'lazada'],
    params={
        'channel': 'lazada',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'orders'
    }
) as lazada_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        # inlets=[LAZADA_ORDER_DATASET]
    )

    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save,
        outlets=[LAZADA_ORDER_PARQUET],
    )
    # Task cuối cùng: Trigger DAG clean_order_data_lazada_with_duckdb
    trigger_clean_dag = TriggerDagRunOperator(
        task_id='trigger_clean_order_data_lazada',
        trigger_dag_id='clean_order_data_lazada_with_duckdb',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}',  # Truyền logical_date (ngày chạy của DAG)
            'channel': 'lazada',        # Truyền thêm thông tin kênh
        },
        wait_for_completion=True,  # Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet >> trigger_clean_dag

with DAG(
    dag_id='transform_shopee_order_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Shopee',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'shopee'],
    params={
        'channel': 'shopee',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'orders'
    }
) as shopee_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        inlets=[SHOPEE_ORDER_DATASET]
    )

    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save,
        outlets=[SHOPEE_ORDER_PARQUET],
    )

    get_file_path >> download_file >> parse_json >> convert_to_parquet

with DAG(
    dag_id='transform_tiki_order_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiki',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'tiki'],
    params={
        'channel': 'tiki',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'orders'
    }
) as tiki_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        inlets=[TIKI_ORDER_DATASET]
    )

    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save,
        outlets=[TIKI_ORDER_PARQUET],
    )

    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Website
with DAG(
    dag_id='transform_website_order_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Website',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'website'],
    params={
        'channel': 'website',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'orders'
    }
) as website_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        inlets=[WEBSITE_ORDER_DATASET]
    )

    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save,
        outlets=[WEBSITE_ORDER_PARQUET],
    )

    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Tiktok
with DAG(
    dag_id='transform_tiktok_order_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiktok',
    schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'tiktok'],
    params={
        'channel': 'tiktok',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'orders'
    }
) as tiktok_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        inlets=[TIKTOK_ORDER_DATASET]
    )

    download_file = PythonOperator(
        task_id='download_all_json_files',
        python_callable=download_all_json_files,
    )

    parse_json = PythonOperator(
        task_id='parse_json_to_table',
        python_callable=parse_json_to_table,
    )

    convert_to_parquet = PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet_and_save,
        outlets=[TIKTOK_ORDER_PARQUET],
    )

    get_file_path >> download_file >> parse_json >> convert_to_parquet

