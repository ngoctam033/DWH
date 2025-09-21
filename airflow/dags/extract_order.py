from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset
import requests
import logging
from datetime import datetime, timedelta

# Import file config chứa thông tin kết nối MinIO
# File này nằm trong thư mục config
from config.minio_config import (
    get_object_name, upload_json_to_minio
)

from extract_user import fetch_json_from_api, save_raw_json_to_minio

# Định nghĩa các Dataset (asset) cho từng kênh theo cấu trúc mới (có partition)
# SHOPEE_ORDER_DATASET = Dataset("s3://minio/raw/shopee/orders/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
# TIKTOK_ORDER_DATASET = Dataset("s3://minio/raw/tiktok/orders/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
# LAZADA_ORDER_DATASET = Dataset("s3://minio/raw/lazada/orders/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
# TIKI_ORDER_DATASET = Dataset("s3://minio/raw/tiki/orders/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
# WEBSITE_ORDER_DATASET = Dataset("s3://minio/raw/website/orders/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")

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
        # #outlets=[LAZADA_ORDER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Lazada
    )

    # Task cuối cùng: Trigger DAG transform_lazada_order_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_lazada_order_to_parquet',
        trigger_dag_id='transform_lazada_order_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

# DAG shopee
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
        # #outlets=[SHOPEE_ORDER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Shopee
    )

    # Task cuối cùng: Trigger DAG transform_shopee_order_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_shopee_order_to_parquet',
        trigger_dag_id='transform_shopee_order_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

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
        # #outlets=[TIKI_ORDER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Tiki
    )

    # Task cuối cùng: Trigger DAG transform_tiki_order_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_tiki_order_to_parquet',
        trigger_dag_id='transform_tiki_order_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

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
        # #outlets=[WEBSITE_ORDER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Website
    )

    # Task cuối cùng: Trigger DAG transform_website_order_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_website_order_to_parquet',
        trigger_dag_id='transform_website_order_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

# DAG tiktok
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
        # #outlets=[TIKTOK_ORDER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Tiktok
    )

    # Task cuối cùng: Trigger DAG transform_tiktok_order_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_tiktok_order_to_parquet',
        trigger_dag_id='transform_tiktok_order_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'  # Truyền logical_date (ngày chạy của DAG)
        },
        wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag