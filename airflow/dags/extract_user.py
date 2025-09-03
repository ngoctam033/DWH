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

# Định nghĩa các Dataset (asset) cho từng kênh theo cấu trúc mới (có partition)
SHOPEE_USER_DATASET = Dataset("s3://minio/raw/shopee/users/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
TIKTOK_USER_DATASET = Dataset("s3://minio/raw/tiktok/users/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
LAZADA_USER_DATASET = Dataset("s3://minio/raw/lazada/users/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
TIKI_USER_DATASET = Dataset("s3://minio/raw/tiki/users/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")
WEBSITE_USER_DATASET = Dataset("s3://minio/raw/website/users/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.json")

# Task 1: Gọi API lấy dữ liệu JSON từ nhiều nguồn khác nhau
def fetch_json_from_api(**context):
    logger = logging.getLogger("airflow.task")
    
    # Lấy tham số từ DAG hoặc truyền vào qua params, nếu không có sẽ dùng giá trị mặc định
    dag_run = context.get('dag_run')
    config = dag_run.conf if dag_run and dag_run.conf else {}

    # Lấy ngày từ config, nếu không có lấy từ logical_date hoặc ngày hiện tại
    logical_date = context.get('logical_date')

    date_str = (logical_date - timedelta(days=1)).strftime('%Y-%m-%d') # Mặc định là ngày hôm qua
    
    # Định nghĩa URL API và tham số động từ config
    # Thử lấy từ conf trước, nếu không có lấy từ params mặc định của DAG
    dag_params = context.get('params', {})
    base_url = dag_params.get('api_url')
    
    # Tham số API có thể tùy chỉnh từ config hoặc params của DAG
    order_channel = dag_params.get('order_channel')
     
    params = {
        "created_at": date_str,
        "order_channel": order_channel
    }
    
    # Thêm các tham số tùy chỉnh khác nếu có
    extra_params = config.get('extra_params', dag_params.get('extra_params', {}))
    if extra_params and isinstance(extra_params, dict):
        params.update(extra_params)
    
    logger.info(f"Bắt đầu gọi API để lấy dữ liệu JSON từ {base_url} với tham số {params}")
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        logger.info("Đã nhận được phản hồi từ API, status code: %s", response.status_code)
        
        # Chuyển đổi phản hồi thành JSON
        json_data = response.json()
        
        # Đếm số key ở cấp cao nhất của JSON
        if isinstance(json_data, dict):
            key_count = len(json_data.keys())
            logger.info(f"Số lượng key ở cấp cao nhất của JSON: {key_count}")
            logger.info(f"Danh sách key: {list(json_data.keys())}")
        else:
            logger.info(f"Kiểu dữ liệu trả về từ API không phải dict: {type(json_data)}")
        
        # Lưu toàn bộ JSON để sử dụng sau này
        context['ti'].xcom_push(key='json_data', value=json_data)
        
        # Lưu các tham số API để sử dụng trong các task khác
        context['ti'].xcom_push(key='params', value=params)
                    
    except Exception as e:
        logger.error("Lỗi khi gọi API: %s", str(e))
        raise

# Task 2: Xử lý phần tử đầu tiên từ JSON
def process_first_element(**context):
    logger = logging.getLogger("airflow.task")
    first_element = context['ti'].xcom_pull(key='first_element', task_ids='fetch_json_from_api')  # Giữ task_id cũ để đảm bảo tương thích với dữ liệu cũ
    
    if not first_element:
        logger.error("Không tìm thấy phần tử đầu tiên trong XCom")
        raise ValueError("Không tìm thấy phần tử đầu tiên trong XCom")
    
    # Lấy các key của phần tử đầu tiên
    keys = list(first_element.keys())
    logger.info(f"Các thuộc tính của phần tử đầu tiên: {keys}")
    
    # Lưu lại keys để sử dụng sau
    context['ti'].xcom_push(key='json_keys', value=keys)
    return keys

# Task 3: Log chi tiết của phần tử đầu tiên
def log_first_element_details(**context):
    logger = logging.getLogger("airflow.task")
    first_element = context['ti'].xcom_pull(key='first_element', task_ids='fetch_json_from_api')  # Giữ task_id cũ để đảm bảo tương thích với dữ liệu cũ
    
    if not first_element:
        logger.error("Không tìm thấy phần tử đầu tiên trong XCom")
        raise ValueError("Không tìm thấy phần tử đầu tiên trong XCom")
    
    # In chi tiết từng trường trong phần tử đầu tiên
    logger.info("Chi tiết phần tử JSON đầu tiên:")
    for key, value in first_element.items():
        logger.info(f"  {key}: {value}")
        
    return first_element

# Hàm tạo kết nối MinIO được định nghĩa trong minio_config.py

# Task 4: Lưu dữ liệu JSON lên MinIO theo cấu trúc thư mục định sẵn
def save_raw_json_to_minio(**context):
    logger = logging.getLogger("airflow.task")
    
    layer = "raw"
    params = context.get('params', {})
    # Lấy toàn bộ dữ liệu JSON từ task extract
    json_data = context['ti'].xcom_pull(key='json_data', task_ids='fetch_json_from_api')  # Giữ task_id cũ để đảm bảo tương thích với dữ liệu cũ
    channel = params.get('order_channel')
    data_model = params.get('data_model')  # Lấy data_model từ params
    if not json_data:
        logger.warning("Không có dữ liệu JSON để lưu vào MinIO")
        return
    
    try:
        # Lấy thông tin thực thi để đặt tên file
        logical_date = context.get('logical_date')
        
        # Lấy thông tin về kênh từ tham số API
        params = context['ti'].xcom_pull(key='params', task_ids='fetch_json_from_api')  # Giữ task_id cũ để đảm bảo tương thích với dữ liệu cũ
        if not params or 'order_channel' not in params:
            # raise lỗi
            raise ValueError("Không tìm thấy thông tin kênh trong tham số")
        else:
            channel = params['order_channel'].lower()
        
        # Sử dụng hàm từ minio_config để tạo tên file theo cấu trúc mới
        file_name = get_object_name(
            layer=layer,
            channel=channel,
            data_model=data_model,
            logical_date=logical_date
        )
        # Log thông tin order channel
        logger.info(f"File này thuộc order channel: {channel}")
        # in ra biens data model
        logger.info(f"Đã tạo MinIO: {data_model}")
        # Sử dụng hàm tiện ích từ minio_config để upload dữ liệu JSON
        minio_path = upload_json_to_minio(
            json_data=json_data,
            object_name=file_name
        )
        # in ra file name
        logger.info(f"Đã tạo tên file cho đối tượng trong MinIO: {file_name}")
        logger.info(f"Đã lưu dữ liệu JSON thành công vào MinIO: {minio_path}")
        
        # Lưu đường dẫn file vào XCom để sử dụng sau này nếu cần
        context['ti'].xcom_push(key='minio_json_path', value=minio_path)
        
    except Exception as e:
        logger.error(f"Lỗi khi lưu dữ liệu JSON vào MinIO: {e}")
        raise

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=1)  # Timeout mặc định là 1 phút
}

with DAG(
    dag_id='daily_extract_shopee_user',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-user, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 2 * * *',         # Lịch chạy: 02:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-user',
        'order_channel': 'shopee',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'users',  # Loại dữ liệu là users
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
        outlets=[SHOPEE_USER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Shopee
    )
    
    # Task 3: Trigger DAG transform_shopee_user_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_shopee_user_to_parquet',
        trigger_dag_id='transform_shopee_user_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'
        },
        wait_for_completion=True,  # False: Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

with DAG(
    dag_id='daily_extract_tiktok_user',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-user, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 2 * * *',         # Lịch chạy: 02:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-user',
        'order_channel': 'tiktok',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'users',
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
        outlets=[TIKTOK_USER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset TikTok
    )
    
    # Task 3: Trigger DAG transform_tiktok_user_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_tiktok_user_to_parquet',
        trigger_dag_id='transform_tiktok_user_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'
        },
        wait_for_completion=True,  # False: Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

with DAG(
    dag_id='daily_extract_tiki_user',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-user, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 2 * * *',         # Lịch chạy: 02:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-user',
        'order_channel': 'tiki',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'users',
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
        outlets=[TIKI_USER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Tiki
    )

    # Task 3: Trigger DAG transform_tiki_user_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_tiki_user_to_parquet',
        trigger_dag_id='transform_tiki_user_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'
        },
        wait_for_completion=True,  # False: Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

with DAG(
    dag_id='daily_extract_website_user',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-user, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 2 * * *',         # Lịch chạy: 02:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-user',
        'order_channel': 'website',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'users',
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
        outlets=[WEBSITE_USER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Website
    )
    
    # Task 3: Trigger DAG transform_website_user_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_website_user_to_parquet',
        trigger_dag_id='transform_website_user_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'
        },
        wait_for_completion=True,  # False: Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag

with DAG(
    dag_id='daily_extract_lazada_user',  # Định danh duy nhất cho DAG
    default_args=default_args,    # Tham số mặc định được định nghĩa ở trên
    description='Job hằng ngày gọi API extract-user, xử lý dữ liệu và lưu vào MinIO theo cấu trúc thư mục dữ liệu chuẩn',
    schedule='0 2 * * *',         # Lịch chạy: 02:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),  # Ngày bắt đầu chạy DAG
    catchup=False,                # False: không chạy các DAG trong quá khứ khi restart Airflow
    params={
        # Tham số mặc định có thể ghi đè khi trigger DAG
        'api_url': 'http://data_source:8000/extract-user',
        'order_channel': 'lazada',
        'days_offset': 1,  # Mặc định lấy dữ liệu của ngày hôm qua
        'data_model': 'users',
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
        outlets=[LAZADA_USER_DATASET]  # Đánh dấu task này sản xuất dữ liệu cho Dataset Lazada
    )

    # Task 3: Trigger DAG transform_lazada_user_to_parquet
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_lazada_user_to_parquet',
        trigger_dag_id='transform_lazada_user_to_parquet',  # ID của DAG cần trigger
        conf={
            'logical_date': '{{ ds }}'
        },
        wait_for_completion=True,  # False: Không chờ DAG được trigger hoàn thành
    )

    # Định nghĩa luồng thực thi
    fetch_json_task >> save_to_minio_task >> trigger_transform_dag
