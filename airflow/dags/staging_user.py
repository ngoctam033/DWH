from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException
import pandas as pd
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
# Sử dụng BytesIO thay vì file tạm thời
from io import BytesIO
import os

# Import thư viện kết nối MinIO
from config.minio_config import (
    get_object_name, download_json_from_minio, list_files_in_minio_dir
)

# Tham số chung cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # <-- Không retry
    'retry_delay': timedelta(minutes=5)
}

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
    conf = context['dag_run'].conf
    # logical date lấy trong context, nếu không có thì lấy ngày hôm qua
    logical_date = conf.get('logical_date', context.get('logical_date'))
    logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")
    if not logical_date:
        raise ValueError("logical_date không tồn tại trong context 1.")
    
    # Lấy tham số từ context
    params = context.get('params', {})
    layer = params.get('layer_inlets')  # Mặc định là 'raw'
    channel = params.get('channel')
    data_model = params.get('data_model')
    file_type = params.get('file_type')
    
    if not channel:
        raise ValueError("Tham số 'channel' phải được cung cấp trong params của DAG")
    
    # Kết quả chứa đường dẫn file
    result = []
    
    # Nếu có data_model cụ thể
    if data_model:
        object_name = get_object_name(layer, channel, data_model, logical_date, file_type=file_type)
        object_dir = os.path.dirname(object_name)
        logging.info(f"Thư mục chứa file: {object_dir}")
        # Lấy danh sách file thực tế trong thư mục object_dir
        result = list_files_in_minio_dir(object_dir)
        logging.info(f"Danh sách file trong thư mục: {result}")
    else:
        error_msg = "Tham số 'data_model' không được cung cấp trong params của DAG"
        logging.error(error_msg)
        raise ValueError(error_msg)
    # Kiểm tra nếu kết quả trống
    if not result:
        error_msg = f"Không tìm thấy file nào cho {channel}/{data_model} ngày {logical_date} trong thư mục {object_dir}"
        logging.error(error_msg)
        # Đánh dấu task là failed
        raise AirflowFailException(error_msg)
    
    # Kiểm tra và chuyển đổi logical_date nếu cần
    if isinstance(logical_date, str):
        logical_date = datetime.strptime(logical_date, '%Y-%m-%d')  # Chuyển từ chuỗi sang datetime
    logging.info(f"Danh sách file ngày hôm qua ({logical_date.strftime('%Y-%m-%d')}): {result}")

    # Lưu kết quả vào XCom để các task sau có thể sử dụng
    context['ti'].xcom_push(key='yesterday_file_paths', value=result)

def download_all_json_files(**context):
    """
    Lấy danh sách đường dẫn từ XCom của task trước, sau đó download tất cả các file JSON từ MinIO.
    Kết quả download được lưu vào XCom dưới dạng dictionary với key là tên file.
    
    Parameters:
    -----------
    **context : dict
        Context từ Airflow DAG
    
    Returns:
    --------
    None
        Dữ liệu được lưu vào XCom thay vì return trực tiếp
    """
    # Lấy danh sách đường dẫn từ task get_yesterday_file_paths
    file_paths = context['ti'].xcom_pull(task_ids='get_yesterday_file_paths', key='yesterday_file_paths')
    
    if not file_paths:
        logging.error("Không tìm thấy danh sách đường dẫn file từ task trước")
        raise ValueError("Không tìm thấy danh sách đường dẫn file từ task trước")
    
    # Lấy tham số từ context
    params = context.get('params', {})
    bucket_name = params.get('bucket_name')
    
    # Dictionary lưu nội dung các file đã download
    downloaded_files = {}
    
    logging.info(f"Bắt đầu download {len(file_paths)} file JSON từ MinIO")
    
    # Duyệt qua từng đường dẫn và download file
    for object_name in file_paths:
        try:
            logging.info(f"Đang download file: {object_name}")

            # Download file từ MinIO
            file_content = download_json_from_minio(object_name, bucket_name) if bucket_name else download_json_from_minio(object_name)
            
            # Lưu nội dung vào dictionary
            downloaded_files[object_name] = {
                'content': file_content,
                'path': object_name
            }
            
            # Log thông tin file đã download
            logging.info(f"Đã download thành công file '{object_name}' từ: s3://{bucket_name or 'DEFAULT_BUCKET'}/{object_name}")
            if isinstance(file_content, dict):
                logging.info(f"  - File '{object_name}' có {len(file_content)} keys chính")
            elif isinstance(file_content, list):
                logging.info(f"  - File '{object_name}' có {len(file_content)} items")
            else:
                logging.info(f"  - File '{object_name}' có kiểu dữ liệu: {type(file_content)}")

        except Exception as e:
            logging.error(f"Lỗi khi download file '{object_name}': {e}")
    
    # Lưu kết quả vào XCom để các task sau có thể sử dụng
    context['ti'].xcom_push(key='downloaded_files', value=downloaded_files)
    
    # Tóm tắt kết quả
    logging.info(f"Đã download thành công {len(downloaded_files)}/{len(file_paths)} file JSON")
    # Kiểm tra số lượng file đã download
    if len(downloaded_files) != len(file_paths):
        error_msg = f"Không thể download tất cả các file."
        logging.error(error_msg)
        raise ValueError(error_msg)
    
def parse_json_to_table(**context):
    """
    Chuyển đổi dữ liệu JSON user_list thành dạng bảng.
    
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

    user_list = []
    if order_channel == 'shopee':
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
                
                # Trích xuất user_list từ cấu trúc response
                if 'response' in content and 'user_list' in content['response']:
                    user_list = content['response']['user_list']
                else:
                    # Thử tìm user_list ở root level
                    user_list = content.get('user_list')
                    if not user_list:
                        raise ValueError(f"Không tìm thấy 'user_list' trong JSON của file '{file_key}'")
                
                # Kiểm tra user_list có phải là list
                if not isinstance(user_list, list):
                    raise ValueError(f"'user_list' phải là danh sách, không phải {type(user_list)}")
                
                # Sửa 2 dòng log này
                logging.info(f"Đã parse thành công file '{file_key}': {len(user_list)} rows")
                if user_list:
                    columns = list(user_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
                
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    elif order_channel == 'tiktok':
        logging.info("Bắt đầu parse dữ liệu Tiktok")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
                
                # Trích xuất user_list từ cấu trúc của TikTok
                if 'data' in content and 'user_list' in content['data']:
                    user_list = content['data']['user_list']
                else:
                    # Thử tìm user_list ở root level
                    user_list = content.get('user_list')
                    if not user_list:
                        raise ValueError(f"Không tìm thấy 'user_list' trong JSON của file '{file_key}'")
                
                # Kiểm tra user_list có phải là list
                if not isinstance(user_list, list):
                    raise ValueError(f"'user_list' phải là danh sách, không phải {type(user_list)}")
                
                # Log thông tin
                logging.info(f"Đã parse thành công file '{file_key}': {len(user_list)} rows")
                if user_list:
                    columns = list(user_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
                
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    elif order_channel == 'tiki':
        logging.info("Bắt đầu parse dữ liệu Tiki")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
                
                # Trích xuất user_list từ cấu trúc Tiki (nằm trong data.users.data)
                if ('data' in content and 'users' in content['data'] and 
                    'data' in content['data']['users']):
                    user_list = content['data']['users']['data']
                else:
                    # Thử tìm ở các vị trí khác có thể có
                    user_list = content.get('users', content.get('user_list', []))
                    if not user_list and 'data' in content:
                        user_list = content['data']
                    if not user_list:
                        raise ValueError(f"Không tìm thấy dữ liệu người dùng trong JSON của file '{file_key}'")
                
                # Kiểm tra user_list có phải là list
                if not isinstance(user_list, list):
                    raise ValueError(f"'users.data' phải là danh sách, không phải {type(user_list)}")
                
                # Log thông tin
                logging.info(f"Đã parse thành công file '{file_key}': {len(user_list)} rows")
                if user_list:
                    columns = list(user_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
                    
                # Thêm log cho metadata từ Tiki
                if 'summary' in content.get('data', {}).get('users', {}):
                    summary = content['data']['users']['summary']
                    logging.info(f"Metadata từ Tiki: Tổng số bản ghi: {summary.get('total_count', 'không xác định')}")
                    
                # Log thông tin về paging nếu có
                if 'paging' in content.get('data', {}):
                    logging.info(f"Thông tin phân trang từ Tiki: {content['data']['paging'].get('cursors', {})}")
                
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    elif order_channel == 'lazada':
        logging.info("Bắt đầu parse dữ liệu Lazada")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
                
                # Trích xuất user_list từ cấu trúc Lazada (nằm trong data.users)
                if 'data' in content and 'users' in content['data']:
                    user_list = content['data']['users']
                else:
                    # Thử tìm ở các vị trí khác có thể có
                    user_list = content.get('users', content.get('user_list'))
                    if not user_list:
                        raise ValueError(f"Không tìm thấy dữ liệu người dùng trong JSON của file '{file_key}'")
                
                # Kiểm tra user_list có phải là list
                if not isinstance(user_list, list):
                    raise ValueError(f"'users' phải là danh sách, không phải {type(user_list)}")
                
                # Log thông tin
                logging.info(f"Đã parse thành công file '{file_key}': {len(user_list)} rows")
                if user_list:
                    columns = list(user_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
                    
                # Thêm log cho metadata từ Lazada
                if 'pagination' in content.get('data', {}):
                    pagination = content['data']['pagination']
                    logging.info(f"Metadata phân trang: trang {pagination.get('current', 1)}/{pagination.get('total', 1)}, " 
                                f"{pagination.get('pageSize', 0)} bản ghi/trang")
                
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    elif order_channel == 'website':
        logging.info("Bắt đầu parse dữ liệu Website")
        for file_key, file_data in downloaded_files.items():
            try:
                content = file_data['content']
                
                # Website có cấu trúc JSON đơn giản hơn, user_list nằm trực tiếp trong 'data'
                if 'data' in content:
                    user_list = content['data']
                else:
                    # Thử tìm ở các vị trí khác có thể có
                    user_list = content.get('users', content.get('user_list', []))
                    if not user_list:
                        raise ValueError(f"Không tìm thấy dữ liệu người dùng trong JSON của file '{file_key}'")
                
                # Kiểm tra user_list có phải là list
                if not isinstance(user_list, list):
                    raise ValueError(f"'data' phải là danh sách, không phải {type(user_list)}")
                
                # Log thông tin
                logging.info(f"Đã parse thành công file '{file_key}': {len(user_list)} rows")
                if user_list:
                    columns = list(user_list[0].keys())
                    logging.info(f"Các cột trong bảng: {', '.join(columns)}")
                    
                # Log metadata từ Website nếu có
                if 'metadata' in content:
                    metadata = content['metadata']
                    logging.info(f"Metadata từ Website: Số lượng: {metadata.get('count', 'không xác định')}, "
                                f"Phiên bản: {metadata.get('version', 'không xác định')}")
                    
                # Log thông tin trạng thái và thời gian
                logging.info(f"Trạng thái API: {content.get('status', 'không xác định')}")
                if 'timestamp' in content:
                    logging.info(f"Thời gian tạo dữ liệu: {content['timestamp']}")
                
            except Exception as e:
                logging.error(f"Lỗi khi parse file '{file_key}': {e}")
                raise
    # Lưu kết quả vào XCom
    context['ti'].xcom_push(key='parsed_tables', value=user_list)  

def convert_to_parquet_and_save(**context):
    """
    Chuyển đổi user_list thành DataFrame và lưu dưới dạng Parquet vào MinIO.
    Sử dụng BytesIO thay vì file tạm thời để hiệu quả hơn.
    
    Parameters:
    -----------
    **context : dict
        Context từ Airflow DAG
    
    Returns:
    --------
    str
        Đường dẫn đến file Parquet đã tạo
    """
    # Lấy user_list từ task parse_json_to_table
    user_list = context['ti'].xcom_pull(task_ids='parse_json_to_table', key='parsed_tables')
    conf = context['dag_run'].conf
    if not user_list:
        error_msg = "Không tìm thấy user_list từ task trước"
        logging.warning(error_msg)  # Đổi từ logging.error sang logging.warning
        # Không raise lỗi nữa, chỉ log và cho qua
        # raise ValueError(error_msg)
    
    # Lấy logical_date từ context
    logical_date = conf.get('logical_date', context.get('logical_date'))
    logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")
    # Lấy tham số từ context
    params = context.get('params', {})
    layer = params.get('layer_outlet')
    channel = params.get('channel')
    data_model = params.get('data_model')
    bucket_name = params.get('bucket_name')
    
    try:
        # Chuyển user_list thành DataFrame
        df = pd.DataFrame(user_list)
        
        # Thêm cột order_channel từ params
        order_channel = params.get('channel')
        df['order_channel'] = order_channel

        # Log thông tin DataFrame
        logging.info(f"Đã chuyển đổi user_list thành DataFrame:")
        logging.info(f"  - Số dòng: {len(df)}")
        logging.info(f"  - Số cột: {len(df.columns)}")
        logging.info(f"  - Các cột: {', '.join(df.columns.tolist())}")
        
        # Tạo tên file parquet
        object_name = get_object_name(layer, channel, data_model, logical_date, file_type='parquet')
        logging.info(f"Đường dẫn đối tượng trong MinIO: {object_name}")
        
        # Chuyển toàn bộ giá trị trong DataFrame về string (trừ None)
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list))
                else str(x) if x is not None else None
            )        
        buffer = BytesIO()
        
        # Chuyển DataFrame thành PyArrow Table và ghi vào buffer
        table = pa.Table.from_pandas(df)
        logging.info("Đang chuyển đổi DataFrame thành dữ liệu Parquet")
        pq.write_table(table, buffer)
        
        # Đặt con trỏ về đầu buffer
        buffer.seek(0)
        
        # Upload dữ liệu nhị phân trực tiếp từ buffer lên MinIO
        logging.info(f"Đang upload dữ liệu Parquet lên MinIO: {object_name}")
        
        # Lấy MinIO client
        from config.minio_config import get_minio_client
        client = get_minio_client()
        
        # Upload trực tiếp từ buffer
        client.put_object(
            bucket_name=bucket_name or 'datawarehouse',
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        
        # Log thành công
        logging.info(f"Đã lưu thành công file Parquet: s3://{bucket_name or 'DEFAULT_BUCKET'}/{object_name}")
        
        # Lưu đường dẫn vào XCom
        context['ti'].xcom_push(key='parquet_file_path', value=object_name)
        
        return object_name
        
    except Exception as e:
        logging.error(f"Lỗi khi chuyển đổi và lưu file Parquet: {e}")
        raise

# --- Kết thúc các hàm xử lý ---

# DAG cho Shopee
with DAG(
    dag_id='transform_shopee_user_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Shopee',
    #schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'shopee'],
    params={
        'channel': 'shopee',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'users'
    }
) as shopee_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        ##inlets=[SHOPEE_USER_DATASET]
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
        #outlets=[SHOPEE_USER_PARQUET],
    )
    # Task cuối cùng: Trigger DAG clean_user_data_shopee_with_duckdb
    # trigger_clean_dag = TriggerDagRunOperator(
    #     task_id='trigger_clean_user_data_shopee',
    #     trigger_dag_id='clean_user_data_shopee_with_duckdb',  # ID của DAG cần trigger
    #     conf={
    #         'logical_date': '{{ ds }}',  # Truyền logical_date nếu cần
    #     },
    #     wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    # )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Tiktok
with DAG(
    dag_id='transform_tiktok_user_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiktok',
    #schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'tiktok'],
    params={
        'channel': 'tiktok',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'users'
    }
) as tiktok_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        # ##inlets=[TIKTOK_USER_DATASET]
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
        #outlets=[TIKTOK_USER_PARQUET],
    )

    # Task cuối cùng: Trigger DAG clean_user_data_tiktok_with_duckdb
    # trigger_clean_dag = TriggerDagRunOperator(
    #     task_id='trigger_clean_user_data_tiktok',
    #     trigger_dag_id='clean_user_data_tiktok_with_duckdb',  # ID của DAG cần trigger
    #     conf={
    #         'logical_date': '{{ ds }}',  # Truyền logical_date nếu cần
    #     },
    #     wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    # )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Lazada
with DAG(
    dag_id='transform_lazada_user_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Lazada',
    #schedule='0 3 * * *',         # Lịch chạy: 03:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'lazada'],
    params={
        'channel': 'lazada',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'users'
    }
) as lazada_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        ##inlets=[LAZADA_USER_DATASET]
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
        #outlets=[LAZADA_USER_PARQUET],
    )

    # Task cuối cùng: Trigger DAG clean_user_data_lazada_with_duckdb
    # trigger_clean_dag = TriggerDagRunOperator(
    #     task_id='trigger_clean_user_data_lazada',
    #     trigger_dag_id='clean_user_data_lazada_with_duckdb',  # ID của DAG cần trigger
    #     conf={
    #         'logical_date': '{{ ds }}',  # Truyền logical_date nếu cần
    #     },
    #     wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    # )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Tiki
with DAG(
    dag_id='transform_tiki_user_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiki',
    #schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'tiki'],
    params={
        'channel': 'tiki',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'users'
    }
) as tiki_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        ##inlets=[TIKI_USER_DATASET]
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
        #outlets=[TIKI_USER_PARQUET],
    )

    # Task cuối cùng: Trigger DAG clean_user_data_tiki_with_duckdb
    # trigger_clean_dag = TriggerDagRunOperator(
    #     task_id='trigger_clean_user_data_tiki',
    #     trigger_dag_id='clean_user_data_tiki_with_duckdb',  # ID của DAG cần trigger
    #     conf={
    #         'logical_date': '{{ ds }}',  # Truyền logical_date nếu cần
    #     },
    #     wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    # )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho website
with DAG(
    dag_id='transform_website_user_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Website',
    #schedule='0 3 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'website'],
    params={
        'channel': 'website',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'users'
    }
) as website_dag:

    #  gọi hàm get_yesterday_file_paths để lấy đường dẫn file

    get_file_path = PythonOperator(
        task_id='get_yesterday_file_paths',
        python_callable=get_yesterday_file_paths,
        ##inlets=[WEBSITE_USER_DATASET]
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
        #outlets=[WEBSITE_USER_PARQUET],
    )

    # Task cuối cùng: Trigger DAG clean_user_data_website_with_duckdb
    # trigger_clean_dag = TriggerDagRunOperator(
    #     task_id='trigger_clean_user_data_website',
    #     trigger_dag_id='clean_user_data_website_with_duckdb',  # ID của DAG cần trigger
    #     conf={
    #         'logical_date': '{{ ds }}',  # Truyền logical_date nếu cần
    #     },
    #     wait_for_completion=False,  # Chờ DAG được trigger hoàn thành
    # )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet