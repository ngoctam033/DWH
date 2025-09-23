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
        raise ValueError("logical_date không tồn tại trong context.")
    logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")
    
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
        error_msg = f"Không tìm thấy file nào cho {channel}/{data_model} ngày {logical_date.strftime('%Y-%m-%d')} trong thư mục {object_dir}"
        logging.error(error_msg)
        # Đánh dấu task là failed
        raise AirflowFailException(error_msg)
    
    logging.info(f"Danh sách file ngày hôm qua ({logical_date}): {result}")

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
            downloaded_files[object_name] = file_content
            
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
    Chuyển đổi dữ liệu JSON order_items_list thành dạng bảng.
    
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

    order_items_list = []
    # if order_channel == 'lazada':
    logging.info("Bắt đầu parse dữ liệu Lazada")

    # Duyệt qua từng file JSON đã download
    for file_key, file_data in downloaded_files.items():
        try:
            # Kiểm tra nếu file_data là list
            if isinstance(file_data, list):
                # Append trực tiếp vào order_items_list
                order_items_list.extend(file_data)
                logging.info(f"Đã thêm {len(file_data)} dòng từ file '{file_key}' vào order_items_list")
            elif isinstance(file_data, str):
                # Nếu file_data là chuỗi JSON, parse thành list
                data = json.loads(file_data)
                if isinstance(data, list):
                    order_items_list.extend(data)
                    logging.info(f"Đã parse và thêm {len(data)} dòng từ file '{file_key}' vào order_items_list")
                else:
                    logging.warning(f"Dữ liệu trong file '{file_key}' không phải là list sau khi parse")
            else:
                logging.warning(f"File '{file_key}' không phải là list hoặc chuỗi JSON, bỏ qua.")
        except Exception as e:
            # Bắt các ngoại lệ và đánh dấu task failed với thông tin chi tiết
            error_message = f"Lỗi khi xử lý file '{file_key}': {str(e)}"
            logging.error(error_message)
            raise AirflowFailException(error_message)
    
    # 5. Tổng kết kết quả
    if order_items_list:
        logging.info(f"Tổng số order items từ tất cả các file: {len(order_items_list)}")
        
        # Log một số thông tin mẫu về dữ liệu
        if len(order_items_list) > 0:
            sample_keys = list(order_items_list[0].keys())
            logging.info(f"Các trường dữ liệu có trong order items: {', '.join(sample_keys[:10])}" + 
                        (f"... và {len(sample_keys)-10} trường khác" if len(sample_keys) > 10 else ""))
    else:
        logging.warning("Không tìm thấy order items nào trong tất cả các file")

    # Lưu kết quả vào XCom
    context['ti'].xcom_push(key='parsed_tables', value=order_items_list)  

def convert_to_parquet_and_save(**context):
    """
    Chuyển đổi order_items_list thành DataFrame và lưu dưới dạng Parquet vào MinIO.
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
    # Lấy order_items_list từ task parse_json_to_table
    order_items_list = context['ti'].xcom_pull(task_ids='parse_json_to_table', key='parsed_tables')
    
    if not order_items_list:
        error_msg = "Không tìm thấy order_items_list từ task trước"
        logging.warning(error_msg)  # Đổi từ logging.error sang logging.warning
        # Không raise lỗi nữa, chỉ log và cho qua
        # raise ValueError(error_msg)
    
    conf = context['dag_run'].conf

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
        # Chuyển order_items_list thành DataFrame
        df = pd.DataFrame(order_items_list)
        
        # Thêm cột order_channel từ params
        order_channel = params.get('channel')
        df['order_channel'] = order_channel

        # Log thông tin DataFrame
        logging.info(f"Đã chuyển đổi order_items_list thành DataFrame:")
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
        logging.info(f"Đã lưu thành công file Parquet: s3://{bucket_name}/{object_name}")
        
        # Lưu đường dẫn vào XCom
        context['ti'].xcom_push(key='parquet_file_path', value=object_name)
        
        return object_name
        
    except Exception as e:
        logging.error(f"Lỗi khi chuyển đổi và lưu file Parquet: {e}")
        raise

# DAG cho Lazada
with DAG(
    dag_id='transform_lazada_order_items_to_parquet',
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
        'data_model': 'order_items'
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

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Shopee
with DAG(
    dag_id='transform_shopee_order_items_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Shopee',
    #schedule='0 3 * * *',         # Lịch chạy: 03:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'shopee'],
    params={
        'channel': 'shopee',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'order_items'
    }
) as shopee_dag:

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
        #outlets=[SHOPEE_USER_PARQUET],
    )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Tiki
with DAG(
    dag_id='transform_tiki_order_items_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiki',
    #schedule='0 3 * * *',         # Lịch chạy: 03:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'tiki'],
    params={
        'channel': 'tiki',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'order_items'
    }
) as tiki_dag:

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
        #outlets=[TIKI_USER_PARQUET],
    )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Tiktok
with DAG(
    dag_id='transform_tiktok_order_items_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Tiktok',
    #schedule='0 3 * * *',         # Lịch chạy: 03:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'tiktok'],
    params={
        'channel': 'tiktok',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'order_items'
    }
) as tiktok_dag:

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
        #outlets=[TIKTOK_USER_PARQUET],
    )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet

# DAG cho Website
with DAG(
    dag_id='transform_website_order_items_to_parquet',
    default_args=default_args,
    description='Parse JSON, chuyển sang bảng và lưu dạng Parquet cho dữ liệu Website',
    #schedule='0 3 * * *',         # Lịch chạy: 03:00 mỗi ngày (crontab expression)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'parquet', 'website'],
    params={
        'channel': 'website',
        'layer_inlets': 'raw',
        'layer_outlet': 'staging',
        'file_type': 'json',
        'data_model': 'order_items'
    }
) as website_dag:

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
        #outlets=[WEBSITE_USER_PARQUET],
    )

    # Định nghĩa luồng thực thi
    get_file_path >> download_file >> parse_json >> convert_to_parquet