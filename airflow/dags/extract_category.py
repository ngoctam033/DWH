from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
from config.db_config import get_db_connection
from config.minio_config import DEFAULT_BUCKET, get_minio_client
import io
import pyarrow as pa
import pyarrow.parquet as pq
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

# ORDER_CHANNEL_CLEANED_PARQUET = Dataset("s3://minio/cleaned/erp/order_channel/year={{logical_date.year}}/month={{logical_date.strftime('%m')}}/day={{logical_date.strftime('%d')}}/{{ds}}_data.parquet")

def extract_sub_category(**context):
    """Extract data from sub_category table and push to XCom"""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            # Thực thi query trực tiếp với SQLAlchemy
            query = "SELECT * FROM sub_category"
            result = conn.execute(query)
            
            # Chuyển kết quả thành list of dicts
            data = [dict(row) for row in result]
            
            # Chuyển datetime thành string
            for row in data:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
            
            # Push data vào XCom
            context['ti'].xcom_push(key='sub_category_df', value=data)
            logging.info(f"Extracted {len(data)} rows from sub_category table")

    except Exception as e:
        logging.error(f"Error extracting data from sub_category: {str(e)}")
        raise

def save_sub_category(**context):
    """Save data to parquet file in MinIO"""
    try:
        # Lấy dữ liệu từ XCom
        data = context['ti'].xcom_pull(key='sub_category_df', task_ids='extract_sub_category')

        # Chuyển data thành PyArrow Table trực tiếp
        table = pa.Table.from_pylist(data)
        
        # Ghi vào buffer
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        # Lấy thông tin từ context
        logical_date = context['logical_date']
        params = context['params']
        bucket_name = params.get('bucket_name', DEFAULT_BUCKET)
        
        # Tạo object name
        object_name = f"layer=cleaned/data_model=sub_category/channel=erp/year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}/{logical_date.strftime('%Y-%m-%d')}_data.parquet"

        # Upload lên MinIO
        client = get_minio_client()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        logging.info(f"Đã lưu dữ liệu lên MinIO: s3://{bucket_name}/{object_name}")
            
    except Exception as e:
        logging.error(f"Error saving data to MinIO: {str(e)}")
        raise
with DAG(
    dag_id='extract_sub_category',
    default_args=default_args,
    #schedule=None,
    catchup=False,
    tags=['extract', 'sub_category', 'postgresql', 'minio'],
    description='Extract toàn bộ bảng sub_category từ PostgreSQL',
    params={

    }
) as dag:
    extract_task = PythonOperator(
        task_id='extract_sub_category',
        python_callable=extract_sub_category,
    )

    save_task = PythonOperator(
        task_id='save_sub_category',
        python_callable=save_sub_category,
    )

    extract_task >> save_task