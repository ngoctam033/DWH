# from airflow import DAG
# from airflow.datasets import Dataset
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import duckdb
# import logging
# from io import BytesIO
# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq

# # Import config MinIO
# from config.minio_config import (
#     MINIO_ENDPOINT, MINIO_PORT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
#     DEFAULT_BUCKET,
#     get_minio_client, get_object_name
# )

# default_args = {
#     'owner': 'airflow',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2),
# }

# def clean_order_data(**context):
#     """
#     Đọc dữ liệu Parquet từ MinIO, làm sạch bằng DuckDB, trả về DataFrame sạch.
#     """
#     conf = context['dag_run'].conf
#     params = context.get('params', {})
#     channel = params.get('channel')
#     data_model = params.get('data_model')
#     bucket_name = params.get('bucket_name', 'datawarehouse')
#     layer_in = params.get('layer_in')
#     logical_date = conf.get('logical_date', context.get('logical_date'))
    
#     logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")

#     parquet_in = get_object_name(layer_in, channel, data_model, logical_date, file_type='parquet')
#     folder_prefix = parquet_in.rsplit('/', 1)[0]  # Lấy folder chứa file parquet

#     logging.info(f"Bắt đầu lọc trùng dữ liệu trong folder: s3://{bucket_name}/{folder_prefix}")

#     # Đếm số file parquet trong folder
#     client = get_minio_client()
#     objects = client.list_objects(bucket_name, prefix=folder_prefix + '/', recursive=True)
#     parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
#     logging.info(f"Số lượng file parquet cần lọc trùng: {len(parquet_files)}")
#     if parquet_files:
#         logging.info(f"Danh sách file: {parquet_files}")

#     conn = duckdb.connect(database=':memory:')
#     conn.install_extension('httpfs')
#     conn.load_extension('httpfs')
#     conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}:{MINIO_PORT}';")
#     conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
#     conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
#     conn.execute(f"SET s3_url_style='path';")
#     conn.execute(f"SET s3_use_ssl={'true' if MINIO_SECURE else 'false'};")

#     query = f"""
#     SELECT *
#     FROM read_parquet('s3://{bucket_name}/{parquet_in}')
#     """
#     logging.info(f"Chạy query lọc trùng dữ liệu: {query}")
#     df_clean = conn.execute(query).fetchdf()
#     logging.info(f"Số dòng sau khi lọc trùng: {len(df_clean)}")

#     # Đẩy DataFrame đã lọc trùng lên XCom (dùng to_dict để serialize)
#     context['ti'].xcom_push(key='cleaned_order_data', value=df_clean.to_dict(orient='records'))
#     logging.info(f"Đã đẩy {len(df_clean)} bản ghi đã làm sạch lên XCom.")

# def save_cleaned_order_data(**context):
#     """
#     Nhận DataFrame sạch từ XCom, lưu lại file parquet vào MinIO.
#     """
#     conf = context['dag_run'].conf
#     params = context.get('params', {})
#     channel = params.get('channel')
#     data_model = params.get('data_model')
#     bucket_name = params.get('bucket_name', 'datawarehouse')
#     layer_out = params.get('layer_out', 'cleaned')
#     logical_date = conf.get('logical_date', context.get('logical_date'))
#     logging.info(f"Ngày logical date của DAG run này: {logical_date} (type: {type(logical_date)})")

#     parquet_out = get_object_name(layer_out, channel, data_model, logical_date, file_type='parquet')

#     # Lấy dữ liệu sạch từ XCom
#     cleaned_data = context['ti'].xcom_pull(task_ids='clean_order_data', key='cleaned_order_data')
#     if not cleaned_data:
#         raise ValueError("Không tìm thấy dữ liệu đã làm sạch từ XCom")

#     df_clean = pa.Table.from_pandas(pd.DataFrame(cleaned_data))
    
#     buffer = BytesIO()
#     pq.write_table(df_clean, buffer)
#     buffer.seek(0)

#     client = get_minio_client()
#     client.put_object(
#         bucket_name=bucket_name,
#         object_name=parquet_out,
#         data=buffer,
#         length=buffer.getbuffer().nbytes,
#         content_type='application/octet-stream'
#     )
#     logging.info(f"Đã lưu dữ liệu đã làm sạch lên MinIO: s3://{bucket_name}/{parquet_out}")
#     context['ti'].xcom_push(key='cleaned_parquet_path', value=parquet_out)
#     return parquet_out

# with DAG(
#     dag_id='clean_order_items_data_lazada_with_duckdb',
#     default_args=default_args,
#     description='Làm sạch dữ liệu order_items trên MinIO bằng DuckDB và lưu lại vào MinIO',
#     #schedule='0 4 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=True,
#     tags=['clean', 'duckdb', 'minio', 'lazada'],
#     params={
#         'channel': 'lazada',
#         'data_model': 'order_items',
#         'bucket_name': DEFAULT_BUCKET,
#         'layer_in': 'staging',
#         'layer_out': 'cleaned'
#     }
# ) as dag:

#     clean_data = PythonOperator(
#         task_id='clean_order_data',
#         python_callable=clean_order_data,
#         #inlets = [LAZADA_ORDER_PARQUET]
#     )

#     save_data = PythonOperator(
#         task_id='save_cleaned_order_data',
#         python_callable=save_cleaned_order_data,
#         # outlets = [LAZADA_ORDER_CLEANED_PARQUET]
#     )

#     clean_data >> save_data

# with DAG(
#     dag_id='clean_order_items_data_shopee_with_duckdb',
#     default_args=default_args,
#     description='Làm sạch dữ liệu order_items trên MinIO bằng DuckDB và lưu lại vào MinIO',
#     #schedule='0 4 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=True,
#     tags=['clean', 'duckdb', 'minio', 'shopee'],
#     params={
#         'channel': 'shopee',
#         'data_model': 'order_items',
#         'bucket_name': DEFAULT_BUCKET,
#         'layer_in': 'staging',
#         'layer_out': 'cleaned'
#     }
# ) as dag:

#     clean_data = PythonOperator(
#         task_id='clean_order_data',
#         python_callable=clean_order_data,
#         #inlets = [LAZADA_ORDER_PARQUET]
#     )

#     save_data = PythonOperator(
#         task_id='save_cleaned_order_data',
#         python_callable=save_cleaned_order_data,
#         # outlets = [LAZADA_ORDER_CLEANED_PARQUET]
#     )

#     clean_data >> save_data

# with DAG(
#     dag_id='clean_order_items_data_tiki_with_duckdb',
#     default_args=default_args,
#     description='Làm sạch dữ liệu order_items trên MinIO bằng DuckDB và lưu lại vào MinIO',
#     #schedule='0 4 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=True,
#     tags=['clean', 'duckdb', 'minio', 'tiki'],
#     params={
#         'channel': 'tiki',
#         'data_model': 'order_items',
#         'bucket_name': DEFAULT_BUCKET,
#         'layer_in': 'staging',
#         'layer_out': 'cleaned'
#     }
# ) as dag:

#     clean_data = PythonOperator(
#         task_id='clean_order_data',
#         python_callable=clean_order_data,
#         #inlets = [LAZADA_ORDER_PARQUET]
#     )

#     save_data = PythonOperator(
#         task_id='save_cleaned_order_data',
#         python_callable=save_cleaned_order_data,
#         # outlets = [LAZADA_ORDER_CLEANED_PARQUET]
#     )

#     clean_data >> save_data

# with DAG(
#     dag_id='clean_order_items_data_tiki_with_duckdb',
#     default_args=default_args,
#     description='Làm sạch dữ liệu order_items trên MinIO bằng DuckDB và lưu lại vào MinIO',
#     #schedule='0 4 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=True,
#     tags=['clean', 'duckdb', 'minio', 'tiki'],
#     params={
#         'channel': 'tiki',
#         'data_model': 'order_items',
#         'bucket_name': DEFAULT_BUCKET,
#         'layer_in': 'staging',
#         'layer_out': 'cleaned'
#     }
# ) as dag:

#     clean_data = PythonOperator(
#         task_id='clean_order_data',
#         python_callable=clean_order_data,
#         #inlets = [LAZADA_ORDER_PARQUET]
#     )

#     save_data = PythonOperator(
#         task_id='save_cleaned_order_data',
#         python_callable=save_cleaned_order_data,
#         # outlets = [LAZADA_ORDER_CLEANED_PARQUET]
#     )

#     clean_data >> save_data

# with DAG(
#     dag_id='clean_order_items_data_tiktok_with_duckdb',
#     default_args=default_args,
#     description='Làm sạch dữ liệu order_items trên MinIO bằng DuckDB và lưu lại vào MinIO',
#     #schedule='0 4 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=True,
#     tags=['clean', 'duckdb', 'minio', 'tiktok'],
#     params={
#         'channel': 'tiktok',
#         'data_model': 'order_items',
#         'bucket_name': DEFAULT_BUCKET,
#         'layer_in': 'staging',
#         'layer_out': 'cleaned'
#     }
# ) as dag:

#     clean_data = PythonOperator(
#         task_id='clean_order_data',
#         python_callable=clean_order_data,
#         #inlets = [LAZADA_ORDER_PARQUET]
#     )

#     save_data = PythonOperator(
#         task_id='save_cleaned_order_data',
#         python_callable=save_cleaned_order_data,
#         # outlets = [LAZADA_ORDER_CLEANED_PARQUET]
#     )

#     clean_data >> save_data


# with DAG(
#     dag_id='clean_order_items_data_website_with_duckdb',
#     default_args=default_args,
#     description='Làm sạch dữ liệu order_items trên MinIO bằng DuckDB và lưu lại vào MinIO',
#     #schedule='0 4 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=True,
#     tags=['clean', 'duckdb', 'minio', 'website'],
#     params={
#         'channel': 'website',
#         'data_model': 'order_items',
#         'bucket_name': DEFAULT_BUCKET,
#         'layer_in': 'staging',
#         'layer_out': 'cleaned'
#     }
# ) as dag:

#     clean_data = PythonOperator(
#         task_id='clean_order_data',
#         python_callable=clean_order_data,
#         #inlets = [LAZADA_ORDER_PARQUET]
#     )

#     save_data = PythonOperator(
#         task_id='save_cleaned_order_data',
#         python_callable=save_cleaned_order_data,
#         # outlets = [LAZADA_ORDER_CLEANED_PARQUET]
#     )

#     clean_data >> save_data