import duckdb
import os
from config import DUCKDB_FILE, BASE_DIR

from minio import Minio
from minio.error import S3Error

# Kết nối tới MinIO
client = Minio(
    "minio1:9000",  # hoặc minio1:9000 nếu trong cluster
    access_key="admin",
    secret_key="admin123",
    secure=False
)

bucket_name = "datawarehouse"

# Tạo bucket nếu chưa có
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    # nếu tạo thành công
    print(f"Bucket '{bucket_name}' đã được tạo thành công.")

# Cấu trúc thư mục của bạn
folder_structure = {
    "raw": {
        "shopee": ["orders", "orders_items", "product_review", "discount"],
        "lazada": ["orders", "products"],
        "tiki": ["orders", "products"],
        "website": ["orders", "products"],
        "erp": ["product"]
    },
    "staging": {},
    "cleaned": {},
    "aggregated": {}
}