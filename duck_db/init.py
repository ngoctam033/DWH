from config import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE, DEFAULT_BUCKET_NAME

from minio import Minio

# Kết nối tới MinIO
client = Minio(
    "minio1:9000",  # hoặc minio1:9000 nếu trong cluster
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

bucket_name = DEFAULT_BUCKET_NAME

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