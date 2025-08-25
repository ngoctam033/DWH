"""
Cấu hình kết nối đến PostgreSQL database
"""
from sqlalchemy import create_engine

# Thông tin kết nối database
DB_CONFIG = {
    'host': 'db',
    'port': '5432',  # Cổng ngoài của container Postgres được ánh xạ ra máy host
    'user': 'final_project',
    'password': 'final_project',
    'database': 'final_project'
}

# Tạo URL kết nối
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Sử dụng hàm này để lấy URL kết nối
def get_database_url():
    """Trả về URL kết nối database"""
    return DATABASE_URL

def get_db_connection():
    """
    Tạo kết nối đến PostgreSQL database
    
    Sử dụng cấu hình từ file db_config.py
    """
    try:
        DATABASE_URL = get_database_url()
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        print(f"Không thể kết nối đến database: {e}")
        raise
