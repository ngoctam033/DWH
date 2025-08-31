import random
import pandas as pd
import uuid
from faker import Faker
from sqlalchemy import create_engine
from datetime import datetime
from .db_config import get_database_url

# Khởi tạo Faker
fake = Faker()

# Hàm kết nối đến database
def get_db_connection():
    DATABASE_URL = get_database_url()
    engine = create_engine(DATABASE_URL)
    return engine
    
def is_customer_code_exists(engine, customer_code):
    """
    Kiểm tra xem customer_code đã tồn tại trong database chưa
    
    Parameters:
    -----------
    engine : sqlalchemy.engine.Engine
        Engine kết nối đến database
    customer_code : str
        Mã khách hàng cần kiểm tra
        
    Returns:
    --------
    bool
        True nếu mã đã tồn tại, False nếu chưa tồn tại
    """
    try:
        # with engine.connect() as conn:
        #     query = f"SELECT COUNT(*) FROM customers WHERE customer_code = '{customer_code}'"
        #     result = conn.execute(query)
        #     count = result.scalar()
        #     return count > 0
        return True
    except Exception as e:
        print(f"Lỗi khi kiểm tra customer_code: {e}")
        return False

def save_data_to_db(data_df, table_name, engine):
    """
    Lưu DataFrame vào database
    
    Parameters:
    -----------
    data_df : pandas.DataFrame
        DataFrame chứa dữ liệu cần lưu
    table_name : str
        Tên bảng trong database
    engine : sqlalchemy.engine.Engine
        Engine kết nối đến database
    
    Returns:
    --------
    bool
        True nếu lưu thành công, False nếu có lỗi
    """
    try:
        # Nếu bảng là customers và có thể xảy ra trùng lặp customer_code
        if table_name == 'customers' and 'customer_code' in data_df.columns:
            # Lưu từng bản ghi một và xử lý trường hợp trùng lặp
            successful_records = 0
            failed_records = 0
            for _, row in data_df.iterrows():
                try:
                    # Kiểm tra xem customer_code đã tồn tại chưa
                    if is_customer_code_exists(engine, row['customer_code']):
                        # Nếu mã đã tồn tại, tạo mã mới bằng cách thêm thêm UUID mới
                        channel_prefix = row['customer_code'].split('-')[0]
                        new_uuid = str(uuid.uuid4()).replace('-', '')[:8]
                        new_code = f"{channel_prefix}-{new_uuid}"
                        row['customer_code'] = new_code
                    
                    # Lưu bản ghi
                    pd.DataFrame([row]).to_sql(table_name, engine, if_exists='append', index=False)
                    successful_records += 1
                except Exception as e:
                    print(f"Lỗi khi lưu bản ghi {row['customer_code']}: {e}")
                    failed_records += 1
                    
            print(f"Đã lưu {successful_records}/{len(data_df)} bản ghi vào bảng '{table_name}', {failed_records} bản ghi thất bại")
            return successful_records > 0
        else:
            # Các bảng khác vẫn xử lý bình thường
            data_df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Đã lưu {len(data_df)} bản ghi vào bảng '{table_name}'")
            return True
    except Exception as e:
        print(f"Lỗi khi lưu dữ liệu vào bảng '{table_name}': {e}")
        return False

def generate_customers(num_customers=100, created_at=None, order_channel=None):
    """
    Sinh dữ liệu cho bảng customers
    
    Parameters:
    -----------
    num_customers : int, optional
        Số lượng khách hàng cần tạo, mặc định là 100
    created_at : datetime, optional
        Ngày tạo cho tất cả các bản ghi, nếu None sẽ sử dụng ngày tạo ngẫu nhiên
    order_channel : str, optional
        Kênh bán hàng cho tất cả khách hàng (shopee, lazada, tiki, website, facebook).
        Nếu None, sẽ chọn ngẫu nhiên cho mỗi khách hàng
        
    Returns:
    --------
    dict
        Dữ liệu khách hàng được tạo dưới dạng JSON
    """
    print(f"Bắt đầu tạo {num_customers} khách hàng...")

    # Kết nối đến database
    engine = get_db_connection()

    # Lấy danh sách order_channel_id và geo_location_id từ database
    with engine.connect() as conn:
        # Lấy thông tin kênh đặt hàng với cả ID và tên
        order_channel_df = pd.read_sql("SELECT id, name FROM order_channel", conn)
        order_channel_ids = order_channel_df['id'].tolist()
        geo_location_ids = pd.read_sql("SELECT ward_code FROM geo_location", conn)['ward_code'].tolist()
    
    # Tạo mapping từ tên kênh sang ID
    channel_name_to_id = {
        row['name'].lower(): row['id'] for _, row in order_channel_df.iterrows()
    }
    
    # Tạo danh sách khách hàng
    customers = []

    # Tạo mapping từ order_channel_id sang tên viết tắt
    order_channels = {
        1: "SHO",  # Shopee
        2: "LAZ",  # Lazada
        3: "TIK",  # Tiktok
        4: "WEB",  # Website
        5: "FB"    # Facebook
    }

    for i in range(1, num_customers + 1):
        # Xác định order_channel_id dựa trên tham số đầu vào hoặc chọn ngẫu nhiên
        if order_channel:
            # Nếu có chỉ định kênh bán hàng, sử dụng kênh đó
            # Chuyển sang chữ thường để so sánh không phân biệt hoa thường
            order_channel_lower = order_channel.lower()
            # Kiểm tra xem kênh có tồn tại trong database không
            if order_channel_lower in channel_name_to_id:
                order_channel_id = channel_name_to_id[order_channel_lower]
            else:
                # Nếu không tìm thấy, sử dụng kênh ngẫu nhiên
                print(f"Cảnh báo: Kênh '{order_channel}' không tồn tại, sử dụng kênh ngẫu nhiên")
                order_channel_id = random.choice(order_channel_ids)
        else:
            # Nếu không chỉ định, chọn ngẫu nhiên
            order_channel_id = random.choice(order_channel_ids)
            
        # Chọn ngẫu nhiên geo_location_id
        geo_location_id = random.choice(geo_location_ids)

        # Lấy tiền tố từ order_channels nếu có, nếu không thì dùng "OTH"
        channel_prefix = order_channels.get(order_channel_id, "OTH")
        
        # Tạo UUID ngắn và kết hợp với tiền tố để tạo customer_code
        short_uuid = str(uuid.uuid4()).replace('-', '')[:8]  # Lấy 8 ký tự đầu của UUID, bỏ dấu gạch ngang
        customer_code = f"{channel_prefix}-{short_uuid}"

        # Tạo bản ghi khách hàng
        customer = {
            'customer_code': customer_code,
            'geo_location_id': geo_location_id,
            'order_channel_id': order_channel_id,
            'created_at': (created_at if created_at else fake.date_time_between(start_date="-2y", end_date="now")).strftime("%Y-%m-%d %H:%M:%S"),
            'is_active': random.choice([True, False])
        }
        customers.append(customer)

    # Chuyển đổi danh sách thành DataFrame
    customers_df = pd.DataFrame(customers)

    # Lưu vào database
    save_result = save_data_to_db(customers_df, 'customers', engine)

    if not save_result:
        print("Không lưu được dữ liệu vào database do lỗi kết nối hoặc cấu hình.")
        customers_json = []

    # Chuyển đổi DataFrame thành dạng JSON cơ bản
    customers_basic = customers_df.to_dict(orient='records')
    
    # Thay đổi cấu trúc JSON trả về dựa vào order_channel
    if order_channel:
        order_channel_lower = order_channel.lower()
        
        if order_channel_lower == "shopee":
            # Cấu trúc JSON kiểu Shopee
            return {
                "success": True,
                "response": {
                    "user_list": customers_basic,
                    "total_count": len(customers_basic),
                    "metadata": {
                        "created_at": datetime.now().isoformat(),
                        "source": "shopee-user-api",
                        "version": "v2.1.3"
                    }
                },
                "error": None
            }
        
        elif order_channel_lower == "tiktok":
            # Cấu trúc JSON kiểu TikTok Shop
            return {
                "code": 0,
                "message": "success", 
                "request_id": f"tiktok-{hash(datetime.now().isoformat()) % 10000000}",
                "data": {
                    "user_list": customers_basic,
                    "total": len(customers_basic),
                    "more": False
                }
            }
        elif order_channel_lower == "lazada":
            # Cấu trúc JSON kiểu Lazada
            return {
                "code": "0",  # Lazada thường dùng string cho mã trạng thái
                "request_id": f"lazada-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "data": {
                    "users": customers_basic,
                    "count": len(customers_basic),
                    "has_next": False,
                    "pagination": {
                        "current": 1,
                        "pageSize": 100,
                        "total": len(customers_basic)
                    }
                },
                "created_users": len(customers_basic)
            }
            
        elif order_channel_lower == "website":
            # Cấu trúc JSON kiểu Website (thường đơn giản hơn)
            return {
                "status": 200,
                "timestamp": datetime.now().isoformat(),
                "data": customers_basic,
                "metadata": {
                    "count": len(customers_basic),
                    "generated_at": datetime.now().isoformat(),
                    "version": "3.0.1"
                }
            }
            
        elif order_channel_lower == "tiki":
            # Cấu trúc JSON kiểu Tiki
            return {
                "status": "success",
                "timestamp": datetime.now().timestamp(),
                "data": {
                    "users": {
                        "data": customers_basic,
                        "summary": {
                            "total_count": len(customers_basic)
                        }
                    },
                    "paging": {
                        "cursors": {
                            "before": f"QVFIU{hash(datetime.now().isoformat()) % 1000000}",
                            "after": f"QVFIU{hash(datetime.now().isoformat() + 'after') % 1000000}"
                        },
                        "next": None
                    }
                },
                "app_id": "1234567890123456"
            }
    
    # Cấu trúc mặc định nếu không có order_channel hoặc không khớp với các kênh trên
    return {
        "api_version": "v1.0",
        "status": "ok",
        "result": {
            "users": customers_basic,
            "metadata": {
                "count": len(customers_basic),
                "generated_at": datetime.now().isoformat()
            }
        }
    }

if __name__ == "__main__":
    # Ví dụ 1: Tạo khách hàng với ngày tạo ngẫu nhiên và kênh bán hàng ngẫu nhiên
    customers_data = generate_customers(random.randint(1, 100))
    print(f"Đã tạo {len(customers_data)} khách hàng và nhận được dữ liệu JSON")
    
    # Ví dụ 2: Tạo khách hàng với ngày tạo cố định và sử dụng dữ liệu trả về
    # from datetime import datetime
    # fixed_date = datetime(2025, 8, 17)  # Ngày 17/8/2025
    # customers_data = generate_customers(random.randint(1, 100), created_at=fixed_date)
    # print(f"Mẫu dữ liệu khách hàng đầu tiên: {customers_data[0] if customers_data else None}")
    
    # Ví dụ 3: Tạo khách hàng với kênh bán hàng cụ thể
    # channel_customers = generate_customers(50, order_channel="Shopee")
    # print(f"Đã tạo {len(channel_customers)} khách hàng Shopee")
    
    # Ví dụ 4: Tạo khách hàng với cả ngày tạo và kênh bán hàng cụ thể
    # from datetime import datetime
    # fixed_date = datetime(2025, 8, 17)  # Ngày 17/8/2025
    # channel_customers = generate_customers(25, created_at=fixed_date, order_channel="Lazada")
    # print(f"Đã tạo {len(channel_customers)} khách hàng Lazada vào ngày {fixed_date.strftime('%d/%m/%Y')}")