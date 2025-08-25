"""
Mô-đun tạo dữ liệu mẫu cho các bảng liên quan đến kho hàng.
Mô phỏng đầy đủ quy trình nghiệp vụ kho hàng: nhập kho, xuất kho, kiểm kho, điều chuyển hàng.
"""

import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
# import pandas as pd
# import numpy as np
# from sqlalchemy.engine import Engine
from sqlalchemy import text
from decimal import Decimal
import faker

# from services.db_config import get_db_connection
# from services.db_config import get_db_connection

# Fix import for when running the script directly
from .db_config import get_db_connection

# Khởi tạo faker để tạo dữ liệu giả
fake = faker.Faker('vi_VN')

# Các trạng thái của hàng hóa trong kho
INVENTORY_STATUS = ["Có sẵn", "Đang đặt hàng", "Hết hàng", "Đang chuyển đi"]

# Các trạng thái của hàng hóa trong kho đã được khai báo ở trên

def get_warehouses(conn) -> List[Dict[str, Any]]:
    """Lấy danh sách tất cả kho hàng từ DB."""
    query = """
    SELECT id, name, geo_location_id, capacity_sqm
    FROM warehouse
    WHERE is_active = true
    """
    result = conn.execute(text(query))
    return [dict(row._mapping) for row in result]

def get_products(conn) -> List[Dict[str, Any]]:
    """Lấy danh sách tất cả sản phẩm từ DB."""
    query = """
    SELECT id, name, price, product_sku
    FROM product
    WHERE is_active = true
    """
    result = conn.execute(text(query))
    return [dict(row._mapping) for row in result]

def get_existing_inventory(conn) -> List[Dict[str, Any]]:
    """Lấy danh sách tồn kho hiện tại từ DB."""
    query = """
    SELECT id, product_id, warehouse_id, quantity, safety_stock, reorder_level, unit_cost
    FROM inventory
    WHERE is_active = true
    """
    result = conn.execute(text(query))
    return [dict(row._mapping) for row in result]

def get_existing_warehouses(conn) -> List[Dict[str, Any]]:
    """
    Lấy dữ liệu kho hàng có sẵn từ database.
    """
    # Truy vấn tất cả kho hàng đang hoạt động
    query = """
    SELECT 
        id, name, address, geo_location_id, capacity_sqm, 
        manager, contact_phone, is_active, created_at, updated_at
    FROM warehouse
    WHERE is_active = true
    """
    
    result = conn.execute(text(query))
    warehouses = [dict(row._mapping) for row in result]
    
    if not warehouses:
        print("Cảnh báo: Không tìm thấy kho hàng nào trong database.")
        return []
    
    print(f"Đã tìm thấy {len(warehouses)} kho hàng có sẵn trong database")
    return warehouses

def create_inventory(conn, warehouses: List[Dict[str, Any]], products: List[Dict[str, Any]], 
                     inventory_density: float = 0.7) -> List[Dict[str, Any]]:
    """
    Tạo dữ liệu tồn kho cho các kho và sản phẩm.
    inventory_density: tỷ lệ sản phẩm có trong mỗi kho (0-1)
    """
    inventory_items = []
    existing_inventory = get_existing_inventory(conn)
    
    # Tạo từ điển để lưu trữ inventory hiện có theo cặp (product_id, warehouse_id)
    existing_inv_map = {(inv['product_id'], inv['warehouse_id']): inv for inv in existing_inventory}
    
    # Lấy ID inventory cao nhất hiện tại
    max_id_query = "SELECT COALESCE(MAX(id), 0) as max_id FROM inventory"
    max_id_result = conn.execute(text(max_id_query))
    max_id = max_id_result.scalar() or 0
    
    inventory_id = max_id + 1
    
    # Với mỗi kho
    for warehouse in warehouses:
        # Chọn ngẫu nhiên một số sản phẩm dựa trên mật độ tồn kho
        num_products = int(len(products) * inventory_density)
        selected_products = random.sample(products, num_products)
        
        # Với mỗi sản phẩm được chọn
        for product in selected_products:
            # Kiểm tra xem đã có trong kho chưa
            key = (product['id'], warehouse['id'])
            if key in existing_inv_map:
                continue
                
            # Tính toán các giá trị tồn kho
            quantity = random.randint(5, 100)
            safety_stock = int(quantity * 0.2)  # 20% của số lượng hiện tại
            reorder_level = safety_stock + random.randint(5, 20)
            
            # Chi phí đơn vị = 70-90% giá bán
            unit_cost = Decimal(str(round(float(product['price']) * random.uniform(0.7, 0.9), 2)))
            
            # Tạo bản ghi tồn kho
            inventory_item = {
                'id': inventory_id,
                'product_id': product['id'],
                'warehouse_id': warehouse['id'],
                'quantity': quantity,
                'safety_stock': safety_stock,
                'reorder_level': reorder_level,
                'last_counted_at': datetime.now() - timedelta(days=random.randint(1, 30)),
                'unit_cost': unit_cost,
                'is_active': True,
                'created_at': warehouse['created_at'] + timedelta(days=random.randint(1, 10)),
                'updated_at': datetime.now() - timedelta(days=random.randint(0, 5))
            }
            
            # Thêm vào DB
            insert_query = """
            INSERT INTO inventory (
                id, product_id, warehouse_id, quantity, safety_stock,
                reorder_level, last_counted_at, unit_cost, is_active, created_at, updated_at
            ) VALUES (
                :id, :product_id, :warehouse_id, :quantity, :safety_stock,
                :reorder_level, :last_counted_at, :unit_cost, :is_active, :created_at, :updated_at
            )
            ON CONFLICT (product_id, warehouse_id) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                safety_stock = EXCLUDED.safety_stock,
                reorder_level = EXCLUDED.reorder_level,
                last_counted_at = EXCLUDED.last_counted_at,
                unit_cost = EXCLUDED.unit_cost,
                updated_at = EXCLUDED.updated_at
            """
            conn.execute(text(insert_query), inventory_item)
            
            inventory_items.append(inventory_item)
            inventory_id += 1
    
    conn.commit()
    print(f"Đã tạo {len(inventory_items)} bản ghi tồn kho")
    return inventory_items

# Function create_inventory_transactions has been removed



def generate_warehouse_data(inventory_density: float = 0.5):
    """
    Hàm chính để tạo toàn bộ dữ liệu cho hệ thống kho hàng.
    """
    print("Bắt đầu tạo dữ liệu kho hàng...")
    
    # Kết nối đến database
    engine = get_db_connection()
    
    with engine.connect() as conn:
        # Lấy kho hàng có sẵn từ database
        warehouses = get_existing_warehouses(conn)
        
        if not warehouses:
            print("Không thể tiếp tục: Không có kho hàng nào trong database.")
            return {
                "warehouses": 0,
                "inventory_items": 0
            }
        
        # Lấy danh sách sản phẩm
        products = get_products(conn)
        
        # Tạo dữ liệu tồn kho
        inventory_items = create_inventory(conn, warehouses, products, inventory_density)
    
    print("Hoàn thành tạo dữ liệu kho hàng!")
    
    return {
        "warehouses": len(warehouses),
        "inventory_items": len(inventory_items)
    }

if __name__ == "__main__":
    # Có thể chạy trực tiếp file này để tạo dữ liệu
    results = generate_warehouse_data(
        inventory_density=0.7,       # Tỷ lệ sản phẩm có trong mỗi kho
    )
    print("Kết quả:", results)
