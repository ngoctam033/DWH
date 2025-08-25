"""
Tạo dữ liệu cho kho hàng (warehouse) và tồn kho (inventory) cho cửa hàng công nghệ
trên các sàn thương mại điện tử
"""

import sys
import os
import random
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
import numpy as np
import uuid

# Thêm thư mục cha vào sys.path để import module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .db_config import get_db_connection

# Kết nối đến database
def connect_to_db():
    """Kết nối đến database và trả về connection"""
    try:
        engine = get_db_connection()
        conn = engine.connect()
        print("✅ Đã kết nối thành công đến database")
        return conn
    except Exception as e:
        print(f"❌ Lỗi khi kết nối đến database: {e}")
        sys.exit(1)

# Lấy danh sách địa điểm (geo_location) từ database
def get_geo_locations(conn):
    """Lấy danh sách các địa điểm từ bảng geo_location"""
    try:
        query = text("""
            SELECT ward_code, province_name, district_name, ward_name 
            FROM geo_location 
            WHERE is_active = TRUE
        """)
        result = conn.execute(query).fetchall()
        if not result:
            print("❌ Không tìm thấy dữ liệu địa điểm")
            return []
        print(f"✅ Đã lấy {len(result)} địa điểm từ database")
        return result
    except Exception as e:
        print(f"❌ Lỗi khi lấy địa điểm: {e}")
        return []

# Lấy danh sách sản phẩm từ database
def get_products(conn):
    """Lấy danh sách các sản phẩm từ bảng product"""
    try:
        query = text("""
            SELECT p.id, p.name, p.price, p.product_sku, 
                   b.name as brand_name, 
                   sc.name as subcategory_name,
                   c.name as category_name
            FROM product p
            JOIN brand b ON p.brand_id = b.id
            JOIN sub_category sc ON p.sub_category_id = sc.id
            JOIN category c ON sc.category_id = c.id
            WHERE p.is_active = TRUE
        """)
        result = conn.execute(query).fetchall()
        if not result:
            print("❌ Không tìm thấy sản phẩm nào trong database")
            sys.exit(1)
        print(f"✅ Đã lấy {len(result)} sản phẩm từ database")
        return result
    except Exception as e:
        print(f"❌ Lỗi khi lấy danh sách sản phẩm: {e}")
        sys.exit(1)

# Lấy thông tin kho hàng (warehouse) từ database
def get_warehouses(conn):
    """Lấy dữ liệu các kho hàng từ database"""
    try:
        # Truy vấn danh sách các kho hàng hiện có
        query = text("""
            SELECT id, name, capacity_sqm 
            FROM warehouse
            WHERE is_active = TRUE
        """)
        
        warehouses = conn.execute(query).fetchall()
        
        if not warehouses:
            print("❌ Không tìm thấy dữ liệu kho hàng trong database")
            return []
        
        print(f"✅ Đã lấy {len(warehouses)} kho hàng từ database")
        
        # In thông tin các kho hàng
        for warehouse in warehouses:
            warehouse_id = warehouse[0]
            name = warehouse[1]
            capacity = warehouse[2]
            print(f"📦 Kho: {name} | ID: {warehouse_id} | Diện tích: {capacity}m²")
        
        return warehouses
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu kho hàng: {e}")
        return []

# Tạo thông tin tồn kho (inventory) cho các sản phẩm
def create_inventory(conn, warehouses, products):
    """Tạo dữ liệu tồn kho cho các sản phẩm theo từng kho hàng"""
    try:
        inventory_data = []
        product_distribution = {}
        
        # Phân loại sản phẩm dựa trên danh mục để quyết định phân bổ vào kho nào
        for product in products:
            product_id = product[0]
            category = product[6].lower() if product[6] else ""
            
            # Phân loại sản phẩm theo danh mục
            if "laptop" in category or "máy tính" in category:
                product_type = "laptop"
            elif "điện thoại" in category or "mobile" in category:
                product_type = "phone"
            elif "tablet" in category or "máy tính bảng" in category:
                product_type = "tablet"
            elif "phụ kiện" in category or "accessory" in category:
                product_type = "accessory"
            else:
                product_type = "other"
                
            if product_type not in product_distribution:
                product_distribution[product_type] = []
            product_distribution[product_type].append(product_id)
        
        # Duyệt qua các kho hàng để tạo dữ liệu tồn kho
        for warehouse in warehouses:
            warehouse_id = warehouse[0]
            warehouse_name = warehouse[1]
            warehouse_capacity = warehouse[2]
            
            # Quyết định loại sản phẩm phân bổ nhiều vào kho nào dựa trên tên kho và vùng miền
            warehouse_name_lower = warehouse_name.lower()
            
            # Hệ số cho biết kho nào sẽ chứa nhiều loại sản phẩm nào
            weights = {
                'laptop': 1.0,
                'phone': 1.0,
                'tablet': 1.0,
                'accessory': 1.0,
                'other': 1.0
            }
            
            # Điều chỉnh hệ số dựa trên tên kho và vùng miền
            if "hà nội" in warehouse_name_lower:
                weights['laptop'] = 1.5
                weights['phone'] = 1.2
            elif "hồ chí minh" in warehouse_name_lower:
                weights['phone'] = 1.5
                weights['accessory'] = 1.2
            elif "đà nẵng" in warehouse_name_lower:
                weights['tablet'] = 1.5
                weights['other'] = 1.2
                
            # Điều chỉnh dựa trên kích thước kho
            if float(warehouse_capacity) > 1000:
                for key in weights.keys():
                    weights[key] *= 1.5
            
            # Số lượng các sản phẩm sẽ được đặt trong kho này
            product_count = int(min(len(products) * 0.8, float(warehouse_capacity) / 5))
            
            # Chọn sản phẩm cho kho này
            warehouse_products = []
            for product_type, product_ids in product_distribution.items():
                # Số lượng sản phẩm của loại này sẽ được đặt trong kho
                type_count = int(product_count * weights[product_type] / sum(weights.values()))
                # Chọn ngẫu nhiên sản phẩm từ loại này
                selected = random.sample(product_ids, min(type_count, len(product_ids)))
                warehouse_products.extend(selected)
            
            # Đảm bảo không vượt quá số lượng mong muốn
            if len(warehouse_products) > product_count:
                warehouse_products = random.sample(warehouse_products, product_count)
            
            # Tạo dữ liệu tồn kho cho mỗi sản phẩm
            for product_id in warehouse_products:
                # Tìm thông tin sản phẩm
                product_info = next((p for p in products if p[0] == product_id), None)
                if not product_info:
                    continue
                
                product_name = product_info[1]
                product_price = float(product_info[2])  # Convert Decimal to float
                
                # Tính giá vốn (80-95% giá bán)
                unit_cost = round(product_price * random.uniform(0.80, 0.95), 2)
                
                # Xác định số lượng tồn kho dựa trên loại sản phẩm và giá trị
                base_quantity = 0
                if "laptop" in product_name.lower() or product_price > 15000000:
                    # Sản phẩm giá trị cao, số lượng ít
                    base_quantity = random.randint(5, 20)
                elif "điện thoại" in product_name.lower() or product_price > 5000000:
                    # Sản phẩm giá trị trung bình
                    base_quantity = random.randint(10, 50)
                else:
                    # Phụ kiện hoặc sản phẩm giá trị thấp
                    base_quantity = random.randint(20, 100)
                
                # Điều chỉnh dựa trên dung lượng kho
                quantity = int(base_quantity * (float(warehouse_capacity) / 1000))
                
                # Thiết lập mức tồn kho an toàn và mức đặt hàng lại
                safety_stock = int(quantity * 0.2)
                reorder_level = int(quantity * 0.3)
                
                # Ngày kiểm kê gần nhất
                last_counted_at = datetime.now() - timedelta(days=random.randint(1, 30))
                
                # Thời gian tạo và cập nhật
                created_at = datetime.now() - timedelta(days=random.randint(90, 365))
                updated_at = created_at + timedelta(days=random.randint(1, 30))
                
                inventory_data.append({
                    'product_id': product_id,
                    'warehouse_id': warehouse_id,
                    'quantity': quantity,
                    'safety_stock': safety_stock,
                    'reorder_level': reorder_level,
                    'last_counted_at': last_counted_at,
                    'unit_cost': unit_cost,
                    'is_active': True,
                    'created_at': created_at,
                    'updated_at': updated_at
                })
                
        # In thông tin về số lượng bản ghi sẽ được tạo
        print(f"🔄 Chuẩn bị tạo {len(inventory_data)} bản ghi tồn kho")
        
        # Lưu dữ liệu vào database
        for inventory in inventory_data:
            query = text("""
                INSERT INTO inventory 
                (product_id, warehouse_id, quantity, safety_stock, reorder_level, 
                last_counted_at, unit_cost, is_active, created_at, updated_at)
                VALUES 
                (:product_id, :warehouse_id, :quantity, :safety_stock, :reorder_level, 
                :last_counted_at, :unit_cost, :is_active, :created_at, :updated_at)
            """)
            
            conn.execute(query, inventory)
        
        conn.commit()
        print(f"✅ Đã tạo {len(inventory_data)} bản ghi tồn kho")
        
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi khi tạo dữ liệu tồn kho: {e}")
        return False

# Hàm chính để tạo dữ liệu tồn kho
def generate_inventory_data():
    """Hàm chính để tạo dữ liệu tồn kho cho các kho hàng và sản phẩm"""
    print("\n==== BẮT ĐẦU TẠO DỮ LIỆU TỒN KHO ====\n")
    
    try:
        # Kết nối đến database
        conn = connect_to_db()
        
        # Xóa dữ liệu tồn kho cũ nếu có
        clear_old_data = True
        if clear_old_data:
            print("\n--- XÓA DỮ LIỆU TỒN KHO CŨ ---")
            try:
                tables = [
                    "inventory"
                ]
                for table in tables:
                    conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                conn.commit()
                print("✅ Đã xóa dữ liệu tồn kho cũ")
            except Exception as e:
                conn.rollback()
                print(f"❌ Lỗi khi xóa dữ liệu tồn kho cũ: {e}")
        
        # Lấy dữ liệu cần thiết
        print("\n--- LẤY DỮ LIỆU CẦN THIẾT ---")
        geo_locations = get_geo_locations(conn)
        products = get_products(conn)
        
        if not geo_locations or not products:
            print("❌ Không đủ dữ liệu để tiếp tục")
            return
        
        # Lấy thông tin kho hàng
        print("\n--- LẤY THÔNG TIN KHO HÀNG ---")
        warehouses = get_warehouses(conn)
        
        if not warehouses:
            print("❌ Không tìm thấy kho hàng nào, không thể tiếp tục")
            return
        
        # Tạo tồn kho
        print("\n--- TẠO TỒN KHO ---")
        create_inventory(conn, warehouses, products)
        
        print("\n==== HOÀN THÀNH TẠO DỮ LIỆU TỒN KHO ====")
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")
    finally:
        # Đóng kết nối
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    generate_inventory_data()
