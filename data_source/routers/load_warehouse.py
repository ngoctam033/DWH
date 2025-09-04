# routers/load_warehouse.py

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from datetime import datetime
from services.db_config import get_db_connection
from services.gen_warehouse import generate_warehouse_data
from sqlalchemy import text
from decimal import Decimal

router = APIRouter()

@router.get("/extract-inventory")
def extract_inventory():
    try:
        # Kết nối đến cơ sở dữ liệu
        engine = get_db_connection()
        
        # Tạo mới dữ liệu tồn kho (luôn tạo mới)
        data_generation_result = None
        try:
            # Sử dụng tỷ lệ mặc định là 0.7
            inventory_density = 0.7
            data_generation_result = generate_warehouse_data(inventory_density=inventory_density)
            print(f"Đã tạo dữ liệu tồn kho: {data_generation_result}")
        except Exception as gen_error:
            print(f"Cảnh báo: Không thể tạo dữ liệu tồn kho: {str(gen_error)}")
        
        with engine.connect() as conn:
            # Xây dựng truy vấn SQL cơ bản
            query = """
            SELECT 
                i.id, 
                i.product_id, 
                p.name as product_name,
                p.product_sku,
                i.warehouse_id, 
                w.name as warehouse_name,
                i.quantity, 
                i.safety_stock, 
                i.reorder_level, 
                i.last_counted_at, 
                i.unit_cost,
                p.price as unit_price,
                (p.price - i.unit_cost) as profit_margin,
                (i.quantity * i.unit_cost) as inventory_value
            FROM inventory i
            JOIN product p ON i.product_id = p.id
            JOIN warehouse w ON i.warehouse_id = w.id
            WHERE i.is_active = true
            """
            
            # Sắp xếp kết quả
            query += " ORDER BY w.name, p.name"
            
            # In log câu lệnh SQL cuối cùng được sử dụng
            compiled_sql = str(query)
            print(f"===== SQL QUERY EXECUTED =====\n{compiled_sql}\n===========================")
            
            # Thực thi truy vấn
            result = conn.execute(text(query))
            
            # Chuyển kết quả thành danh sách các từ điển và xử lý các kiểu dữ liệu
            inventory_items = []
            for row in result:
                item_dict = {}
                # Xử lý các kiểu dữ liệu cho từng trường dữ liệu
                for key, value in dict(row._mapping).items():
                    if isinstance(value, Decimal):
                        item_dict[key] = float(value)  # Chuyển Decimal thành float
                    elif isinstance(value, datetime):
                        item_dict[key] = value.isoformat()  # Chuyển datetime thành ISO string
                    else:
                        item_dict[key] = value  # Giữ nguyên các kiểu dữ liệu khác
                inventory_items.append(item_dict)
            
            # Trả về thông tin tổng hợp về tồn kho
            response_data = {
                "success": True,
                "total_items": len(inventory_items),
                "data_generation": data_generation_result,
                "inventory_data": inventory_items
            }
                
            return JSONResponse(response_data)
            
    except Exception as e:
        return JSONResponse({
            "success": False,
            "message": f"Lỗi khi truy vấn dữ liệu tồn kho: {str(e)}",
            "inventory_data": []
        }, status_code=500)


