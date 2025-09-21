# routers/load_order_items_api.py

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import List
from datetime import datetime
from services.db_config import get_db_connection
from sqlalchemy import text
import random
from decimal import Decimal
import logging

router = APIRouter()

@router.get("/extract-order-items")
def extract_order_items(
    order_codes: List[str] = Query(..., description="Danh sách mã đơn hàng (order_code)", example=["ORD001", "ORD002"])
):
    try:
        # in ra order_codes nhận được
        print(f"Received order_codes: {order_codes}")
        # Kết nối đến cơ sở dữ liệu
        engine = get_db_connection()
        
        with engine.connect() as conn:
            # Xây dựng truy vấn SQL để lấy order_items dựa trên order_codes
            query = """
            SELECT 
                oi.id, 
                oi.order_id,
                o.order_code,
                oi.product_id,
                p.name as product_name,
                p.product_sku,
                oi.unit_price,
                oi.discount_amount,
                oi.quantity,
                oi.amount,
                oi.is_active
            FROM 
                order_items oi
            JOIN 
                orders o ON oi.order_id = o.id
            JOIN
                product p ON oi.product_id = p.id
            WHERE 
                o.order_code IN :order_codes
            """
            
            # In log câu lệnh SQL cuối cùng được sử dụng
            compiled_query = text(query).bindparams(order_codes=tuple(order_codes))
            # compiled_sql = str(compiled_query.compile(compile_kwargs={"literal_binds": True}))
            print(f"===== SQL QUERY EXECUTED =====\n{compiled_query}\n===========================")
            
            # Thực thi truy vấn
            result = conn.execute(text(query), {"order_codes": tuple(order_codes)})
            # in ra số bản ghi lấy được
            rows = result.fetchall()
            print(f"Số bản ghi lấy được: {len(rows)}")
            # Chuyển kết quả thành danh sách các từ điển và tạo ra dữ liệu "bẩn"
            order_items = []
            
            # Tạo dữ liệu bẩn cho các record
            for i, row in enumerate(rows):
                item_dict = {}
                # Xử lý các kiểu dữ liệu và tạo lỗi có chủ đích
                for key, value in dict(row._mapping).items():
                    # 1. Vấn đề định dạng số thập phân không nhất quán
                    if key in ('unit_price', 'amount', 'discount_amount') and isinstance(value, Decimal):
                        if i % 5 == 0:  # 20% dữ liệu bị lỗi
                            item_dict[key] = str(value) + " VND"  # Thêm đơn vị tiền tệ vào chuỗi
                        elif i % 7 == 0:
                            item_dict[key] = str(value).replace(".", ",")  # Đổi dấu thập phân
                        else:
                            item_dict[key] = float(value)  # Giữ nguyên định dạng float
                    
                    # 2. Vấn đề về kiểu dữ liệu không nhất quán cho số lượng
                    elif key == 'quantity':
                        if i % 8 == 0:  # 12.5% dữ liệu
                            item_dict[key] = str(value) + " cái"  # Thêm đơn vị
                        elif i % 10 == 0:
                            item_dict[key] = float(value)  # Số lượng là float thay vì int
                        else:
                            item_dict[key] = value  # Giữ nguyên là int
                    
                    # 3. Vấn đề về boolean không nhất quán
                    elif key == 'is_active':
                        if i % 6 == 0:
                            item_dict[key] = "YES" if value else "NO"  # String thay vì boolean
                        elif i % 9 == 0:
                            item_dict[key] = 1 if value else 0  # Số thay vì boolean
                        else:
                            item_dict[key] = value  # Giữ nguyên là boolean
                    
                    # 4. Vấn đề tên sản phẩm
                    elif key == 'product_name':
                        if i % 7 == 0:
                            item_dict[key] = value.upper() if value else None  # Chuyển thành chữ hoa
                        elif i % 11 == 0:
                            item_dict[key] = value.lower() if value else None  # Chuyển thành chữ thường
                        else:
                            item_dict[key] = value
                            
                    # Giữ nguyên các trường ID, không làm "bẩn" chúng
                    elif key.endswith('_id') or key == 'id' or key == 'order_code' or key == 'product_sku':
                        item_dict[key] = value
                    
                    # Các trường còn lại giữ nguyên
                    else:
                        item_dict[key] = value
                
                # 5. Thêm trường dư thừa ngẫu nhiên để tạo schema không nhất quán
                if i % 5 == 0:
                    item_dict['extra_field'] = f"Thông tin không cần thiết {i}"
                if i % 7 == 0:
                    item_dict['_metadata'] = {"source": "system", "timestamp": datetime.now().isoformat()}
                # in ra item_dict để kiểm tra
                print(f"Generated item_dict: {item_dict}")
                order_items.append(item_dict)
            
            # 6. Thêm một số bản ghi trùng lặp
            duplicate_count = min(3, len(order_items) // 10)  # Khoảng 10% là bản ghi trùng
            for _ in range(duplicate_count):
                if order_items:
                    duplicate = random.choice(order_items).copy()
                    order_items.append(duplicate)
            
            # In ra kích thước của order_items
            print(f"Kích thước của order_items: {len(order_items)}")
            
            # Tạo cấu trúc response
            return JSONResponse({
                "success": True,
                "response": {
                    "order_item_list": order_items,
                    "total_count": len(order_items),
                    "metadata": {
                        "created_at": datetime.now().isoformat(),
                        "source": "order-items-api",
                        "version": "v1.0.0",
                        "requested_order_codes": order_codes
                    }
                },
                "message": f"Tìm thấy tổng cộng {len(order_items)} chi tiết đơn hàng cho {len(order_codes)} mã đơn"
            })
            
    except Exception as e:
        logging.error(f"Lỗi khi truy vấn dữ liệu: {str(e)}")
        return JSONResponse({
            "success": False,
            "message": f"Lỗi khi truy vấn dữ liệu: {str(e)}",
            "order_items": []
        }, status_code=500)