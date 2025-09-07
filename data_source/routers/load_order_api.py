# routers/load_order_api.py

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from datetime import datetime
from services.db_config import get_db_connection  # Giữ nguyên import này
from services.gen_order import create_multiple_orders
from sqlalchemy import text
import random
from decimal import Decimal
router = APIRouter()

@router.get("/extract-order")
def extract_order(
    created_at: str = Query(..., description="Ngày đặt hàng, định dạng YYYY-MM-DD"),
    order_channel: str = Query(..., description="Kênh bán hàng (shopee, lazada, tiktok, website, facebook)")
):
    # Chuyển đổi ngày tạo sang kiểu datetime
    try:
        created_at_dt = datetime.strptime(created_at, "%Y-%m-%d")
    except Exception:
        return JSONResponse({"error": "Sai định dạng ngày. Định dạng đúng: YYYY-MM-DD"}, status_code=400)
    
    # Gọi hàm create_multiple_orders để tạo đơn hàng
    num_orders = random.randint(1, 100)
    created_orders = create_multiple_orders(num_orders=num_orders, order_date=created_at_dt)
    
    # Số lượng đơn hàng đã tạo
    created_count = len(created_orders) if created_orders else num_orders

    try:
        # Kết nối đến cơ sở dữ liệu
        engine = get_db_connection()
        
        with engine.connect() as conn:
            # Xây dựng truy vấn SQL - chỉ lấy dữ liệu từ bảng orders
            query = """
            SELECT 
                id, order_code, created_at, order_date, status, total_price, 
                payment_id, shipping_id,
                customer_id, logistics_partner_id
            FROM orders
            WHERE DATE(order_date) = DATE(:created_at)
            """
            
            # Thêm điều kiện lọc theo kênh bán hàng nếu có
            params = {"created_at": created_at_dt}
            if order_channel and order_channel.lower() != "all":
                query += " AND order_channel_id = (SELECT id FROM order_channel WHERE LOWER(name) = LOWER(:order_channel))"
                params["order_channel"] = order_channel
            
            # In log câu lệnh SQL cuối cùng được sử dụng
            # Hiển thị giá trị thực tế của tham số để debug
            compiled_query = text(query).bindparams(**params)
            compiled_sql = str(compiled_query.compile(compile_kwargs={"literal_binds": True}))
            print(f"===== SQL QUERY EXECUTED =====\n{compiled_sql}\n===========================")
            
            # Thực thi truy vấn
            result = conn.execute(text(query), params)
            
            # Chuyển kết quả thành danh sách các từ điển và cố tình tạo ra dữ liệu "bẩn"
            orders = []
            
            # Danh sách các giá trị status không đồng nhất
            status_variations = {
                                "DONE": ["COMPLETED", "FINISHED", "SUCCESSFUL"],  # Hoàn thành
                                "completed": ["DONE", "FINISHED", "SUCCESSFUL"],  # Hoàn thành
                                "DELIVERED": ["FULFILLED", "DELIVERY_SUCCESS", "RECEIVED"],  # Đã giao hàng
                                "CANCELLED": ["VOIDED", "TERMINATED", "ORDER_CANCELLED"],  # Đã hủy
                                "PROCESSING": ["IN_PROGRESS", "UNDER_PROCESS", "BEING_PREPARED"],  # Đang xử lý
                                "SHIPPED": ["DISPATCHED", "SENT_FROM_WAREHOUSE", "OUT_FOR_DELIVERY"],  # Đã gửi đi
                                "RETURNED": ["REFUNDED", "SENT_BACK", "RETURN_INITIATED"]  # Đã trả lại
                            }
            
            # Tạo dữ liệu bẩn cho các record
            for i, row in enumerate(result):
                order_dict = {}
                # Xử lý các kiểu dữ liệu và tạo lỗi có chủ đích
                for key, value in dict(row._mapping).items():
                    # 1. Vấn đề định dạng số thập phân không nhất quán
                    if key == 'total_price' and isinstance(value, Decimal):
                        if i % 5 == 0:  # 20% dữ liệu bị lỗi
                            order_dict[key] = str(value) + " VND"  # Thêm đơn vị tiền tệ vào chuỗi
                        elif i % 7 == 0:
                            order_dict[key] = str(value).replace(".", ",")  # Đổi dấu thập phân
                        else:
                            order_dict[key] = float(value)  # Giữ nguyên định dạng float
                            
                    # 2. Vấn đề định dạng ngày tháng không nhất quán
                    elif isinstance(value, datetime):
                        if i % 6 == 0:  # ~17% dữ liệu bị lỗi
                            order_dict[key] = value.strftime("%d/%m/%Y %H:%M:%S")  # DD/MM/YYYY
                        elif i % 8 == 0:
                            order_dict[key] = value.strftime("%m-%d-%Y")  # MM-DD-YYYY
                        elif i % 10 == 0:
                            order_dict[key] = int(value.timestamp())  # Unix timestamp
                        else:
                            order_dict[key] = value.isoformat()  # ISO format
                    
                    # 3. Vấn đề thiếu dữ liệu và dữ liệu null không nhất quán

                    
                    # 4. Vấn đề không nhất quán về các giá trị enum
                    elif key == 'status':
                        # Thay thế status bằng các biến thể khác nhau
                        if i % 3 == 0:  # 33% dữ liệu status bị thay đổi
                            # Chọn ngẫu nhiên một giá trị thay thế từ danh sách tương ứng
                            if value in status_variations:
                                order_dict[key] = random.choice(status_variations[value])
                            else:
                                order_dict[key] = value  # Nếu không tìm thấy key, giữ nguyên giá trị gốc
                        else:
                            order_dict[key] = value
                    
                    # 5. Các giá trị outlier và không hợp lệ
                    
                    # 6. Vấn đề về kiểu dữ liệu không nhất quán
                    elif key == 'order_code':
                        if i % 10 == 0:  # 10% dữ liệu
                            # Đôi khi order_code là số, đôi khi là chuỗi
                            order_dict[key] = int(hash(str(value)) % 100000)
                        else:
                            order_dict[key] = value
                            
                    # Giữ nguyên các trường ID, không làm "bẩn" chúng
                    elif key.endswith('_id') or key == 'id':
                        # Luôn giữ nguyên giá trị ID từ database
                        order_dict[key] = value
                    
                    # Các trường còn lại giữ nguyên
                    else:
                        order_dict[key] = value
                
                # 7. Thêm trường dư thừa ngẫu nhiên để tạo schema không nhất quán
                if i % 5 == 0:
                    order_dict['extra_field'] = f"Thông tin không cần thiết {i}"
                if i % 7 == 0:
                    order_dict['_metadata'] = {"source": "system", "timestamp": datetime.now().isoformat()}
                
                # 8. Đổi tên trường cho một số bản ghi

                
                orders.append(order_dict)
            
            # 9. Thêm một số bản ghi trùng lặp
            duplicate_count = min(5, len(orders) // 10)  # Khoảng 10% là bản ghi trùng
            for _ in range(duplicate_count):
                if orders:
                    duplicate = random.choice(orders).copy()
                    # Khi tạo bản ghi trùng lặp, giữ nguyên ID gốc
                    # Không thay đổi ID để tạo ra bản ghi hoàn toàn trùng lặp
                    orders.append(duplicate)
            
            # 10. Thêm một số bản ghi hoàn toàn sai định dạng
            if len(orders) > 0 and orders[0].get('id') is not None:
                # Lấy một ID thực từ bản ghi đầu tiên để đảm bảo ID là giá trị thực từ database
                sample_id = orders[0].get('id')
                weird_record = {
                    "id": sample_id,  # Sử dụng ID thực từ database
                    "order_date": "25/13/2025",  # Ngày không hợp lệ
                    "total_price": "free",
                }
                orders.append(weird_record)
            # In ra kích thước của orders
            print(f"Kích thước của orders: {len(orders)}")
            # Thay đổi cấu trúc JSON tùy thuộc vào order_channel để mô phỏng các nguồn dữ liệu khác nhau
            if order_channel.lower() == "shopee":
                # Cấu trúc JSON kiểu Shopee
                return JSONResponse({
                    "success": True,
                    "response": {
                        "order_list": orders,
                        "total_count": len(orders),
                        "metadata": {
                            "created_at": datetime.now().isoformat(),
                            "source": "shopee-api",
                            "version": "v2.1.3"
                        }
                    },
                    "error": None,
                    "message": f"Đã tạo {created_count} đơn hàng mới và tìm thấy tổng cộng {len(orders)} đơn hàng cho ngày {created_at}"
                })
            elif order_channel.lower() == "lazada":
                # Cấu trúc JSON kiểu Lazada
                return JSONResponse({
                    "code": "0",
                    "request_id": f"lazada-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    "data": {
                        "orders": orders,
                        "count": len(orders),
                        "has_next": False
                    },
                    "created_orders": created_count
                })
            elif order_channel.lower() == "tiktok":
                # Cấu trúc JSON kiểu TikTok Shop
                return JSONResponse({
                    "code": 0,
                    "message": "success", 
                    "request_id": f"tiktok-{hash(datetime.now().isoformat()) % 10000000}",
                    "data": {
                        "order_list": orders,
                        "total": len(orders),
                        "more": False
                    },
                    "created_count": created_count
                })
            elif order_channel.lower() == "tiki":
                # Cấu trúc JSON kiểu Tiki
                return JSONResponse({
                    "status": 200,
                    "data": {
                        "orders": orders,
                        "summary": {
                            "total_orders": len(orders),
                            "created_at": datetime.now().isoformat(),
                            "source": "tiki-api",
                            "version": "1.0.0"
                        }
                    },
                    "message": f"Đã tạo {created_count} đơn hàng mới và tổng cộng {len(orders)} đơn hàng cho ngày {created_at}"
                })
            elif order_channel.lower() == "website":
                # Cấu trúc JSON kiểu Website
                return JSONResponse({
                    "result": "success",
                    "orders": orders,
                    "meta": {
                        "count": len(orders),
                        "created_count": created_count,
                        "channel": "website",
                        "generated_at": datetime.now().isoformat()
                    }
                })
            
    except Exception as e:
        return JSONResponse({
            "success": False,
            "message": f"Lỗi khi truy vấn dữ liệu: {str(e)}",
            "orders": []
        }, status_code=500)
