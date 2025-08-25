import random
import uuid
from sqlalchemy import text
from datetime import datetime, timedelta

# Import từ db_config
from .db_config import get_database_url, get_db_connection

def create_one_order(order_date = None):
    """
    Tạo một đơn hàng và tất cả các bản ghi liên quan (order items, shipment, status history)
    theo đúng quy trình từ A-Z của thương mại điện tử.
    
    Quy trình:
    1. Chọn khách hàng ngẫu nhiên
    2. Chọn sản phẩm ngẫu nhiên (1-5 sản phẩm)
    3. Tạo bản ghi order và order_items
    4. Tạo bản ghi shipment
    5. Tạo các bản ghi order_status_history theo trạng thái của đơn hàng
    
    Bảo đảm sự nhất quán giữa các bảng và logic nghiệp vụ.
    """
    try:
        # Kết nối đến cơ sở dữ liệu
        engine = get_db_connection()
        
        # Lấy ngày hiện tại để tạo đơn hàng
        now = datetime.now()
        # order_date = now - timedelta(days=random.randint(0, 30))  # Đơn hàng trong vòng 30 ngày

        print(f"Creating order for date: {order_date}")

        # Tạo kết nối trực tiếp
        conn = engine.connect()
        print("Đã kết nối đến database thành công")
        
        try:
            # Bắt đầu transaction thủ công
            conn.execute(text("BEGIN"))
            print("Transaction started.")
            
            # 1. Lấy một khách hàng ngẫu nhiên
            customer_result = conn.execute(text("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1"))
            customer_row = customer_result.fetchone()
            if not customer_row:
                print("Không tìm thấy khách hàng trong hệ thống!")
                conn.execute(text("ROLLBACK"))
                conn.close()
                return None
            
            customer_id = customer_row[0]
            print(f"Selected customer ID: {customer_id}")

            # 2. Lấy kênh đặt hàng
            channel_result = conn.execute(text("SELECT id FROM order_channel ORDER BY RANDOM() LIMIT 1"))
            channel_row = channel_result.fetchone()
            order_channel_id = channel_row[0] if channel_row else None
            print(f"Selected order channel ID: {order_channel_id}")
            
            # 3. Lấy đối tác logistics
            logistics_result = conn.execute(text("SELECT id FROM logistics_partner ORDER BY RANDOM() LIMIT 1"))
            logistics_row = logistics_result.fetchone()
            logistics_partner_id = logistics_row[0] if logistics_row else None
            print(f"Selected logistics partner ID: {logistics_partner_id}")
            
            # 4. Lấy phương thức vận chuyển
            shipping_result = conn.execute(text("SELECT id FROM shipping_method ORDER BY RANDOM() LIMIT 1"))
            shipping_row = shipping_result.fetchone()
            shipping_method_id = shipping_row[0] if shipping_row else None
            print(f"Selected shipping method ID: {shipping_method_id}")

            # 5. Lấy kho hàng
            warehouse_result = conn.execute(text("SELECT id FROM warehouse ORDER BY RANDOM() LIMIT 1"))
            warehouse_row = warehouse_result.fetchone()
            warehouse_id = warehouse_row[0] if warehouse_row else None
            print(f"Selected warehouse ID: {warehouse_id}")

            # 6. Lấy phương thức thanh toán
            payment_result = conn.execute(text("SELECT id FROM payment ORDER BY RANDOM() LIMIT 1"))
            payment_row = payment_result.fetchone()
            payment_id = payment_row[0] if payment_row else None
            print(f"Selected payment ID: {payment_id}")

            # 7. Chọn số lượng sản phẩm từ 1-5
            num_items = random.randint(1, 5)
            products_result = conn.execute(text(f"SELECT id FROM product ORDER BY RANDOM() LIMIT {num_items}"))
            products = products_result.fetchall()
            print(f"Selected products: {products}")
            if not products:
                print("Không tìm thấy sản phẩm nào trong hệ thống!")
                return None
            
            # 8. Tạo mã đơn hàng
            order_code = f"ORD-{order_date.strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
            print(f"Generated order code: {order_code}")
            
            # 9. Quyết định trạng thái đơn hàng
            # Phân bổ: 60% DELIVERED, 15% SHIPPED, 10% PROCESSING, 10% CANCELLED, 5% RETURNED
            status_choice = random.random()
            if status_choice < 0.10:
                final_status = "CANCELLED"
            elif status_choice < 0.20:
                final_status = "PROCESSING"
            elif status_choice < 0.35:
                final_status = "SHIPPED"
            elif status_choice < 0.40:
                final_status = "RETURNED"
            else:
                final_status = "DELIVERED"
            print(f"Final order status: {final_status}")
            
            # 10. Tính tổng giá trị đơn hàng và phí vận chuyển
            total_amount = 0
            items_data = []
            
            for product_row in products:
                product_id = product_row[0]
                unit_price = random.randint(50000, 500000)  # Giá sản phẩm từ 50k - 500k
                quantity = random.randint(1, 3)  # Mua 1-3 sản phẩm mỗi loại
                
                # Áp dụng giảm giá cho 30% sản phẩm
                discount_amount = 0
                if random.random() < 0.3:
                    discount_percent = random.uniform(0.05, 0.2)  # Giảm 5-20%
                    discount_amount = round(unit_price * discount_percent)
                
                amount = (unit_price - discount_amount) * quantity
                total_amount += amount
                
                items_data.append({
                    'product_id': product_id,
                    'unit_price': unit_price,
                    'discount_amount': discount_amount,
                    'quantity': quantity,
                    'amount': amount
                })
            
            # 11. Tính phí vận chuyển
            shipping_costs = {1: 30000, 2: 50000, 3: 70000, 4: 20000}  # Giá cơ bản theo phương thức
            base_shipping = shipping_costs.get(shipping_method_id, 30000)
            
            # Miễn phí cho đơn > 1tr (Standard/Economy)
            if total_amount >= 1000000 and shipping_method_id in [1, 4]:
                shipping_cost = 0
            # Giảm 50% cho đơn từ 500k-1tr
            elif 500000 <= total_amount < 1000000 and shipping_method_id in [1, 4]:
                shipping_cost = int(base_shipping * 0.5)
            else:
                shipping_cost = base_shipping
            
            # 12. Tính lợi nhuận (20-30% giá trị đơn hàng)
            profit_margin = random.uniform(0.2, 0.3)
            profit = round(total_amount * profit_margin, 2)
            
            # 13. Tạo bản ghi shipment
            tracking_number = f"TRK{uuid.uuid4().hex[:12].upper()}"
            is_expedited = shipping_method_id in [2, 3]  # Express và Same day
            
            # Tạo ngày tạo và cập nhật ngẫu nhiên trong quá khứ
            # Ngày tạo: trong khoảng từ 60 đến 30 ngày trước order_date
            created_date = order_date - timedelta(days=random.randint(30, 60))
            # Ngày cập nhật: trong khoảng từ ngày tạo đến order_date
            days_diff = (order_date - created_date).days
            updated_date = created_date + timedelta(days=random.randint(0, max(1, days_diff)))
            
            shipment_data = {
                'logistics_partner_id': logistics_partner_id,
                'warehouse_id': warehouse_id,
                'is_expedited': is_expedited,
                'shipping_method_id': shipping_method_id,
                'tracking_number': tracking_number,
                'shipping_cost': shipping_cost,
                'shipping_status': final_status,
                'is_active': True,
                'created_at': created_date,
                'updated_at': updated_date
            }
            
            try:
                result = conn.execute(
                    text("""
                    INSERT INTO shipment (logistics_partner_id, warehouse_id, is_expedited, 
                                        shipping_method_id, tracking_number, shipping_cost, 
                                        shipping_status, is_active, created_at, updated_at)
                    VALUES (:logistics_partner_id, :warehouse_id, :is_expedited, 
                            :shipping_method_id, :tracking_number, :shipping_cost, 
                            :shipping_status, :is_active, :created_at, :updated_at)
                    RETURNING id
                    """), 
                    shipment_data
                )
                shipment_id = result.scalar_one()
                print(f"Created shipment with ID: {shipment_id}")
            except Exception as e:
                print(f"Lỗi khi tạo shipment: {e}")
                # Rollback giao dịch hiện tại
                conn.execute(text("ROLLBACK"))
                conn.close()
                print("Transaction rolled back due to shipment error.")
                print("Database connection closed.")
                raise e
            
            # 14. Tạo bản ghi order
            # Lấy location_id từ khách hàng
            try:
                location_result = conn.execute(text("SELECT geo_location_id FROM customers WHERE id = :customer_id"), 
                                              {"customer_id": customer_id})
                location_row = location_result.fetchone()
                location_id = location_row[0] if location_row else None
                print(f"Selected location ID: {location_id}")
            except Exception as e:
                print(f"Lỗi khi lấy location ID: {e}")
                location_id = None
            
            # Random chọn discount hoặc không
            discount_id = None
            if random.random() < 0.4:  # 40% khả năng áp dụng khuyến mãi
                try:
                    discount_result = conn.execute(text("SELECT id FROM discount WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1"))
                    discount_row = discount_result.fetchone()
                    if discount_row:
                        discount_id = discount_row[0]
                        print(f"Applied discount with ID: {discount_id}")
                except Exception as e:
                    print(f"Lỗi khi lấy discount: {e}")
            
            order_data = {
                'customer_id': customer_id,
                'payment_id': payment_id,
                'shipping_id': shipment_id,
                'discount_id': discount_id,  # Random áp dụng discount hoặc None
                'location_id': location_id,
                'logistics_partner_id': logistics_partner_id,
                'order_channel_id': order_channel_id,
                'order_code': order_code,
                'order_date': order_date,
                'status': final_status,
                'shipping_cost': shipping_cost,
                'total_price': total_amount,
                'total_shipping_cost': shipping_cost,
                'profit': profit,
                'created_at': now,
                'updated_at': now,
                'is_active': True
            }
            
            try:
                result = conn.execute(
                    text("""
                    INSERT INTO orders (customer_id, payment_id, shipping_id, discount_id, location_id, 
                                       logistics_partner_id, order_channel_id, order_code, order_date, 
                                       status, shipping_cost, is_active, created_at, updated_at,
                                       total_price, total_shipping_cost, profit)
                    VALUES (:customer_id, :payment_id, :shipping_id, :discount_id, :location_id,
                            :logistics_partner_id, :order_channel_id, :order_code, :order_date, 
                            :status, :shipping_cost, :is_active, :created_at, :updated_at,
                            :total_price, :total_shipping_cost, :profit)
                    RETURNING id
                    """),
                    order_data
                )
                order_id = result.scalar_one()
                print(f"Created order with ID: {order_id}")
            except Exception as e:
                print(f"Lỗi khi tạo order: {e}")
                # Rollback giao dịch hiện tại
                conn.execute(text("ROLLBACK"))
                conn.close()
                print("Transaction rolled back due to order error.")
                print("Database connection closed.")
                raise e
            
            # 15. Tạo order_items
            order_item_ids = []
            for item in items_data:
                item_data = {
                    'order_id': order_id,
                    'product_id': item['product_id'],
                    'unit_price': item['unit_price'],
                    'quantity': item['quantity'],
                    'discount_amount': item['discount_amount'],
                    'amount': item['amount'],
                    'is_active': True
                }
                
                try:
                    result = conn.execute(
                        text("""
                        INSERT INTO order_items (order_id, product_id, unit_price, quantity,
                                              discount_amount, amount, is_active)
                        VALUES (:order_id, :product_id, :unit_price, :quantity,
                                :discount_amount, :amount, :is_active)
                        RETURNING id
                        """),
                        item_data
                    )
                    item_id = result.scalar_one()
                except Exception as e:
                    print(f"Lỗi khi tạo order item: {e}")
                    # Rollback giao dịch hiện tại
                    conn.execute(text("ROLLBACK"))
                    conn.close()
                    print("Transaction rolled back due to order item error.")
                    print("Database connection closed.")
                    raise e
                        
                order_item_ids.append(item_id)
            
            print(f"Created {len(order_item_ids)} order items.")

            # 16. Tạo lịch sử trạng thái đơn hàng
            status_history = []
            current_time = order_date
            
            # Luôn bắt đầu với CREATED
            status_history.append({
                'order_id': order_id,
                'status': 'CREATED',
                'changed_at': current_time,
                'changed_by': 'system',
                'is_active': True
            })
            
            # Tạo các trạng thái tiếp theo tùy thuộc vào final_status
            if final_status == "CANCELLED":
                # Đơn hàng bị hủy
                current_time += timedelta(hours=random.randint(1, 24))
                status_history.append({
                    'order_id': order_id,
                    'status': 'CANCELLED',
                    'changed_at': current_time,
                    'changed_by': random.choice(['customer', 'system', 'admin']),
                    'is_active': True
                })
            else:
                # Xác nhận đơn hàng
                current_time += timedelta(hours=random.randint(1, 12))
                status_history.append({
                    'order_id': order_id,
                    'status': 'CONFIRMED',
                    'changed_at': current_time,
                    'changed_by': 'admin',
                    'is_active': True
                })
                
                # Đang xử lý
                current_time += timedelta(hours=random.randint(1, 12))
                status_history.append({
                    'order_id': order_id,
                    'status': 'PROCESSING',
                    'changed_at': current_time,
                    'changed_by': 'warehouse',
                    'is_active': True
                })
                
                # Các trạng thái tiếp theo cho đơn hàng thành công/trả lại
                if final_status in ["SHIPPED", "DELIVERED", "RETURNED"]:
                    current_time += timedelta(hours=random.randint(6, 48))
                    status_history.append({
                        'order_id': order_id,
                        'status': 'SHIPPED',
                        'changed_at': current_time,
                        'changed_by': 'warehouse',
                        'is_active': True
                    })
                    
                if final_status == "DELIVERED":
                    current_time += timedelta(hours=random.randint(24, 72))
                    status_history.append({
                        'order_id': order_id,
                        'status': 'DELIVERED',
                        'changed_at': current_time,
                        'changed_by': 'logistics',
                        'is_active': True
                    })
                    
                if final_status == "RETURNED":
                    current_time += timedelta(hours=random.randint(24, 72))
                    status_history.append({
                        'order_id': order_id,
                        'status': 'RETURNED',
                        'changed_at': current_time,
                        'changed_by': 'logistics',
                        'is_active': True
                    })
            
            # Lưu lịch sử trạng thái
            status_ids = []
            for status in status_history:
                try:
                    result = conn.execute(
                        text("""
                        INSERT INTO order_status_history (order_id, status, changed_at, changed_by, is_active)
                        VALUES (:order_id, :status, :changed_at, :changed_by, :is_active)
                        RETURNING id
                        """),
                        status
                    )
                    status_id = result.scalar_one()
                except Exception as e:
                    print(f"Lỗi khi tạo status history: {e}")
                    # Rollback giao dịch hiện tại
                    conn.execute(text("ROLLBACK"))
                    conn.close()
                    print("Transaction rolled back due to status history error.")
                    print("Database connection closed.")
                    raise e
                
                status_ids.append(status_id)
            
            print(f"Created {len(status_ids)} status history records.")
            
            # 17. Cập nhật tồn kho (nếu đơn hàng không bị hủy)
            if final_status != "CANCELLED":
                try:
                    # Kiểm tra xem bảng inventory có tồn tại và có các cột cần thiết không
                    for item in items_data:
                        conn.execute(
                            text("""
                            UPDATE inventory
                            SET quantity = GREATEST(quantity - :qty, 0), 
                                updated_at = CURRENT_TIMESTAMP
                            WHERE product_id = :product_id AND warehouse_id = :warehouse_id
                            """),
                            {
                                "qty": item['quantity'],
                                "product_id": item['product_id'],
                                "warehouse_id": warehouse_id
                            }
                        )
                    print("Inventory updated successfully.")
                except Exception as e:
                    # Bỏ qua nếu không có bảng inventory hoặc thiếu cột
                    pass
            
            # Kết quả
            result = {
                'order_id': order_id,
                'order_code': order_code,
                'shipment_id': shipment_id,
                'customer_id': customer_id,
                'order_date': order_date,
                'final_status': final_status,
                'total_amount': total_amount,
                'shipping_cost': shipping_cost,
                'profit': profit,
                'items': [
                    {
                        'id': item_id,
                        'product_id': item['product_id'],
                        'quantity': item['quantity'],
                        'unit_price': item['unit_price'],
                        'discount': item['discount_amount'],
                        'amount': item['amount']
                    }
                    for item_id, item in zip(order_item_ids, items_data)
                ],
                'status_history': [
                    {
                        'id': status_id,
                        'status': status['status'],
                        'changed_at': status['changed_at'],
                        'changed_by': status['changed_by']
                    }
                    for status_id, status in zip(status_ids, status_history)
                ]
            }
            
            # Commit transaction
            conn.execute(text("COMMIT"))
            print("Transaction committed successfully.")
            
            # In thông tin chi tiết
            print("\n========== TẠO ĐƠN HÀNG THÀNH CÔNG ==========")
            print(f"Order ID: {result['order_id']}")
            print(f"Mã đơn hàng: {result['order_code']}")
            print(f"Khách hàng: {result['customer_id']}")
            print(f"Ngày đặt: {result['order_date']}")
            print(f"Trạng thái cuối: {result['final_status']}")
            print(f"Tổng tiền: {result['total_amount']:,} VND")
            print(f"Phí vận chuyển: {result['shipping_cost']:,} VND")
            print(f"Lợi nhuận: {result['profit']:,} VND")
            
            print("\nChi tiết đơn hàng:")
            for idx, item in enumerate(result['items'], 1):
                print(f"  {idx}. Sản phẩm ID {item['product_id']}: {item['quantity']} x {item['unit_price']:,} VND")
                if item['discount'] > 0:
                    print(f"     → Giảm giá: {item['discount']:,} VND")
                print(f"     → Thành tiền: {item['amount']:,} VND")
            
            print("\nLịch sử trạng thái:")
            for idx, status in enumerate(result['status_history'], 1):
                print(f"  {idx}. {status['status']} ({status['changed_at']}) - bởi {status['changed_by']}")
            
            print("=============================================")
            
        except Exception as e:
            # Rollback trong trường hợp có lỗi
            conn.execute(text("ROLLBACK"))
            print("Transaction rolled back due to an error.")
            raise e
        finally:
            # Đóng kết nối trong mọi trường hợp
            conn.close()
            print("Database connection closed.")
                
    except Exception as e:
        print(f"Lỗi khi tạo đơn hàng: {e}")
        return None

def create_multiple_orders(num_orders=100, order_date=None):
    """
    Tạo nhiều đơn hàng cùng một lúc
    
    Args:
        num_orders: Số lượng đơn hàng cần tạo (mặc định là 1000)
    """
    successful_orders = 0
    failed_orders = 0
    
    print(f"\n===== BẮT ĐẦU TẠO {num_orders} ĐƠN HÀNG =====\n")
    
    for i in range(1, num_orders + 1):
        print(f"\nĐang tạo đơn hàng {i}/{num_orders}...")
        try:
            create_one_order(order_date=order_date)
            successful_orders += 1
        except Exception as e:
            print(f"Không thể tạo đơn hàng thứ {i}: {e}")
            failed_orders += 1
        
        # In thống kê sau mỗi 100 đơn hàng
        if i % 100 == 0:
            print(f"\n----- THỐNG KÊ SAU {i}/{num_orders} ĐƠN HÀNG -----")
            print(f"Thành công: {successful_orders}")
            print(f"Thất bại: {failed_orders}")
            print(f"Tỷ lệ thành công: {successful_orders/i:.2%}")
            print("----------------------------------------\n")
    
    # Thống kê tổng kết
    print(f"\n===== KẾT QUẢ TẠO {num_orders} ĐƠN HÀNG =====")
    print(f"Tổng số đơn hàng thành công: {successful_orders}")
    print(f"Tổng số đơn hàng thất bại: {failed_orders}")
    print(f"Tỷ lệ thành công: {successful_orders/num_orders:.2%}")
    print("=====================================\n")
    
    # return successful_orders

if __name__ == "__main__":
    # Để thay đổi số lượng đơn hàng, hãy thay đổi tham số này
    create_multiple_orders(100)
