# routers/load_user_api.py

from services.gen_user import generate_customers
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from datetime import datetime
import random

router = APIRouter()

@router.get("/extract-user")
def extract_user(
    created_at: str = Query(..., description="Ngày tạo user, định dạng YYYY-MM-DD"),
    order_channel: str = Query(..., description="Kênh bán hàng (shopee, lazada, tiktok, website, facebook)")
):
    # Chuyển đổi ngày tạo sang kiểu datetime
    try:
        created_at_dt = datetime.strptime(created_at, "%Y-%m-%d")
    except Exception:
        return JSONResponse({"error": "Sai định dạng ngày. Định dạng đúng: YYYY-MM-DD"}, status_code=400)

    # Gọi hàm generate_customers với tham số từ client
    users = generate_customers(num_customers=random.randint(20, 100),created_at=created_at_dt, order_channel=order_channel)
    return JSONResponse(users)
