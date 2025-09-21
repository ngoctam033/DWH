from fastapi import FastAPI
from routers import load_user_api, load_order_api, load_warehouse, load_order_items_api # send_data  # Import router

app = FastAPI()

# Gắn router từ các file trong thư mục routers
app.include_router(load_user_api.router)
app.include_router(load_order_api.router)
app.include_router(load_warehouse.router)
app.include_router(load_order_items_api.router)
# app.include_router(send_data.router)

# uvicorn main:app --reload --host 0.0.0.0 --port 9000
# 