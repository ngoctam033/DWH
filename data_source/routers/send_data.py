# from fastapi import APIRouter, Query
# from fastapi.responses import JSONResponse, FileResponse
# router = APIRouter()
# from services.get_data import clean_and_save_parquet
# import logging
# import pandas as pd
# from typing import Dict, List, Any
# import uuid
# import random
# from datetime import datetime, timedelta
# import os
# import tempfile
# import shutil
# from pathlib import Path
# from fastapi import Request
# from fastapi.responses import StreamingResponse
# import io

# # ...existing code...

# def generate_mock_data() -> Dict[str, List[Dict[str, Any]]]:
#     """Generate mock data for orders, order_items, and customers."""
#     # Generate customer data
#     customers = []
#     for i in range(1, 6):
#         customers.append({
#             "customer_id": f"C{i:03d}",
#             "name": f"Customer {i}",
#             "email": f"customer{i}@example.com",
#             "phone": f"+1-555-{i:03d}-{i*1111}",
#             "address": f"{i*100} Main St, City {i}",
#             "registration_date": (datetime.now() - timedelta(days=i*30)).strftime("%Y-%m-%d")
#         })
    
#     # Generate order data
#     orders = []
#     for i in range(1, 8):
#         customer_id = f"C{random.randint(1, 5):03d}"
#         orders.append({
#             "order_id": f"ORD{i:03d}",
#             "customer_id": customer_id,
#             "order_date": (datetime.now() - timedelta(days=i*3)).strftime("%Y-%m-%d"),
#             "status": random.choice(["Completed", "Processing", "Shipped", "Cancelled"]),
#             "total_amount": round(random.uniform(100, 1000), 2)
#         })
    
#     # Generate order item data
#     order_items = []
#     for i in range(1, 15):
#         order_id = f"ORD{random.randint(1, 7):03d}"
#         order_items.append({
#             "item_id": f"ITEM{i:03d}",
#             "order_id": order_id,
#             "product_name": f"Product {i}",
#             "quantity": random.randint(1, 5),
#             "unit_price": round(random.uniform(10, 200), 2),
#             "subtotal": round(random.uniform(10, 200) * random.randint(1, 5), 2)
#         })
    
#     return {
#         "customers": customers,
#         "orders": orders,
#         "order_items": order_items
#     }

# def save_data_to_parquet(data_dict):
#     """Convert dictionary data to parquet files and save them in a zip file."""
#     # Create a temporary directory
#     temp_dir = tempfile.mkdtemp()
#     try:
#         parquet_files = {}
        
#         # Convert each data table to a parquet file
#         for table_name, table_data in data_dict.items():
#             df = pd.DataFrame(table_data)
#             file_path = os.path.join(temp_dir, f"{table_name}.parquet")
#             df.to_parquet(file_path, index=False)
#             parquet_files[table_name] = file_path
        
#         return temp_dir, parquet_files
#     except Exception as e:
#         # Clean up in case of error
#         shutil.rmtree(temp_dir)
#         raise e

# @router.get("/mock-data")
# def get_mock_data():
#     """
#     Returns mock data for orders, order_items, and customers tables as JSON.
#     """
#     try:
#         mock_data = generate_mock_data()
#         return JSONResponse(
#             content={"status": "success", "data": mock_data},
#             status_code=200
#         )
#     except Exception as e:
#         logging.error(f"Error generating mock data: {str(e)}")
#         return JSONResponse(
#             content={"status": "error", "message": str(e)},
#             status_code=500
#         )

# @router.get("/mock-data-parquet/{table_name}")
# def get_mock_data_parquet(table_name: str):
#     """
#     Returns mock data for a specific table (customers, orders, or order_items) as a parquet file.
    
#     Args:
#         table_name: The name of the table to return (customers, orders, or order_items)
#     """
#     try:
#         if table_name not in ["customers", "orders", "order_items"]:
#             return JSONResponse(
#                 content={"status": "error", "message": f"Invalid table name: {table_name}. Valid options: customers, orders, order_items"},
#                 status_code=400
#             )
        
#         mock_data = generate_mock_data()
#         temp_dir, parquet_files = save_data_to_parquet(mock_data)
        
#         # Return the requested parquet file
#         return FileResponse(
#             path=parquet_files[table_name],
#             filename=f"{table_name}.parquet",
#             media_type="application/octet-stream",
#             background=shutil.rmtree(temp_dir)  # Clean up temp directory after response
#         )
#     except Exception as e:
#         logging.error(f"Error generating parquet data: {str(e)}")
#         return JSONResponse(
#             content={"status": "error", "message": str(e)},
#             status_code=500
#         )

# @router.get("/mock-data-all-parquet")
# def get_all_mock_data_parquet():
#     """
#     Returns all mock data tables (customers, orders, and order_items) as parquet files in a zip archive.
#     """
#     try:
#         mock_data = generate_mock_data()
        
#         # Convert each table to DataFrame and save as parquet
#         customers_df = pd.DataFrame(mock_data["customers"])
#         orders_df = pd.DataFrame(mock_data["orders"])
#         order_items_df = pd.DataFrame(mock_data["order_items"])
        
#         # Create temp directory for saving files
#         temp_dir = tempfile.mkdtemp()
        
#         # Save each DataFrame as a parquet file
#         customers_path = Path(temp_dir) / "customers.parquet"
#         orders_path = Path(temp_dir) / "orders.parquet"
#         order_items_path = Path(temp_dir) / "order_items.parquet"
        
#         customers_df.to_parquet(customers_path, index=False)
#         orders_df.to_parquet(orders_path, index=False)
#         order_items_df.to_parquet(order_items_path, index=False)
        
#         # Create a zip file containing all parquet files
#         zip_path = Path(temp_dir) / "mock_data.zip"
#         shutil.make_archive(zip_path.with_suffix(''), 'zip', temp_dir, 
#                            base_dir=None, 
#                            root_dir=temp_dir)
        
#         # Return the zip file
#         return FileResponse(
#             path=str(zip_path),
#             filename="mock_data.zip",
#             media_type="application/zip",
#             background=lambda: shutil.rmtree(temp_dir)  # Clean up temp directory after response
#         )
#     except Exception as e:
#         logging.error(f"Error generating parquet data: {str(e)}")
#         return JSONResponse(
#             content={"status": "error", "message": str(e)},
#             status_code=500
#         )