import os

# Tạo path tuyệt đối cho file .duckdb trong project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DUCKDB_FILE = os.path.join(BASE_DIR, "warehouse.duckdb")

