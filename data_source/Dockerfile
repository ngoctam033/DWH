# Bắt đầu từ một image Python cơ bản
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

# Tạo thư mục làm việc trong container
WORKDIR /app

# Sao chép toàn bộ nội dung thư mục `kafka_producer` trong máy host vào `/app` trong container
COPY . /app/

# Cài đặt các gói phụ thuộc từ requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Lệnh mặc định khi container khởi động
# uvicorn main:app --reload --host 0.0.0.0 --port 9000
CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0", "--port", "8000"]