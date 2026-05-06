FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY banking_producer/ banking_producer/

CMD ["python", "-m", "banking_producer.main"]
