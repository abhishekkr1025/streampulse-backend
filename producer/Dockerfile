FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install kafka-python
COPY producer_kafka.py .
CMD ["python", "producer_kafka.py"]