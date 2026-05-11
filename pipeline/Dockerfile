FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install kafka-python psycopg2-binary
COPY pipeline_kafka.py aggregator.py .
CMD ["sh", "-c", "python pipeline_kafka.py & python aggregator.py"]