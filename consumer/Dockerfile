FROM bitnami/spark:3.5.1

USER root
# Install Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy consumer code
COPY consumer.py .
COPY .env .

# Run Spark with Kafka + Snowflake connectors
CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,net.snowflake:spark-snowflake_2.12:3.1.3", \
     "/app/consumer.py"]
