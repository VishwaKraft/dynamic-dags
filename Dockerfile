FROM apache/airflow:2.5.2
COPY requirements.txt .
# Install Git
USER root
# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    g++ \
    python3-dev \
    libffi-dev \
    libssl-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install -r requirements.txt