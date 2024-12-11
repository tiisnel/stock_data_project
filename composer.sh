#!/bin/bash

# Load .env file
if [ -f .env ]; then
    echo ".env exists"
else
    echo ".env file not found in the current directory!"
    exit 1
fi

# Ensure the shared network exists
docker network ls | grep -q "shared_network" || docker network create shared_network

# Ensure external volumes exist
docker volume ls | grep -q "airflow_logs" || docker volume create airflow_logs
docker volume ls | grep -q "airflow_postgres_data" || docker volume create airflow_postgres_data
docker volume ls | grep -q "minio_data" || docker volume create minio_data
docker volume ls | grep -q "duckdb_data" || docker volume create duckdb_data

# Define the command from the first argument
COMMAND=$1

# Handle commands
if [ "$COMMAND" == "init" ]; then
    echo "Initializing Airflow..."
    docker-compose -f docker-compose.base.yml \
                   -f airflow/docker-compose.airflow.yml run --rm airflow-init
    echo "Airflow initialized successfully!"
elif [ "$COMMAND" == "up" ]; then
    docker-compose -f docker-compose.base.yml \
                   -f airflow/docker-compose.airflow.yml \
                   -f minio/docker-compose.minio.yml up --build -d # to rebuild dockerfile, add --build flag!! 
elif [ "$COMMAND" == "down" ]; then
    docker-compose -f docker-compose.base.yml \
                   -f airflow/docker-compose.airflow.yml \
                   -f minio/docker-compose.minio.yml down
elif [ "$COMMAND" == "logs" ]; then
    echo "Fetching logs from all services..."
    docker-compose -f docker-compose.base.yml \
                   -f airflow/docker-compose.airflow.yml \
                   -f minio/docker-compose.minio.yml logs -f
elif [ "$COMMAND" == "start" ]; then
    docker-compose -f docker-compose.base.yml \
                   -f airflow/docker-compose.airflow.yml \
                   -f minio/docker-compose.minio.yml start

elif [ "$COMMAND" == "stop" ]; then
    docker-compose -f docker-compose.base.yml \
                   -f airflow/docker-compose.airflow.yml \
                   -f minio/docker-compose.minio.yml stop
else
    echo "Usage: $0 {init|up|down|start|stop|logs}"
    echo "  init   - run first time to create user"
    echo "  up   - Initialize and start all services"
    echo "  down - Stop all services"
    echo "  start - Restart all services"
    echo "  stop - Pause all services"
    echo "  logs - View logs of running services"
fi
