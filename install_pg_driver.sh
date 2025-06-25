#!/bin/bash
set -euxo pipefail # Garante que o script pare em caso de erro
DRIVER_S3_PATH="s3://spark-processor3/libs/postgresql-42.7.7.jar"
LOCAL_DRIVER_PATH="/usr/lib/spark/jars/postgresql-42.7.7.jar"

echo "Baixando o driver JDBC do PostgreSQL de $DRIVER_S3_PATH para $LOCAL_DRIVER_PATH"
sudo mkdir -p $(dirname $LOCAL_DRIVER_PATH)
sudo aws s3 cp "$DRIVER_S3_PATH" "$LOCAL_DRIVER_PATH"

if [ -f "$LOCAL_DRIVER_PATH" ]; then
  echo "Driver PostgreSQL baixado com sucesso para $LOCAL_DRIVER_PATH"
else
  echo "ERRO: Falha ao baixar o driver PostgreSQL para $LOCAL_DRIVER_PATH"
  exit 1
fi