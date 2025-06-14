#!/bin/bash

echo "Submitting Spark job..."

sleep 20 # Manter o sleep para garantir que o master/worker estejam prontos

/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.jars.ivy=/tmp/.ivy2_cache_app \
    /app/main.py

echo "Spark job submitted. Exiting entrypoint script."