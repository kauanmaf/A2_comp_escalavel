#!/bin/bash

echo "$(date): Submitting Spark job..."

/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.jars.ivy=/tmp/.ivy2_cache_app \
    /app/main.py

if [ $? -eq 0 ]; then
    echo "$(date): Spark job submitted successfully."
else
    echo "$(date): Spark job submission FAILED!"
fi