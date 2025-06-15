# app.py
from flask import Flask, request, jsonify
import subprocess
import os
import threading

app = Flask(__name__)

# Configurações do Spark
SPARK_SUBMIT_PATH = "/opt/bitnami/spark/bin/spark-submit"
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
MAIN_PY_PATH = "/app/main.py"
SPARK_JARS_IVY = "/tmp/.ivy2_cache_app"

@app.route('/')
def index():
    return "Spark Job Trigger API is running!"

@app.route('/trigger-spark-job', methods=['POST'])
def trigger_spark_job():
    data = request.json
    if not data or 'redis_list_key' not in data:
        return jsonify({"status": "error", "message": "Missing 'redis_list_key' in request body"}), 400

    redis_list_key = data['redis_list_key']

    print(f"Received request to trigger Spark job for Redis list: {redis_list_key}")

    # Inicia o job Spark em uma nova thread para não bloquear a requisição HTTP
    def run_spark_job_in_background(key):
        try:
            command = [
                SPARK_SUBMIT_PATH,
                "--master", SPARK_MASTER_URL,
                "--deploy-mode", "client",
                "--conf", f"spark.jars.ivy={SPARK_JARS_IVY}",
                MAIN_PY_PATH,
                key
            ]
            print(f"Executing Spark submit command: {' '.join(command)}")
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print(f"Spark job for {key} finished successfully. Stdout:\n{result.stdout}")
            if result.stderr:
                print(f"Spark job for {key} stderr:\n{result.stderr}")

        except subprocess.CalledProcessError as e:
            print(f"Spark job for {key} failed with exit code {e.returncode}. Stdout:\n{e.stdout}\nStderr:\n{e.stderr}")
        except Exception as e:
            print(f"An unexpected error occurred while triggering Spark job: {e}")

    threading.Thread(target=run_spark_job_in_background, args=(redis_list_key,)).start()

    return jsonify({"status": "success", "message": f"Spark job triggered for {redis_list_key}"}), 202 # 202 Accepted

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)