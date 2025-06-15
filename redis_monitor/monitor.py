# monitor.py
import redis
import requests
import os
import json
import time

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
SPARK_APP_URL = os.getenv('SPARK_APP_URL', 'http://localhost:5000/trigger-spark-job')
MONITOR_INTERVAL = int(os.getenv('MONITOR_INTERVAL_SECONDS', 10))
LIST_THRESHOLDS = json.loads(os.getenv('LIST_THRESHOLDS_JSON', '{}'))

# Armazenar o estado dos jobs em andamento para evitar triggers duplicados
active_jobs = set()

def run_monitor():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print("Redis monitor started. Waiting for lists to accumulate data...")

    while True:
        for list_key, threshold in LIST_THRESHOLDS.items():
            try:
                list_size = r.llen(list_key)
                print(f"Checking list '{list_key}': Current size = {list_size}, Threshold = {threshold}")

                if list_size >= threshold and list_key not in active_jobs:
                    print(f"Threshold reached for '{list_key}' ({list_size} >= {threshold}). Triggering Spark job...")

                    # Adicionar ao set de jobs ativos imediatamente
                    active_jobs.add(list_key)

                    # Enviar requisição HTTP POST para o endpoint do Spark App
                    payload = {"redis_list_key": list_key}
                    headers = {'Content-Type': 'application/json'}
                    response = requests.post(SPARK_APP_URL, json=payload, headers=headers)

                    if response.status_code == 202:
                        print(f"Spark job trigger accepted for '{list_key}'.")

                    else:
                        print(f"Failed to trigger Spark job for '{list_key}'. Status: {response.status_code}, Response: {response.text}")
                        active_jobs.discard(list_key)

                elif list_key in active_jobs and list_size == 0:
                    # Se o job estava ativo e a lista foi limpa, considera o job concluído
                    print(f"List '{list_key}' is empty. Assuming Spark job completed. Removing from active jobs.")
                    active_jobs.remove(list_key)


            except redis.exceptions.ConnectionError as e:
                print(f"Error connecting to Redis: {e}. Retrying in {MONITOR_INTERVAL} seconds...")
                r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            except requests.exceptions.ConnectionError as e:
                print(f"Error connecting to Spark App: {e}. Retrying in {MONITOR_INTERVAL} seconds...")
            except Exception as e:
                print(f"An unexpected error occurred in monitor for '{list_key}': {e}")

        time.sleep(MONITOR_INTERVAL)

if __name__ == "__main__":
    run_monitor()