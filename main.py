# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
import redis
import json
import time
import datetime

# Definindo os esquemas para os dados aninhados
flight_data_schema = StructType([
    StructField("flight_id", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("dest", StringType(), True),
    StructField("price", DoubleType(), True)
])

hotel_data_schema = StructType([
    StructField("hotel_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("nights", DoubleType(), True),
    StructField("cost", DoubleType(), True)
])

# Definindo o esquema para os dados brutos recebidos
raw_data_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("data", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TravelBatchProcessorPersistent") \
        .getOrCreate()

    print("SparkSession criada com sucesso para processamento em lote persistente!")

    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-server')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    MONITOR_INTERVAL = int(os.getenv('MONITOR_INTERVAL_SECONDS', 5))
    
    # Lista de chaves do Redis e seus thresholds
    list_thresholds_json = os.getenv('LIST_THRESHOLDS_JSON', '{"raw_travel_data_list": 50, "raw_flights": 50}')
    LIST_THRESHOLDS = json.loads(list_thresholds_json)

    r_client = None

    # Loop principal que mantém o Spark job acordado
    while True:
        try:
            if r_client is None:
                r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
                r_client.ping()
                print(f"Conectado ao Redis em {REDIS_HOST}:{REDIS_PORT}.")
            
            # Itera sobre cada lista definida e seu threshold
            for list_key, threshold in LIST_THRESHOLDS.items():
                list_size = r_client.llen(list_key)
                print(f"Verificando lista '{list_key}': Tamanho atual = {list_size}, Threshold = {threshold}")

                if list_size >= threshold:
                    print(f"Threshold atingido para '{list_key}' ({list_size} >= {threshold}). Processando lote...")

                    # Ler dados do Redis
                    raw_messages = r_client.lrange(list_key, 0, list_size - 1)
                    
                    if not raw_messages:
                        print(f"Erro: Threshold atingido para '{list_key}', mas a lista está vazia. Pulando.")
                        continue

                    # Converte as strings JSON em dicionários Python
                    parsed_data = []
                    for msg in raw_messages:
                        try:
                            item = json.loads(msg)
                            # Converte a string de timestamp para objeto datetime
                            item['timestamp'] = datetime.datetime.fromisoformat(item['timestamp'])
                            parsed_data.append(item)
                        except (json.JSONDecodeError, ValueError) as e:
                            print(f"Erro ao parsear ou converter mensagem JSON/timestamp de '{list_key}': {msg} - {e}")
                            continue 

                    # Cria um DataFrame a partir dos dados lidos
                    if parsed_data:
                        df_raw = spark.createDataFrame(parsed_data, schema=raw_data_schema)
                        print(f"DataFrame raw criado para '{list_key}':")
                        df_raw.show(truncate=False)

                        # Exemplo de processamento: Estatísticas de Reservas de Voos
                        if list_key == "raw_travel_data_list" or list_key == "raw_flights":
                            df_flight_bookings = df_raw.filter(col("event_type") == "flight_booking")

                            flight_booking_stats = df_flight_bookings \
                                .groupBy("company_id") \
                                .agg(count("*").alias("total_flight_bookings"))

                            print(f"Estatísticas de Reservas de Voos por Companhia para '{list_key}':")
                            flight_booking_stats.show()

                            # Armazenar estatísticas
                            output_stats_path = f"/tmp/processed_stats_batch/{list_key}"
                            flight_booking_stats.write.mode("append").parquet(output_stats_path)
                            print(f"Estatísticas de reservas de voos salvas em: {output_stats_path}")
                        else:
                            print(f"Processamento específico para '{list_key}' não implementado neste exemplo.")

                        # Remove os dados que acabaram de ser processados da lista Redis
                        r_client.ltrim(list_key, list_size, -1)
                        print(f"Limpos {list_size} mensagens da lista Redis '{list_key}'.")
                    else:
                        print(f"Nenhum dado válido para criar DataFrame para '{list_key}'.")
                else:
                    print(f"Não há dados suficientes na lista '{list_key}' para atingir o threshold.")

        except redis.exceptions.ConnectionError as e:
            print(f"Erro de conexão com Redis: {e}. Tentando reconectar no próximo ciclo...")
            r_client = None
        except Exception as e:
            print(f"Ocorreu um erro inesperado no job Spark: {e}")
            import traceback
            traceback.print_exc()
            
        print(f"Aguardando {MONITOR_INTERVAL} segundos para a próxima verificação...")
        time.sleep(MONITOR_INTERVAL)