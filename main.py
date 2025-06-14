# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
import redis
import json

# Define o esquema para os dados brutos recebidos
raw_data_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("data", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TravelBatchProcessor") \
        .getOrCreate()

    print("SparkSession criada com sucesso para processamento em lote!")

    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_LIST_KEY = os.getenv('REDIS_LIST_KEY', 'raw_travel_data_list')

    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Ler dados do Redis
    # Obtém todos os dados da lista
    raw_messages = r_client.lrange(REDIS_LIST_KEY, 0, -1)
    print(f"Read {len(raw_messages)} messages from Redis list '{REDIS_LIST_KEY}'.")

    if not raw_messages:
        print("No new messages to process. Exiting Spark job.")
        spark.stop()
        exit() # Sai se não houver dados

    # Converte as strings JSON em dicionários Python
    parsed_data = [json.loads(msg) for msg in raw_messages]

    # Cria um DataFrame a partir dos dados lidos
    df_raw = spark.createDataFrame(parsed_data, schema=raw_data_schema)
    print("DataFrame raw criado:")
    df_raw.show(truncate=False)

    # Processar Dados
    df_flight_bookings = df_raw.filter(col("event_type") == "flight_booking")

    flight_booking_stats = df_flight_bookings \
        .groupBy("company_id") \
        .agg(count("*").alias("total_flight_bookings"))

    print("Estatísticas de Reservas de Voos por Companhia:")
    flight_booking_stats.show()

    # Armazenar estatísticas
    output_stats_path = "/tmp/processed_stats_batch"
    flight_booking_stats.write.mode("append").parquet(output_stats_path)
    print(f"Estatísticas de reservas de voos salvas em: {output_stats_path}")

    # Remover os dados que acabaram de ser processados da lista Redis
    r_client.ltrim(REDIS_LIST_KEY, len(raw_messages), -1)
    print(f"Cleaned {len(raw_messages)} messages from Redis list '{REDIS_LIST_KEY}'.")


    print("Spark Batch job concluído.")
    spark.stop()
    print("SparkSession encerrada.")