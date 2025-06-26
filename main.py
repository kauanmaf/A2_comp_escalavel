# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, to_timestamp 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType
import os
import redis
import json
import time
import datetime
import pipeline_functions as pf
import psycopg2
import threading
import uuid
import db_stats_utils

# Definindo os esquemas para os dados aninhados
flight_data_payload_schema = StructType([
    StructField("id_voo", IntegerType(), True),
    StructField("id_reserva_voo", IntegerType(), True),
    StructField("valor", DoubleType(), True),
    StructField("data_reserva", StringType(), True)
])

hotel_data_payload_schema = StructType([
    StructField("id_reserva_hotel", IntegerType(), True),
    StructField("id_hotel", IntegerType(), True),
    StructField("valor", DoubleType(), True),
    StructField("data_reservada", StringType(), True),
    StructField("data_reserva", StringType(), True)
])

# Definindo o esquema para os dados brutos recebidos do Redis
raw_redis_message_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("data", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Esquema para a base de Hoteis (Master Data)
hoteis_master_schema = StructType([
    StructField("id_hotel", IntegerType(), False),
    StructField("cidade", StringType(), False),
    StructField("estrelas", IntegerType(), False)
])

# Esquema para a base de Voos (Master Data)
voos_master_schema = StructType([
    StructField("id_voo", IntegerType(), False),
    StructField("cidade_origem", StringType(), False),
    StructField("cidade_destino", StringType(), False),
    StructField("data", StringType(), False)
])

# Canal para ouvir solicitações de estatísticas
REDIS_CHANNEL_STAT_REQUEST = 'stat_requests'

# Esquema para as mensagens de solicitação de estatísticas
stat_request_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Variáveis globais para compartilhar o SparkSession e o cliente Redis
global_spark_session = None
global_redis_client = None

# Canal para ouvir solicitações de estatísticas
REDIS_CHANNEL_STAT_REQUEST = 'stat_requests'

# Esquema para as mensagens de solicitação de estatísticas
stat_request_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Variáveis globais para compartilhar o SparkSession e o cliente Redis
global_spark_session = None
global_redis_client = None

# Variáveis de ambiente para conexão com o RDS
PG_MASTER_HOST = os.getenv('PG_MASTER_HOST', 'a2-comp-escalavel-dados-fixos.col2wfyf2csx.us-east-1.rds.amazonaws.com')
PG_MASTER_PORT = int(os.getenv('PG_MASTER_PORT', 5432))
PG_MASTER_DB = os.getenv('PG_MASTER_DB', 'postgres')
PG_MASTER_USER = os.getenv('PG_MASTER_USER', 'A2CompEscalavel')
PG_MASTER_PASSWORD = os.getenv('PG_MASTER_PASSWORD', 'euadoroaemap')
THRESHOLD = os.getenv('THRESHOLD', 10000)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TravelBatchProcessorPersistent") \
        .getOrCreate()
    
    REDIS_HOST = os.getenv('REDIS_HOST', 'master.redis-brocker.slp5lw.use1.cache.amazonaws.com')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    MONITOR_INTERVAL = int(os.getenv('MONITOR_INTERVAL_SECONDS', 5))
    
    global_redis_client = None

    # Inicializa o cliente Redis para o thread principal e para o listener
    try:
        print(f"Host: {REDIS_HOST}, port: {REDIS_PORT}")
        global_redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        global_redis_client.ping()
        print(f"Conectado ao Redis em {REDIS_HOST}:{REDIS_PORT}.")
    except redis.exceptions.ConnectionError as e:
        print(f"Erro inicial de conexão com Redis: {e}. Exiting.")
        exit(1)

    conn = None

    # Loop principal que mantém o Spark job acordado
    try:
        while True:
            list_size_hoteis = global_redis_client.llen("raw_hotels")
            list_size_voos = global_redis_client.llen("raw_flights")
            total_rows = list_size_hoteis + list_size_voos
            print(f"Verificando listas: Tamanho atual Hoteis = {list_size_hoteis}, Voos = {list_size_voos}. Total = {total_rows}")

            if total_rows >= THRESHOLD:
                print(f"Threshold atingido! Processando {total_rows} registros.")

                # --- INÍCIO CAPTURA DE TEMPO E QUANTIDADES ---
                pipeline_start_time = datetime.datetime.now(datetime.timezone.utc)
                rows_hotels = list_size_hoteis
                rows_flights = list_size_voos
                
                # --- FIM CAPTURA DE TEMPO E QUANTIDADES ---

                raw_messages_hoteis = global_redis_client.lrange("raw_hotels", 0, list_size_hoteis - 1)
                raw_messages_voos = global_redis_client.lrange("raw_flights", 0, list_size_voos - 1)

                # Converte as strings JSON em dicionários Python
                parsed_data_hoteis = []
                for msg in raw_messages_hoteis:
                    try:
                        item = json.loads(msg)
                        # Converte a string de timestamp para objeto datetime
                        item['timestamp'] = datetime.datetime.fromisoformat(item['timestamp'])
                        parsed_data_hoteis.append(item)
                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"Erro ao parsear ou converter mensagem JSON/timestamp de hotéis")
                        continue 
                parsed_data_voos = []
                for msg in raw_messages_voos:
                    try:
                        item = json.loads(msg)
                        # Converte a string de timestamp para objeto datetime
                        item['timestamp'] = datetime.datetime.fromisoformat(item['timestamp'])
                        parsed_data_voos.append(item)
                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"Erro ao parsear ou converter mensagem JSON/timestamp de voos")
                        continue 

                # Cria um DataFrame a partir dos dados lidos
                if parsed_data_hoteis and parsed_data_voos:
                    df_raw_redis_message_hoteis = spark.createDataFrame(parsed_data_hoteis, schema=raw_redis_message_schema)
                    df_raw_redis_message_voos = spark.createDataFrame(parsed_data_voos, schema=raw_redis_message_schema)
                    print(f"DataFrames raw criados")

                    # Parseia o JSON na coluna 'data' usando o schema de payload
                    df_hoteis = df_raw_redis_message_hoteis.withColumn(
                        "parsed_payload", from_json(col("data"), hotel_data_payload_schema)
                    ).select(
                        col("company_id"),
                        col("timestamp").alias("event_timestamp"),
                        col("parsed_payload.id_hotel").alias("id_hotel"),
                        col("parsed_payload.valor").alias("valor"),
                        col("parsed_payload.data_reservada").alias("data_reservada"),
                        col("parsed_payload.data_reserva").alias("data_reserva"),
                        col("parsed_payload.id_reserva_hotel").alias("id_reserva_hotel"),
                    )

                    df_voos = df_raw_redis_message_voos.withColumn(
                        "parsed_payload", from_json(col("data"), flight_data_payload_schema)
                    ).select(
                        col("company_id"),
                        col("timestamp").alias("event_timestamp"),
                        col("parsed_payload.id_voo").alias("id_voo"),
                        col("parsed_payload.valor").alias("valor"),
                        col("parsed_payload.id_reserva_voo").alias("id_reserva_voo"),
                        col("parsed_payload.data_reserva").alias("data_reserva"),
                    )

                    # Converte a coluna 'data_reservada' para TimestampType
                    df_hoteis = df_hoteis.withColumn(
                        "data_reservada", to_timestamp(col("data_reservada")))
                    df_hoteis = df_hoteis.withColumn(
                        "data_reserva", to_timestamp(col("data_reserva")))     
                    df_voos = df_voos.withColumn(
                        "data_reserva", to_timestamp(col("data_reserva")))
                    
                    print(f"Lendo dados mestres de hotéis do RDS: {PG_MASTER_DB}.hoteis")
                    df_hoteis_master = spark.read.format("jdbc") \
                        .option("url", f"jdbc:postgresql://{PG_MASTER_HOST}:{PG_MASTER_PORT}/{PG_MASTER_DB}") \
                        .option("dbtable", "hoteis") \
                        .option("user", PG_MASTER_USER) \
                        .option("password", PG_MASTER_PASSWORD) \
                        .load()
                    print(f"Dados mestres de hotéis carregados do RDS.")

                    print(f"Lendo dados mestres de voos do RDS: {PG_MASTER_DB}.voos")
                    df_voos_master = spark.read.format("jdbc") \
                        .option("url", f"jdbc:postgresql://{PG_MASTER_HOST}:{PG_MASTER_PORT}/{PG_MASTER_DB}") \
                        .option("dbtable", "voos") \
                        .option("user", PG_MASTER_USER) \
                        .option("password", PG_MASTER_PASSWORD) \
                        .load()
                    print(f"Dados mestres de voos carregados do RDS.")

                    # Após carregar df_voos_master
                    df_voos_master = df_voos_master \
                        .withColumnRenamed("cidade_ida", "cidade_origem") \
                        .withColumnRenamed("cidade_volta", "cidade_destino")
                    
                    spark_proc_start = time.perf_counter()
                    joined_hotel = pf.join(df_hoteis_master, df_hoteis, "id_hotel")
                    all_stats_hotel = pf.groupby_city_month_hotels(joined_hotel)

                    grouped_city_hotel = pf.groupby_city_hotels(all_stats_hotel)
                    stats_city_hotel = grouped_city_hotel.drop("num_reservas")

                    stats_month_hotel = pf.groupby_month(all_stats_hotel)

                    joined_voos = pf.join(df_voos_master, df_voos, "id_voo")
                    all_stats_voos = pf.groupby_city_month_flights(joined_voos)

                    grouped_city_voos = pf.groupby_city_flights(all_stats_voos)
                    stats_city_voos = grouped_city_voos.drop("num_reservas")

                    stats_month_voos = pf.groupby_month(all_stats_voos)

                    joined_faturamentos = pf.join_profits(stats_month_hotel, stats_month_voos)
                    stats_faturamentos_totais = pf.sum_profits(joined_faturamentos)

                    stats_ticket_medio = pf.average_profits(grouped_city_hotel, grouped_city_voos)

                    stats_stars_hotel = pf.groupby_stars_hotels(joined_hotel)
                    stats_estrelas_medias_mes = pf.groupby_month_stars(joined_hotel)

                    filtered_sp_voos = pf.filter_sao_paulo_flights(joined_voos)
                    stats_month_sp_voos = pf.groupby_month_sp_flights(filtered_sp_voos)
                    stats_day_sp_voos = pf.groupby_day_sp_flights(filtered_sp_voos)

                    # print(f"Estatísticas de faturamento de hotéis por mês e companhia:")
                    # stats_month_hotel.show()
                    # print(f"Estatísticas de faturamento de hotéis por cidade e companhia:")
                    # stats_city_hotel.show()
                    # print(f"Estatísticas de faturamento de voos por mês e companhia:")
                    # stats_month_voos.show()
                    # print(f"Estatísticas de faturamento de voos por cidade e companhia:")
                    # stats_city_voos.show()
                    # print(f"Estatísticas de faturamento total por mês e companhia:")
                    # stats_faturamentos_totais.show()
                    # print(f"Estatísticas de faturamento médio por cidade e companhia:")
                    # stats_ticket_medio.show()
                    # print(f"Estatísticas de reservas de hotel por estrela e companhia:")
                    # stats_stars_hotel.show()
                    # print(f"Estatísticas de estrela média dos hotéis por mês e companhia:")
                    # stats_estrelas_medias_mes.show()
                    # print(f"Estatísticas de voos de SP reservados por mês e companhia:")
                    # stats_month_sp_voos.show()
                    # print(f"Estatísticas de reservas de voos de SP por dia e companhia:")
                    # stats_day_sp_voos.show()

                    # --- FIM DO PROCESSAMENTO SPARK ---
                    spark_proc_end = time.perf_counter()
                    spark_duration = spark_proc_end - spark_proc_start
                    db_write_start = time.perf_counter()
                    # --- INÍCIO DA GRAVAÇÃO NO BANCO ---

                    db_stats_utils.save_stats_dataframe(stats_month_hotel, "stats_month_hotel") 
                    db_stats_utils.save_stats_dataframe(stats_city_hotel, "stats_city_hotel")
                    db_stats_utils.save_stats_dataframe(stats_month_voos, "stats_month_voos")
                    db_stats_utils.save_stats_dataframe(stats_city_voos, "stats_city_voos")
                    db_stats_utils.save_stats_dataframe(stats_faturamentos_totais, "stats_faturamentos_totais")
                    db_stats_utils.save_stats_dataframe(stats_ticket_medio, "stats_ticket_medio")
                    db_stats_utils.save_stats_dataframe(stats_stars_hotel, "stats_stars_hotel")
                    db_stats_utils.save_stats_dataframe(stats_estrelas_medias_mes, "stats_estrelas_medias_mes")
                    db_stats_utils.save_stats_dataframe(stats_month_sp_voos, "stats_month_sp_voos")
                    db_stats_utils.save_stats_dataframe(stats_day_sp_voos, "stats_day_sp_voos")

                    db_write_end = time.perf_counter()
                    db_write_duration = db_write_end - db_write_start
                    pipeline_end_time = datetime.datetime.now(datetime.timezone.utc)

                    # Salva o log de execução da pipeline
                    db_stats_utils.save_pipeline_execution_log(
                        start_time=pipeline_start_time,
                        end_time=pipeline_end_time,
                        rows_hotels=rows_hotels,
                        rows_flights=rows_flights,
                        spark_duration_seconds=spark_duration,
                        db_write_duration_seconds=db_write_duration
                    )

                    # Limpeza atômica no Redis após o processamento bem-sucedido
                    global_redis_client.ltrim("raw_hotels", list_size_hoteis, -1)
                    global_redis_client.ltrim("raw_flights", list_size_voos, -1)
                    print(f"Limpos {list_size_hoteis} mensagens da lista Redis 'raw_hotels'.")
                    print(f"Limpos {list_size_voos} mensagens da lista Redis 'raw_flights'.")
                else:
                    print(f"Nenhum dado válido para criar DataFrame.")
            else:
                print(f"Não há dados suficientes para atingir o threshold. Total: {total_rows}, Threshold: {THRESHOLD}")
                time.sleep(5)

    except redis.exceptions.ConnectionError as e:
        print(f"Erro de conexão com Redis: {e}. Tentando reconectar no próximo ciclo...")
        global_redis_client = None
    except Exception as e:
        print(f"Ocorreu um erro inesperado no job Spark: {e}")
        import traceback
        traceback.print_exc()