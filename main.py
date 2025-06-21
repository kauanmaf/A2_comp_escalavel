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

while True:
    try:
        db_stats_utils.get_pg_connection()
        break
    except:
        print("Erro de conexão")
        time.sleep(0.5)

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

def listen_for_stat_requests(spark: SparkSession, redis_conn: redis.Redis):
    """
    Função que será executada em uma thread separada para escutar solicitações de estatísticas.
    """
    pubsub = redis_conn.pubsub()
    pubsub.subscribe(REDIS_CHANNEL_STAT_REQUEST)
    print(f"Inscrito no canal Redis '{REDIS_CHANNEL_STAT_REQUEST}' para solicitações de estatísticas.")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                print(f"Mensagem de solicitação de estatísticas recebida: {data}")
                
                # Exemplo de como você processaria a solicitação:
                company_id = data.get('company_id')
                request_id = data.get('request_id')

                print(f"Processando solicitação de '{company_id}'. Request ID: {request_id}")
                                
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar mensagem JSON do Redis: {e}")
            except Exception as e:
                print(f"Erro inesperado ao processar solicitação de estatísticas: {e}")

THREASHOLD = 100

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TravelBatchProcessorPersistent") \
        .getOrCreate()

    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-server')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    MONITOR_INTERVAL = int(os.getenv('MONITOR_INTERVAL_SECONDS', 5))

    # PostgreSQL connection parameters (replace with your actual credentials)
    PG_HOST = os.getenv('PG_DATA_HOST', 'postgres-data')  # Nome do serviço no docker-compose
    PG_PORT = os.getenv('PG_DATA_PORT', '5432')
    PG_DATABASE = os.getenv('PG_DATA_DB', 'dados_gerais')
    PG_USER = os.getenv('PG_DATA_USER', 'emap')
    PG_PASSWORD = os.getenv('PG_DATA_PASSWORD', 'emap123')

    PG_CONN_PARAMS = {
        'host': PG_HOST,
        'port': PG_PORT,
        'database': PG_DATABASE,
        'user': PG_USER,
        'password': PG_PASSWORD
    }
    
    # Lista de chaves do Redis e seus thresholds
    list_thresholds_json = os.getenv('LIST_THRESHOLDS_JSON', '{"raw_hotels": 50, "raw_flights": 50}')
    LIST_THRESHOLDS = json.loads(list_thresholds_json)

    global_redis_client = None

    # Inicializa o cliente Redis para o thread principal e para o listener
    try:
        global_redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        global_redis_client.ping()
        print(f"Conectado ao Redis em {REDIS_HOST}:{REDIS_PORT}.")
    except redis.exceptions.ConnectionError as e:
        print(f"Erro inicial de conexão com Redis: {e}. Exiting.")
        exit(1)

    # Inicia a thread que escuta por solicitações de estatísticas
    stat_listener_thread = threading.Thread(target=listen_for_stat_requests, args=(global_spark_session, global_redis_client))
    stat_listener_thread.daemon = True
    stat_listener_thread.start()
    print("Thread de escuta de estatísticas iniciada.")

    conn = None

    # Loop principal que mantém o Spark job acordado
    while True:
        try:
            # if global_redis_client is None:
            #     global_redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            #     global_redis_client.ping()
            #     print(f"Conectado ao Redis em {REDIS_HOST}:{REDIS_PORT}.")

            if conn is None:
                    conn = psycopg2.connect(
                        host=PG_HOST,
                        port=PG_PORT,
                        database=PG_DATABASE,
                        user=PG_USER,
                        password=PG_PASSWORD
                    )

            total_rows = 0
            
            list_sizes = {}
            for list_key, threshold in LIST_THRESHOLDS.items():
                list_size = global_redis_client.llen(list_key)
                list_sizes[list_key] = list_size
                # Removido log detalhado de verificação de lista

            # Só processa se TODAS as listas existem E atingirem seus thresholds
            if all(list_key in list_sizes and list_sizes[list_key] >= LIST_THRESHOLDS[list_key] for list_key in LIST_THRESHOLDS):
                # print(f"Threshold atingido para todas as listas!")  # Removido log

                raw_messages_hoteis = global_redis_client.lrange("raw_hotels", 0, list_sizes["raw_hotels"] - 1)
                raw_messages_voos = global_redis_client.lrange("raw_flights", 0, list_sizes["raw_flights"] - 1)

                # Se qualquer uma das listas estiver vazia, não processa e aguarda o próximo ciclo
                if len(raw_messages_hoteis) == 0 or len(raw_messages_voos) == 0:
                    # print(f"Uma ou mais listas estão vazias após atingir o threshold. Aguardando próximo ciclo...")
                    time.sleep(MONITOR_INTERVAL)
                    continue

                # Converte as strings JSON em dicionários Python
                parsed_data_hoteis = []
                for msg in raw_messages_hoteis:
                    try:
                        item = json.loads(msg)
                        item['timestamp'] = datetime.datetime.fromisoformat(item['timestamp'])
                        parsed_data_hoteis.append(item)
                    except (json.JSONDecodeError, ValueError):
                        continue 
                parsed_data_voos = []
                for msg in raw_messages_voos:
                    try:
                        item = json.loads(msg)
                        item['timestamp'] = datetime.datetime.fromisoformat(item['timestamp'])
                        parsed_data_voos.append(item)
                    except (json.JSONDecodeError, ValueError):
                        continue 

                # Cria um DataFrame a partir dos dados lidos
                if parsed_data_hoteis and parsed_data_voos:
                    df_raw_redis_message_hoteis = spark.createDataFrame(parsed_data_hoteis, schema=raw_redis_message_schema)
                    df_raw_redis_message_voos = spark.createDataFrame(parsed_data_voos, schema=raw_redis_message_schema)
                    # print(f"DataFrames raw criados")  # Removido log

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
        

                    set_key_hoteis = pf.create_redis_set(df_hoteis,
                                                        "id_hotel", 
                                                        global_redis_client)
                    
                    set_key_voos = pf.create_redis_set(df_voos,
                                                       "id_voo",
                                                       global_redis_client)
                    
                    df_hoteis_master = pf.postgres_by_redis_set(set_key_hoteis,
                                                                "hoteis",
                                                                "id_hotel",
                                                                spark,
                                                                global_redis_client,
                                                                conn)

                    df_voos_master = pf.postgres_by_redis_set(set_key_voos,
                                                                "voos",
                                                                "id_voo",
                                                                spark,
                                                                global_redis_client,
                                                                conn)
                    
                    # Após carregar df_voos_master
                    df_voos_master = df_voos_master \
                        .withColumnRenamed("cidade_ida", "cidade_origem") \
                        .withColumnRenamed("cidade_volta", "cidade_destino")

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

                    # Limpa apenas os dados processados de cada lista
                    global_redis_client.ltrim("raw_hotels", list_sizes["raw_hotels"], -1)
                    global_redis_client.ltrim("raw_flights", list_sizes["raw_flights"], -1)
                    # print(f"Limpos {list_sizes['raw_hotels']} mensagens da lista Redis 'raw_hotels'.")
                    # print(f"Limpos {list_sizes['raw_flights']} mensagens da lista Redis 'raw_flights'.")
                # else:
                    # print(f"Nenhum dado válido para criar DataFrame.")
            else:
                # print(f"Não há dados suficientes para atingir o threshold em todas as listas.")
                time.sleep(MONITOR_INTERVAL)

        except redis.exceptions.ConnectionError as e:
            print(f"Erro de conexão com Redis: {e}. Tentando reconectar no próximo ciclo...")
            global_redis_client = None
        except Exception as e:
            print(f"Ocorreu um erro inesperado no job Spark: {e}")
            import traceback
            traceback.print_exc()
            
        # print(f"Aguardando {MONITOR_INTERVAL} segundos para a próxima verificação...")
        time.sleep(MONITOR_INTERVAL)