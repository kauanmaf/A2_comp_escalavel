# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, to_timestamp 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
import redis
import json
import time
import datetime
import pipeline_functions as pf
import psycopg2

# Definindo os esquemas para os dados aninhados (PAYLOADS DO REDIS)
# Estes são os schemas para o CONTEÚDO da coluna 'data'
flight_data_payload_schema = StructType([
    StructField("id_voo", IntegerType(), True), # Ajustado para IntegerType
    StructField("id_reserva_voo", IntegerType(), True), # Ajustado para IntegerType
    StructField("valor", DoubleType(), True),
    StructField("data_reserva", StringType(), True)
])

hotel_data_payload_schema = StructType([
    StructField("id_reserva_hotel", IntegerType(), True), # Ajustado para IntegerType
    StructField("id_hotel", IntegerType(), True), # Ajustado para IntegerType
    StructField("valor", DoubleType(), True),
    StructField("data_reservada", StringType(), True), # A data dentro do JSON ainda é string
    StructField("data_reserva", StringType(), True) # A data dentro do JSON ainda é string
])

# Definindo o esquema para os dados brutos recebidos DO REDIS (estrutura da MENSAGEM)
raw_redis_message_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("data", StringType(), True), # 'data' é uma STRING JSON
    StructField("timestamp", TimestampType(), True) # 'timestamp' já vem como TimestampType
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
    StructField("data", StringType(), False) # Data do voo como string (será convertida depois se precisar de Timestamp)
])

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

    r_client = None
    conn = None

    # Loop principal que mantém o Spark job acordado
    while True:
        try:
            if r_client is None:
                r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
                r_client.ping()
                print(f"Conectado ao Redis em {REDIS_HOST}:{REDIS_PORT}.")

            if conn is None:
                    conn = psycopg2.connect(
                        host=PG_HOST,
                        port=PG_PORT,
                        database=PG_DATABASE,
                        user=PG_USER,
                        password=PG_PASSWORD
                    )

            total_rows = 0
            
            # Itera sobre cada lista definida e seu threshold
            for list_key, threshold in LIST_THRESHOLDS.items():
                list_size = r_client.llen(list_key)
                total_rows += list_size
                print(f"Verificando lista '{total_rows}': Tamanho atual = {list_size}, Threshold = {THREASHOLD}")

            if total_rows >= THREASHOLD:
                print(f"Threshold atingido!")

                # Ler dados do Redis
                raw_messages_hoteis = r_client.lrange("raw_hotels", 0, list_size - 1)
                raw_messages_voos = r_client.lrange("raw_flights", 0, list_size - 1)
                
                if not raw_messages_hoteis or not raw_messages_voos:
                    print(f"Erro após threshold atingido.")
                    continue

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
                    

                    set_key_hoteis = pf.create_redis_set(df_hoteis,
                                                        "id_hotel", 
                                                        r_client)
                    
                    set_key_voos = pf.create_redis_set(df_voos,
                                                       "id_voo",
                                                       r_client)
                    
                    df_hoteis_master = pf.postgres_by_redis_set(set_key_hoteis,
                                                                "hoteis",
                                                                "id_hotel",
                                                                spark,
                                                                r_client,
                                                                conn)

                    df_voos_master = pf.postgres_by_redis_set(set_key_voos,
                                                                "voos",
                                                                "id_voo",
                                                                spark,
                                                                r_client,
                                                                conn)
                    
                    # Após carregar df_voos_master
                    df_voos_master = df_voos_master \
                        .withColumnRenamed("cidade_ida", "cidade_origem") \
                        .withColumnRenamed("cidade_volta", "cidade_destino")
                    
                    df_hoteis_master.show()

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

                    print(f"Estatísticas de faturamento de hotéis por mês e companhia:")
                    stats_month_hotel.show()
                    print(f"Estatísticas de faturamento de hotéis por cidade e companhia:")
                    stats_city_hotel.show()
                    print(f"Estatísticas de faturamento de voos por mês e companhia:")
                    stats_month_voos.show()
                    print(f"Estatísticas de faturamento de voos por cidade e companhia:")
                    stats_city_voos.show()
                    print(f"Estatísticas de faturamento total por mês e companhia:")
                    stats_faturamentos_totais.show()
                    print(f"Estatísticas de faturamento médio por cidade e companhia:")
                    stats_ticket_medio.show()
                    print(f"Estatísticas de reservas de hotel por estrela e companhia:")
                    stats_stars_hotel.show()
                    print(f"Estatísticas de estrela média dos hotéis por mês e companhia:")
                    stats_estrelas_medias_mes.show()
                    print(f"Estatísticas de voos de SP reservados por mês e companhia:")
                    stats_month_sp_voos.show()
                    print(f"Estatísticas de reservas de voos de SP por dia e companhia:")
                    stats_day_sp_voos.show()

                    # Remove os dados que acabaram de ser processados da lista Redis
                    r_client.ltrim("raw_hotels", list_size, -1)
                    r_client.ltrim("raw_flights", list_size, -1)
                    print(f"Limpos {list_size} mensagens da lista Redis '{list_key}'.")
                else:
                    print(f"Nenhum dado válido para criar DataFrame.")
            else:
                print(f"Não há dados suficientes para atingir o threshold.")

        except redis.exceptions.ConnectionError as e:
            print(f"Erro de conexão com Redis: {e}. Tentando reconectar no próximo ciclo...")
            r_client = None
        except Exception as e:
            print(f"Ocorreu um erro inesperado no job Spark: {e}")
            import traceback
            traceback.print_exc()
            
        print(f"Aguardando {MONITOR_INTERVAL} segundos para a próxima verificação...")
        time.sleep(MONITOR_INTERVAL)