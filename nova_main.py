from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
import redis
import json
import time
import datetime
import pipeline_functions as pf # Assuming pipeline_functions.py is in the same directory
import psycopg2 # Import psycopg2 for direct PostgreSQL connection if needed for simple operations

# These are the schemas for the CONTENT of the 'data' column
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

# Defining the schema for raw data received FROM REDIS (message structure)
raw_redis_message_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("data", StringType(), True),  # 'data' is a JSON STRING
    StructField("timestamp", TimestampType(), True) # 'timestamp' already comes as TimestampType
])

# Schema for Hotels Master Data
hoteis_master_schema = StructType([
    StructField("id_hotel", IntegerType(), False),
    StructField("cidade", StringType(), False),
    StructField("estrelas", IntegerType(), False)
])

# Schema for Flights Master Data
voos_master_schema = StructType([
    StructField("id_voo", IntegerType(), False),
    StructField("cidade_origem", StringType(), False),
    StructField("cidade_destino", StringType(), False),
    StructField("data", StringType(), False) # Flight date as string (will be converted to Timestamp if needed later)
])

# --- FIXED DATA SIMULATING DATABASE TABLES ---
DADOS_VOOS_FIXOS = [
    {"id_voo": 1, "cidade_origem": "São Paulo", "cidade_destino": "Rio de Janeiro", "data": "2025-10-15T10:00:00"},
    {"id_voo": 2, "cidade_origem": "Rio de Janeiro", "cidade_destino": "Salvador", "data": "2025-10-16T12:30:00"},
    {"id_voo": 3, "cidade_origem": "Belo Horizonte", "cidade_destino": "Porto Alegre", "data": "2025-11-05T08:45:00"},
    {"id_voo": 4, "cidade_origem": "Nova York", "cidade_destino": "São Paulo", "data": "2025-11-20T22:00:00"},
    {"id_voo": 5, "cidade_origem": "Lisboa", "cidade_destino": "Recife", "data": "2025-12-01T15:10:00"},
]

DADOS_HOTEIS_FIXOS = [
    {"id_hotel": 1, "cidade": "Rio de Janeiro", "estrelas": 5},
    {"id_hotel": 2, "cidade": "Salvador", "estrelas": 4},
    {"id_hotel": 3, "cidade": "Porto Alegre", "estrelas": 4},
    {"id_hotel": 4, "cidade": "São Paulo", "estrelas": 5},
    {"id_hotel": 5, "cidade": "Recife", "estrelas": 3},
]

# PostgreSQL Master Data (Simulated - replace with actual DB connection if using)
# In a real scenario, you would connect to a PostgreSQL database
# and load this data dynamically. For this example, we'll keep it fixed.
DADOS_VOOS_TOTAL = [
    {"id_voo": 1, "company": "Empresa A", "origin": "São Paulo", "destination": "Rio de Janeiro", "price": 500.0, "duration_hours": 2, "flight_date": "2025-10-15"},
    {"id_voo": 2, "company": "Empresa B", "origin": "Rio de Janeiro", "destination": "Salvador", "price": 750.0, "duration_hours": 1.5, "flight_date": "2025-10-16"},
    {"id_voo": 3, "company": "Empresa C", "origin": "Belo Horizonte", "destination": "Porto Alegre", "price": 600.0, "duration_hours": 3, "flight_date": "2025-11-05"},
    {"id_voo": 4, "company": "Empresa A", "origin": "Nova York", "destination": "São Paulo", "price": 1200.0, "duration_hours": 10, "flight_date": "2025-11-20"},
    {"id_voo": 5, "company": "Empresa B", "origin": "Lisboa", "destination": "Recife", "price": 900.0, "duration_hours": 8, "flight_date": "2025-12-01"},
    {"id_voo": 6, "company": "Empresa C", "origin": "São Paulo", "destination": "Paris", "price": 1500.0, "duration_hours": 12, "flight_date": "2026-01-10"}
]

DADOS_HOTEIS_TOTAL = [
    {"id_hotel": 1, "company": "Hotelaria X", "name": "Grand Copacabana", "city": "Rio de Janeiro", "stars": 5, "daily_rate": 300.0, "amenities": ["pool", "spa"]},
    {"id_hotel": 2, "company": "Hotelaria Y", "name": "Bahia Encanto", "city": "Salvador", "stars": 4, "daily_rate": 150.0, "amenities": ["beach access"]},
    {"id_hotel": 3, "company": "Hotelaria Z", "name": "Planalto Palace", "city": "Porto Alegre", "stars": 4, "daily_rate": 180.0, "amenities": ["gym"]},
    {"id_hotel": 4, "company": "Hotelaria X", "name": "Paulista Executive", "city": "São Paulo", "stars": 5, "daily_rate": 250.0, "amenities": ["restaurant"]},
    {"id_hotel": 5, "company": "Hotelaria Y", "name": "Recife Inn", "city": "Recife", "stars": 3, "daily_rate": 100.0, "amenities": []},
    {"id_hotel": 6, "company": "Hotelaria Z", "name": "Manaus Jungle", "city": "Manaus", "stars": 3, "daily_rate": 200.0, "amenities": ["jungle tours"]}
]

THREASHOLD = 10 # This threshold is for processing from Redis lists

# Redis Set key prefix for IDs to fetch from Postgres
REDIS_SET_IDS_TO_FETCH_HOTELS = "ids_to_fetch:hotels"
REDIS_SET_IDS_TO_FETCH_FLIGHTS = "ids_to_fetch:flights"

# PostgreSQL connection parameters (replace with your actual credentials)
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres-db')
PG_PORT = int(os.getenv('POSTGRES_PORT', 5432))
PG_DATABASE = os.getenv('POSTGRES_DB', 'mydatabase')
PG_USER = os.getenv('POSTGRES_USER', 'myuser')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'mypassword')

PG_CONN_PARAMS = {
    'host': PG_HOST,
    'port': PG_PORT,
    'database': PG_DATABASE,
    'user': PG_USER,
    'password': PG_PASSWORD
}

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TravelBatchProcessorPersistent") \
        .getOrCreate()

    # Create DataFrames using inferred schema
    df_voos_master = spark.createDataFrame(DADOS_VOOS_FIXOS, schema=voos_master_schema)
    df_hoteis_master = spark.createDataFrame(DADOS_HOTEIS_FIXOS, schema=hoteis_master_schema)
    df_voos_master = df_voos_master.withColumn("data", to_timestamp(col("data")))

    print("SparkSession created successfully for persistent batch processing!")
    print("\nMaster Flights DataFrame loaded:")
    df_voos_master.show()
    df_voos_master.printSchema()
    print("\nMaster Hotels DataFrame loaded:")
    df_hoteis_master.show()
    df_hoteis_master.printSchema()

    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-server')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    MONITOR_INTERVAL = int(os.getenv('MONITOR_INTERVAL_SECONDS', 5))
    
    # List of Redis keys and their thresholds
    list_thresholds_json = os.getenv('LIST_THRESHOLDS_JSON', '{"raw_hotels": 5, "raw_flights": 5}') # Adjusted for testing
    LIST_THRESHOLDS = json.loads(list_thresholds_json)

    r_client = None

    # Loop principal that keeps the Spark job alive
    while True:
        try:
            if r_client is None:
                r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False) # Keep decode_responses=False for smembers
                r_client.ping()
                print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}.")

            total_rows = 0
            
            # Iterate over each defined list and its threshold
            for list_key, threshold in LIST_THRESHOLDS.items():
                list_size = r_client.llen(list_key)
                total_rows += list_size
                print(f"Checking list '{list_key}': Current size = {list_size}, Threshold = {threshold}")

            if total_rows >= THREASHOLD:
                print(f"Threshold reached! Processing {total_rows} messages.")

                # Read data from Redis
                raw_messages_hoteis = r_client.lrange("raw_hotels", 0, r_client.llen("raw_hotels") - 1)
                raw_messages_voos = r_client.lrange("raw_flights", 0, r_client.llen("raw_flights") - 1)
                
                if not raw_messages_hoteis and not raw_messages_voos:
                    print(f"No messages found after threshold check, or lists were cleared.")
                    continue

                parsed_data_hoteis = []
                for msg in raw_messages_hoteis:
                    try:
                        item = json.loads(msg.decode('utf-8')) # Decode message from bytes to string
                        item['timestamp'] = datetime.datetime.fromisoformat(item['timestamp'])
                        parsed_data_hoteis.append(item)
                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"Error parsing or converting JSON/timestamp for hotels: {e} - Message: {msg}")
                        continue
                
                parsed_data_voos = []
                for msg in raw_messages_voos:
                    try:
                        item = json.loads(msg.decode('utf-8')) # Decode message from bytes to string
                        item['timestamp'] = datetime.datetime.fromisoformat(item['timestamp'])
                        parsed_data_voos.append(item)
                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"Error parsing or converting JSON/timestamp for flights: {e} - Message: {msg}")
                        continue

                # Create DataFrames from parsed data
                df_raw_redis_message_hoteis = None
                df_raw_redis_message_voos = None

                if parsed_data_hoteis:
                    df_raw_redis_message_hoteis = spark.createDataFrame(parsed_data_hoteis, schema=raw_redis_message_schema)
                if parsed_data_voos:
                    df_raw_redis_message_voos = spark.createDataFrame(parsed_data_voos, schema=raw_redis_message_schema)
                
                if df_raw_redis_message_hoteis or df_raw_redis_message_voos:
                    print(f"Raw DataFrames created.")

                    # Process hotels data if available
                    if df_raw_redis_message_hoteis:
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
                        df_hoteis = df_hoteis.withColumn("data_reservada", to_timestamp(col("data_reservada")))
                        df_hoteis = df_hoteis.withColumn("data_reserva", to_timestamp(col("data_reserva")))

                        # Add IDs to Redis set for later PostgreSQL fetching
                        pf.create_redis_set(df_hoteis, "id_hotel", r_client, REDIS_SET_IDS_TO_FETCH_HOTELS)

                    # Process flights data if available
                    if df_raw_redis_message_voos:
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
                        df_voos = df_voos.withColumn("data_reserva", to_timestamp(col("data_reserva")))

                        # Add IDs to Redis set for later PostgreSQL fetching
                        pf.create_redis_set(df_voos, "id_voo", r_client, REDIS_SET_IDS_TO_FETCH_FLIGHTS)
                    
                    # --- BATCH PROCESSING (Existing Logic) ---
                    # Only proceed with batch processing if we have actual dataframes
                    if df_raw_redis_message_hoteis and df_raw_redis_message_voos:
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
                    else:
                        print("Not enough dataframes to perform full batch processing.")

                    # --- Fetching from PostgreSQL using Redis Sets ---
                    print("\n--- Fetching additional data from PostgreSQL based on Redis Sets ---")
                    
                    # Fetch hotel master data from PostgreSQL
                    if r_client.scard(REDIS_SET_IDS_TO_FETCH_HOTELS) > 0:
                        df_hotels_total = pf.postgres_by_redis_set(
                            REDIS_SET_IDS_TO_FETCH_HOTELS, 
                            "hoteis_total", # Replace with your actual PostgreSQL table name for hotels
                            "id_hotel", 
                            spark, 
                            r_client, 
                            PG_CONN_PARAMS
                        )
                        if df_hotels_total:
                            print("\nData from PostgreSQL (Hotels Total):")
                            df_hotels_total.show()
                            df_hotels_total.printSchema()
                            # Clear the Redis set after successful fetch
                            r_client.delete(REDIS_SET_IDS_TO_FETCH_HOTELS)
                            print(f"Redis set '{REDIS_SET_IDS_TO_FETCH_HOTELS}' cleared.")
                        else:
                            print(f"No data fetched from PostgreSQL for hotels using set '{REDIS_SET_IDS_TO_FETCH_HOTELS}'.")
                    else:
                        print(f"Redis set '{REDIS_SET_IDS_TO_FETCH_HOTELS}' is empty. No hotel data to fetch from PostgreSQL.")

                    # Fetch flight master data from PostgreSQL
                    if r_client.scard(REDIS_SET_IDS_TO_FETCH_FLIGHTS) > 0:
                        df_flights_total = pf.postgres_by_redis_set(
                            REDIS_SET_IDS_TO_FETCH_FLIGHTS, 
                            "voos_total", # Replace with your actual PostgreSQL table name for flights
                            "id_voo", 
                            spark, 
                            r_client, 
                            PG_CONN_PARAMS
                        )
                        if df_flights_total:
                            print("\nData from PostgreSQL (Flights Total):")
                            df_flights_total.show()
                            df_flights_total.printSchema()
                            # Clear the Redis set after successful fetch
                            r_client.delete(REDIS_SET_IDS_TO_FETCH_FLIGHTS)
                            print(f"Redis set '{REDIS_SET_IDS_TO_FETCH_FLIGHTS}' cleared.")
                        else:
                            print(f"No data fetched from PostgreSQL for flights using set '{REDIS_SET_IDS_TO_FETCH_FLIGHTS}'.")
                    else:
                        print(f"Redis set '{REDIS_SET_IDS_TO_FETCH_FLIGHTS}' is empty. No flight data to fetch from PostgreSQL.")


                    # Remove processed data from Redis lists
                    for list_key, threshold in LIST_THRESHOLDS.items():
                        current_list_size = r_client.llen(list_key)
                        if current_list_size > 0:
                            r_client.ltrim(list_key, current_list_size, -1) # Keep elements from current_list_size to end
                            print(f"Trimmed '{list_key}', removed {current_list_size} messages.")
                        else:
                            print(f"List '{list_key}' was already empty.")

                else:
                    print(f"No valid data to create DataFrames from Redis messages.")
            else:
                print(f"Not enough data to reach the threshold for processing.")

        except redis.exceptions.ConnectionError as e:
            print(f"Redis connection error: {e}. Attempting to reconnect on next cycle...")
            r_client = None
        except Exception as e:
            print(f"An unexpected error occurred in the Spark job: {e}")
            import traceback
            traceback.print_exc()
            
        print(f"Waiting {MONITOR_INTERVAL} seconds for the next check...")
        time.sleep(MONITOR_INTERVAL)