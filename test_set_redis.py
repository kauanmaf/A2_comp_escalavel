import redis
import psycopg2
from pyspark.sql import SparkSession
from functions import create_mock_data
from set_redis import createRedisSet, postgresByRedisSet
import os

# --- Configuration ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

PG_DATA_HOST = os.getenv('PG_DATA_HOST', 'localhost')
PG_DATA_PORT = int(os.getenv('PG_DATA_PORT', 5432))
PG_DATA_DB = os.getenv('PG_DATA_DB', 'dados_gerais')
PG_DATA_USER = os.getenv('PG_DATA_USER', 'emap')
PG_DATA_PASSWORD = os.getenv('PG_DATA_PASSWORD', 'emap123')

PG_STATS_HOST = os.getenv('PG_STATS_HOST', 'localhost')
PG_STATS_PORT = int(os.getenv('PG_STATS_PORT', 5433)) # Mapped to 5433 in docker-compose
PG_STATS_DB = os.getenv('PG_STATS_DB', 'dados_stats')
PG_STATS_USER = os.getenv('PG_STATS_USER', 'emap')
PG_STATS_PASSWORD = os.getenv('PG_STATS_PASSWORD', 'emap123')

def run_test():
    spark = None
    redis_conn = None
    pg_data_conn = None
    pg_stats_conn = None

    try:
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("TestRedisPostgres") \
            .getOrCreate()
        print("SparkSession created.")

        # Connect to Redis
        redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_conn.ping()
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        # Connect to PostgreSQL (dados_gerais)
        pg_data_conn = psycopg2.connect(
            host=PG_DATA_HOST,
            port=PG_DATA_PORT,
            database=PG_DATA_DB,
            user=PG_DATA_USER,
            password=PG_DATA_PASSWORD
        )
        pg_data_conn.autocommit = True # Important for creating tables immediately
        print(f"Connected to PostgreSQL (dados_gerais) at {PG_DATA_HOST}:{PG_DATA_PORT}/{PG_DATA_DB}")

        # Connect to PostgreSQL (dados_stats)
        pg_stats_conn = psycopg2.connect(
            host=PG_STATS_HOST,
            port=PG_STATS_PORT,
            database=PG_STATS_DB,
            user=PG_STATS_USER,
            password=PG_STATS_PASSWORD
        )
        pg_stats_conn.autocommit = True # Important for creating tables immediately
        print(f"Connected to PostgreSQL (dados_stats) at {PG_STATS_HOST}:{PG_STATS_PORT}/{PG_STATS_DB}")

        # --- Step 1: Create mock data using functions.py ---
        print("\nCreating mock data with Spark...")
        df_voos, df_reservas_voos, df_hoteis, df_reservas_hoteis = create_mock_data(spark)
        print("Mock DataFrames created:")
        df_voos.show(5)
        df_reservas_voos.show(5)
        df_hoteis.show(5)
        df_reservas_hoteis.show(5)

        # --- Step 2: Use createRedisSet to add IDs from a Spark DataFrame to Redis ---
        print("\nTesting createRedisSet for df_voos...")
        flight_ids_redis_set_key = createRedisSet(df_voos, "id_voo", redis_conn)
        if flight_ids_redis_set_key:
            print(f"Flight IDs added to Redis set: {flight_ids_redis_set_key}")
            print(f"Members in set '{flight_ids_redis_set_key}': {redis_conn.smembers(flight_ids_redis_set_key)}")

        print("\nTesting createRedisSet for df_hoteis...")
        hotel_ids_redis_set_key = createRedisSet(df_hoteis, "id_hotel", redis_conn)
        if hotel_ids_redis_set_key:
            print(f"Hotel IDs added to Redis set: {hotel_ids_redis_set_key}")
            print(f"Members in set '{hotel_ids_redis_set_key}': {redis_conn.smembers(hotel_ids_redis_set_key)}")

        # --- Step 3: Prepare PostgreSQL tables (if they don't exist) ---
        print("\nEnsuring PostgreSQL tables exist for testing postgresByRedisSet...")
        cursor_data = pg_data_conn.cursor()
        cursor_stats = pg_stats_conn.cursor()

        # Create 'voos' table in 'dados_gerais' if not exists
        cursor_data.execute("""
            CREATE TABLE IF NOT EXISTS voos (
                id_voo INTEGER PRIMARY KEY,
                cidade_origem VARCHAR(255),
                cidade_destino VARCHAR(255),
                data VARCHAR(255)
            );
        """)
        # Insert mock data into 'voos' table
        print("Inserting mock flight data into 'voos' table...")
        df_voos.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{PG_DATA_HOST}:{PG_DATA_PORT}/{PG_DATA_DB}") \
            .option("dbtable", "voos") \
            .option("user", PG_DATA_USER) \
            .option("password", PG_DATA_PASSWORD) \
            .mode("append") \
            .save()
        print("Mock flight data inserted.")

        # Create 'hoteis' table in 'dados_gerais' if not exists
        cursor_data.execute("""
            CREATE TABLE IF NOT EXISTS hoteis (
                id_hotel INTEGER PRIMARY KEY,
                cidade VARCHAR(255),
                estrelas INTEGER
            );
        """)
        # Insert mock data into 'hoteis' table
        print("Inserting mock hotel data into 'hoteis' table...")
        df_hoteis.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{PG_DATA_HOST}:{PG_DATA_PORT}/{PG_DATA_DB}") \
            .option("dbtable", "hoteis") \
            .option("user", PG_DATA_USER) \
            .option("password", PG_DATA_PASSWORD) \
            .mode("append") \
            .save()
        print("Mock hotel data inserted.")

        cursor_data.close()
        cursor_stats.close()

        # --- Step 4: Use postgresByRedisSet to retrieve data from PostgreSQL ---
        if flight_ids_redis_set_key:
            print(f"\nTesting postgresByRedisSet for flights using set '{flight_ids_redis_set_key}'...")
            df_flights_from_pg = postgresByRedisSet(flight_ids_redis_set_key, "voos", "id_voo", spark, redis_conn, pg_data_conn)
            if df_flights_from_pg:
                print("DataFrame retrieved from PostgreSQL (voos):")
                df_flights_from_pg.show()
                df_flights_from_pg.printSchema()

        if hotel_ids_redis_set_key:
            print(f"\nTesting postgresByRedisSet for hotels using set '{hotel_ids_redis_set_key}'...")
            df_hotels_from_pg = postgresByRedisSet(hotel_ids_redis_set_key, "hoteis", "id_hotel", spark, redis_conn, pg_data_conn)
            if df_hotels_from_pg:
                print("DataFrame retrieved from PostgreSQL (hoteis):")
                df_hotels_from_pg.show()
                df_hotels_from_pg.printSchema()

    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis. Please ensure Redis is running. Error: {e}")
    except psycopg2.OperationalError as e:
        print(f"Could not connect to PostgreSQL. Please ensure PostgreSQL is running and accessible. Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            spark.stop()
            print("SparkSession stopped.")
        if redis_conn:
            redis_conn.close()
            print("Redis connection closed.")
        if pg_data_conn:
            pg_data_conn.close()
            print("PostgreSQL (dados_gerais) connection closed.")
        if pg_stats_conn:
            pg_stats_conn.close()
            print("PostgreSQL (dados_stats) connection closed.")

if __name__ == "__main__":
    run_test()