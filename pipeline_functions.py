from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, date_format, count, sum, avg, month
from datetime import datetime, timedelta
import redis
import uuid
from datetime import datetime
import psycopg2 # Import psycopg2 for PostgreSQL connection

def join(df1, df2, id):
    df_joined = df1.join(df2, on = id)
    return df_joined

def groupby_city_month_hotels(df_joined_hotel):
    df_with_month = df_joined_hotel.withColumn(
        "mes_reserva", month(col("data_reserva"))
    )
    
    df_grouped_stats = df_with_month.groupBy(
        "company_id",
        "cidade",
        "mes_reserva"
    ).agg(
        sum("valor").alias("sum_valor")
    )

    return df_grouped_stats

def groupby_city_month_flights(df_joined_flights):
    df_with_month = df_joined_flights.withColumn(
        "mes_reserva", month(col("data_reserva"))
    )
    
    df_grouped_stats = df_with_month.groupBy(
        "company_id",
        "cidade_destino",
        "mes_reserva"
    ).agg(
        sum("valor").alias("sum_valor")
    )

    return df_grouped_stats

def groupby_city_hotels(df_stats_hotels):
    df_grouped_city = df_stats_hotels.groupBy(
        "company_id",
        "cidade",
    ).agg(
        sum("sum_valor").alias("sum_valor"),
        count("*").alias("num_reservas")
    ).orderBy(
        "company_id", "sum_valor"
    )

    return df_grouped_city

def groupby_city_flights(df_stats_flights):
    df_grouped_city = df_stats_flights.groupBy(
        "company_id",
        "cidade_destino",
    ).agg(
        sum("sum_valor").alias("sum_valor"),
        count("*").alias("num_reservas")
    ).orderBy(
        "company_id", "sum_valor"
    )

    return df_grouped_city

def groupby_month(df_stats):
    df_grouped_month = df_stats.groupBy(
        "company_id",
        "mes_reserva",
    ).agg(
        sum("sum_valor").alias("sum_valor")
    ).orderBy(
        "company_id", "mes_reserva"
    )

    return df_grouped_month

def join_profits(df_month_profits_hotels, df_month_profits_flights):
    renamed_df_month_profits_hotels = df_month_profits_hotels.withColumnRenamed("sum_valor", "sum_valor_hoteis")
    renamed_df_month_profits_flights = df_month_profits_flights.withColumnRenamed("sum_valor", "sum_valor_voos")

    joined_profits = renamed_df_month_profits_hotels.join(renamed_df_month_profits_flights,
                                                          on = ["company_id", "mes_reserva"])
    
    return joined_profits
    
def sum_profits(joined_profits):
    df_sum_profits = joined_profits.withColumn(
        "total_valor", col("sum_valor_hoteis") + col("sum_valor_voos")
    ).drop("sum_valor_hoteis", "sum_valor_voos")

    return df_sum_profits

def average_profits(grouped_city_hotels, grouped_city_flights):
    renamed_grouped_city_hotels = grouped_city_hotels.withColumnRenamed("sum_valor", "sum_valor_hoteis") \
                                                     .withColumnRenamed("num_reservas", "num_reservas_hoteis") \
                                                     .withColumnRenamed("cidade", "cidade_destino")

    renamed_grouped_city_flights = grouped_city_flights.withColumnRenamed("sum_valor", "sum_valor_voos") \
                                                       .withColumnRenamed("num_reservas", "num_reservas_voos")

    joined_df = renamed_grouped_city_hotels.join(renamed_grouped_city_flights,
                                                 on = ["company_id", "cidade_destino"])
    
    df_average_profits = joined_df \
        .withColumn(
            "sum_valor", col("sum_valor_hoteis") + col("sum_valor_voos")
        ).drop("sum_valor_hoteis", "sum_valor_voos") \
        .withColumn(
            "num_transacoes", col("num_reservas_hoteis") + col("num_reservas_voos")
        ).drop("num_reservas_hoteis", "num_reservas_voos")
    
    return df_average_profits

def groupby_stars_hotels(df_joined_hotels):
    df_grouped_stars = df_joined_hotels.groupBy(
        "company_id",
        "estrelas",
    ).agg(
        count("*").alias("num_reservas")
    ).orderBy(
        "company_id", "estrelas"
    )

    return df_grouped_stars

def groupby_month_stars(df_joined_hotels):
    df_with_month = df_joined_hotels.withColumn(
        "mes_reserva", month(col("data_reserva"))
    )

    df_grouped_month_stars = df_with_month.groupBy(
        "company_id",
        "mes_reserva"
    ).agg(
        sum("estrelas").alias("sum_estrelas"),
        count("*").alias("num_reservas")
    ).orderBy(
        "company_id", "mes_reserva"
    )

    return df_grouped_month_stars

def filter_sao_paulo_flights(df_joined_flights: DataFrame) -> DataFrame:
    # Filter for flights where either 'cidade_origem' or 'cidade_destino' is 'São Paulo'
    df_sp_relevant_flights = df_joined_flights.filter(
        (col("cidade_origem") == "São Paulo") | (col("cidade_destino") == "São Paulo")
    )
    return df_sp_relevant_flights

def groupby_month_sp_flights(df_sp_relevant_flights: DataFrame) -> DataFrame:
    df_with_month = df_sp_relevant_flights.withColumn(
        "mes", month(col("data_reserva"))
    )

    # Group by year_month and company_id to count total flights for the month
    return df_with_month.groupBy("mes", "company_id") \
                        .agg(count("*").alias("num_voos_reservados"))

def groupby_day_sp_flights(df_sp_relevant_flights: DataFrame) -> DataFrame:
    # Ensure 'event_date' is derived from 'data_reserva' for grouping
    df_with_event_date = df_sp_relevant_flights.withColumn(
        "data_reserva", to_date(col("data_reserva"))
    )

    # Group by event_date and company_id to count total flights
    return df_with_event_date.groupBy("data_reserva", "company_id") \
                            .agg(count("*").alias("num_reservas"))


def create_redis_set(spark_df: DataFrame, id_column_name: str, redis_conn: redis.Redis, redis_set_prefix: str = "ids_to_fetch") -> str:
    """
    Collects IDs from a Spark DataFrame and adds them to a Redis set.
    A new set key is generated with a timestamp and UUID for uniqueness.
    """
    if id_column_name not in spark_df.columns:
        print(f"Coluna '{id_column_name}' não encontrada no DataFrame.")
        return None

    ids_to_add = [str(row[id_column_name]) for row in spark_df.select(id_column_name).distinct().collect()]
    if not ids_to_add:
        print("Nenhum ID encontrado no DataFrame Spark para adicionar ao Redis.")
        return None

    # Generate a unique set key
    redis_set_key = f"{redis_set_prefix}:{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    print(f"Criando novo set Redis com chave '{redis_set_key}'...")

    try:
        redis_conn.sadd(redis_set_key, *ids_to_add)
        print(f"{len(ids_to_add)} IDs adicionados com sucesso ao set '{redis_set_key}'!")
        print(f"Número total de membros no set: {redis_conn.scard(redis_set_key)}")
    except Exception as e:
        print(f"Erro ao adicionar IDs ao Redis: {e}")
        return None

    return redis_set_key

def postgres_by_redis_set(redis_set_key: str, table_name: str, id_column_name_pg: str, spark_session: SparkSession, redis_conn: redis.Redis, pg_conn_params: dict) -> DataFrame:
    """
    Fetches data from a PostgreSQL table using IDs stored in a Redis set.
    
    Args:
        redis_set_key (str): The key of the Redis set containing the IDs.
        table_name (str): The name of the PostgreSQL table to query.
        id_column_name_pg (str): The name of the ID column in the PostgreSQL table.
        spark_session (SparkSession): The SparkSession object.
        redis_conn (redis.Redis): The Redis connection object.
        pg_conn_params (dict): Dictionary containing PostgreSQL connection parameters (e.g., 'host', 'database', 'user', 'password').

    Returns:
        DataFrame: A Spark DataFrame containing the fetched data, or None if an error occurs.
    """
    try:
        ids_from_redis = list(redis_conn.smembers(redis_set_key))
        if not ids_from_redis:
            print(f"O set '{redis_set_key}' está vazio ou não existe.")
            return None

        # Decode bytes to strings
        ids_from_redis = [id_val.decode('utf-8') for id_val in ids_from_redis]
        
        print(f"{len(ids_from_redis)} IDs recuperados do set '{redis_set_key}'")

    except redis.exceptions.ConnectionError as e:
        print(f"Erro ao conectar ao Redis: {e}")
        return None
    except Exception as e:
        print(f"Erro ao recuperar IDs do Redis: {e}")
        return None

    # Connect to PostgreSQL and fetch data
    try:
        # PostgreSQL connection string for Spark
        jdbc_url = f"jdbc:postgresql://{pg_conn_params['host']}:{pg_conn_params['port']}/{pg_conn_params['database']}"
        
        # Convert list of IDs to a comma-separated string for the IN clause
        # Ensure IDs are properly quoted if they are string types in PG
        ids_str = ','.join([f"'{id_val}'" for id_val in ids_from_redis]) if isinstance(ids_from_redis[0], str) else ','.join(ids_from_redis)
        
        # Build the query
        query = f"(SELECT * FROM {table_name} WHERE {id_column_name_pg} IN ({ids_str})) as data"

        df = spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", pg_conn_params['user']) \
            .option("password", pg_conn_params['password']) \
            .load()
        
        print(f"DataFrame do PostgreSQL para a tabela '{table_name}' criado com sucesso.")
        
        return df

    except Exception as e:
        print(f"Erro ao consultar o PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return None