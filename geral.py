from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, to_date, when, lit, date_format, sum
from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import json
import random
from datetime import datetime, timedelta

# Initialize Spark Session (This should be done in your Spark environment)
spark = SparkSession.builder.appName("TravelDataProcessor").getOrCreate()

# Mock data creation function (for testing and demonstration purposes)
def create_mock_data(spark_session: SparkSession):
    voos_data = []
    reservas_voos_data = []
    
    # NEW: Data for Hotels
    DADOS_HOTEIS_FIXOS = [
        {"id_hotel": 1, "cidade": "Rio de Janeiro", "estrelas": 5},
        {"id_hotel": 2, "cidade": "Salvador", "estrelas": 4},
        {"id_hotel": 3, "cidade": "Porto Alegre", "estrelas": 4},
        {"id_hotel": 4, "cidade": "São Paulo", "estrelas": 5},
        {"id_hotel": 5, "cidade": "Recife", "estrelas": 3},
        {"id_hotel": 6, "cidade": "Belo Horizonte", "estrelas": 4},
        {"id_hotel": 7, "cidade": "Curitiba", "estrelas": 3},
        {"id_hotel": 8, "cidade": "Fortaleza", "estrelas": 5},
        {"id_hotel": 9, "cidade": "Brasília", "estrelas": 4},
        {"id_hotel": 10, "cidade": "Manaus", "estrelas": 3}
    ]
    
    reservas_hoteis_data = [] # NEW: List to hold hotel reservations mock data

    companies = ["CiaViagemA", "CiaVoosB", "AgenciaTurC"]

    DADOS_VOOS_FIXOS = [
        {"id_voo": 1, "cidade_origem": "São Paulo", "cidade_destino": "Rio de Janeiro", "data": "2025-10-15T10:00:00"},
        {"id_voo": 2, "cidade_origem": "Rio de Janeiro", "cidade_destino": "Salvador", "data": "2025-10-16T12:30:00"},
        {"id_voo": 3, "cidade_origem": "Belo Horizonte", "cidade_destino": "Porto Alegre", "data": "2025-11-05T08:45:00"},
        {"id_voo": 4, "cidade_origem": "Nova York", "cidade_destino": "São Paulo", "data": "2025-11-20T22:00:00"},
        {"id_voo": 5, "cidade_origem": "Lisboa", "cidade_destino": "Recife", "data": "2025-12-01T15:10:00"},
        {"id_voo": 6, "cidade_origem": "São Paulo", "cidade_destino": "Rio de Janeiro", "data": "2025-10-16T11:00:00"},
        {"id_voo": 7, "cidade_origem": "Rio de Janeiro", "cidade_destino": "São Paulo", "data": "2025-10-17T13:00:00"},
        {"id_voo": 8, "cidade_origem": "São Paulo", "cidade_destino": "São Paulo", "data": "2025-10-18T14:00:00"},
        {"id_voo": 9, "cidade_origem": "São Paulo", "cidade_destino": "Belo Horizonte", "data": "2025-10-19T09:00:00"},
        {"id_voo": 10, "cidade_origem": "Curitiba", "cidade_destino": "São Paulo", "data": "2025-10-20T16:00:00"}
    ]

    reserva_voo_id_counter = 1
    reserva_hotel_id_counter = 1 # NEW: Counter for hotel reservation IDs
    start_date = datetime(2025, 10, 1) # Start date for reservations

    for i in range(50): # Generate some mock data
        # Flight Reservations
        voo_selecionado = random.choice(DADOS_VOOS_FIXOS)
        company_id_flight_res = random.choice(companies)
        valor_reserva_voo = round(random.uniform(300, 2500), 2)
        
        current_date_flight = start_date + timedelta(days=i % 10)
        data_reserva_voo = current_date_flight.isoformat()

        reservas_voos_data.append({
            "id_reserva_voo": reserva_voo_id_counter,
            "company_id": company_id_flight_res,
            "id_voo": voo_selecionado['id_voo'],
            "valor": valor_reserva_voo,
            "data_reserva": data_reserva_voo,
            "event_type": "flight_reservation"
        })
        reserva_voo_id_counter += 1

        # NEW: Hotel Reservations
        hotel_selecionado = random.choice(DADOS_HOTEIS_FIXOS)
        valor_reserva_hotel = round(random.uniform(150, 2000), 2)
        
        # Simulate data_reservada within a reasonable range from data_reserva
        current_date_hotel_reserva = start_date + timedelta(days=random.randint(0, 30)) # Reservation date
        data_reserva_hotel = current_date_hotel_reserva.isoformat()

        # Simulate data_reservada (check-in date) a bit after data_reserva
        data_reservada_hotel_dt = current_date_hotel_reserva + timedelta(days=random.randint(5, 60))
        data_reservada_hotel = data_reservada_hotel_dt.isoformat()

        reservas_hoteis_data.append({
            "id_reserva_hotel": reserva_hotel_id_counter,
            "id_hotel": hotel_selecionado['id_hotel'],
            "valor": valor_reserva_hotel,
            "data_reservada": data_reservada_hotel,
            "data_reserva": data_reserva_hotel,
            "company_id": random.choice(companies), # Assign a company_id for hotel reservations too
            "event_type": "hotel_reservation"
        })
        reserva_hotel_id_counter += 1


    # Create 'voos' DataFrame from fixed data
    df_voos = spark_session.createDataFrame(DADOS_VOOS_FIXOS).select(
        col("id_voo").cast(IntegerType()),
        col("cidade_origem").cast(StringType()),
        col("cidade_destino").cast(StringType()),
        col("data").cast(StringType())
    )

    # Create 'reservas_voos' DataFrame
    df_reservas_voos = spark_session.createDataFrame(reservas_voos_data).select(
        col("id_reserva_voo").cast(IntegerType()),
        col("company_id").cast(StringType()),
        col("id_voo").cast(IntegerType()),
        col("valor").cast(DoubleType()),
        col("data_reserva").cast(StringType()),
        col("event_type").cast(StringType())
    )

    # NEW: Create 'hoteis' DataFrame
    df_hoteis = spark_session.createDataFrame(DADOS_HOTEIS_FIXOS).select(
        col("id_hotel").cast(IntegerType()),
        col("cidade").cast(StringType()),
        col("estrelas").cast(IntegerType())
    )

    # NEW: Create 'reservas_hoteis' DataFrame
    df_reservas_hoteis = spark_session.createDataFrame(reservas_hoteis_data).select(
        col("id_reserva_hotel").cast(IntegerType()),
        col("id_hotel").cast(IntegerType()),
        col("valor").cast(DoubleType()),
        col("data_reservada").cast(StringType()),
        col("data_reserva").cast(StringType()),
        col("company_id").cast(StringType()),
        col("event_type").cast(StringType())
    )
    
    return df_voos, df_reservas_voos, df_hoteis, df_reservas_hoteis

def join_flight_and_reservation_data(df_voos: DataFrame, df_reservas_voos: DataFrame) -> DataFrame:
    """
    Performs an inner join between flight details and flight reservations.
    """
    df_joined = df_reservas_voos.join(
        df_voos,
        on="id_voo",
        how="inner"
    ).withColumnRenamed("data", "flight_date") # Rename 'data' from df_voos to 'flight_date'

    return df_joined

def group_by_city_month_client_sum_value(df_flight_reservations_detailed: DataFrame) -> DataFrame:
    """
    Groups flight reservation data by origin city, destination city, month, and client,
    then sums the 'valor' (value). This represents 'GroupBy (cidade mês, cliente) SUM (valor)'
    from your diagram.
    """
    df_with_month = df_flight_reservations_detailed.withColumn(
        "year_month", date_format(to_date(col("data_reserva")), "yyyy-MM")
    )

    # Group by origin city, destination city, month, and company_id (cliente)
    return df_with_month.groupBy(
        "cidade_origem",
        "cidade_destino",
        "year_month",
        "company_id" # This is 'cliente'
    ).agg(
        sum("valor").alias("total_valor_reservas_mensal_cidade_cliente")
    )

def group_by_month_reservation_sum_value_top_10(df_flight_reservations_detailed: DataFrame) -> DataFrame:
    """
    Groups flight reservation data by month, sums the 'valor' (value),
    and returns the top 10 months by total value.
    This represents 'GroupBy mes Reserva, sum(valor) select 10' from your diagram.
    """
    df_with_month = df_flight_reservations_detailed.withColumn(
        "year_month", date_format(to_date(col("data_reserva")), "yyyy-MM")
    )

    # Group by month and sum the 'valor'
    df_sum_by_month = df_with_month.groupBy("year_month").agg(
        sum("valor").alias("total_valor_reservas_mensal")
    )

    # Order by total_valor_reservas_mensal descending and select top 10
    return df_sum_by_month.orderBy(col("total_valor_reservas_mensal").desc()).limit(10)


def join_hotel_and_reservation_data(df_hoteis: DataFrame, df_reservas_hoteis: DataFrame) -> DataFrame:
    """
    Performs an inner join between hotel details and hotel reservations.
    """
    df_joined = df_reservas_hoteis.join(
        df_hoteis,
        on="id_hotel",
        how="inner"
    )
    return df_joined

def group_by_hotel_city_month_sum_value(df_hotel_reservations_detailed: DataFrame) -> DataFrame:
    """
    Groups hotel reservation data by hotel city, month of reservation, and client,
    then sums the 'valor' (value).
    """
    df_with_month = df_hotel_reservations_detailed.withColumn(
        "year_month", date_format(to_date(col("data_reserva")), "yyyy-MM")
    )

    # Group by hotel city, month, and company_id (cliente)
    return df_with_month.groupBy(
        "cidade",
        "year_month",
        "company_id"
    ).agg(
        sum("valor").alias("total_valor_reservas_hoteis_mensal_cidade_cliente")
    )

def group_by_month_hotel_reservation_sum_value_top_10(df_hotel_reservations_detailed: DataFrame) -> DataFrame:
    """
    Groups hotel reservation data by month of reservation, sums the 'valor' (value),
    and returns the top 10 months by total value.
    """
    df_with_month = df_hotel_reservations_detailed.withColumn(
        "year_month", date_format(to_date(col("data_reserva")), "yyyy-MM")
    )

    # Group by month and sum the 'valor'
    df_sum_by_month = df_with_month.groupBy("year_month").agg(
        sum("valor").alias("total_valor_reservas_hoteis_mensal")
    )

    # Order by total_valor_reservas_hoteis_mensal descending and select top 10
    return df_sum_by_month.orderBy(col("total_valor_reservas_hoteis_mensal").desc()).limit(10)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TravelDataProcessor").getOrCreate()

    # Create mock dataframes for flights AND hotels
    df_voos, df_reservas_voos, df_hoteis, df_reservas_hoteis = create_mock_data(spark)

    print("--- df_voos Schema and Sample Data ---")
    df_voos.show(5, truncate=False)

    print("\n--- df_reservas_voos Schema and Sample Data ---")
    df_reservas_voos.printSchema()
    df_reservas_voos.show(5, truncate=False)

    # NEW: Show Hotel DataFrames
    print("\n--- df_hoteis Schema and Sample Data ---")
    df_hoteis.printSchema()
    df_hoteis.show(5, truncate=False)

    print("\n--- df_reservas_hoteis Schema and Sample Data ---")
    df_reservas_hoteis.printSchema()
    df_reservas_hoteis.show(5, truncate=False)

    # --- Flight Data Processing ---
    print("\n--- Starting Flight Data Processing ---")

    # 1. Join flight and reservation data (Join Voos)
    df_flight_reservations_detailed = join_flight_and_reservation_data(df_voos, df_reservas_voos)
    print("\n--- df_flight_reservations_detailed (joined) Schema and Sample Data ---")
    df_flight_reservations_detailed.printSchema()
    df_flight_reservations_detailed.show(5, truncate=False)

    # 2. GroupBy (cidade mês, cliente) SUM (valor) for Flights
    df_grouped_by_flight_city_month_client = group_by_city_month_client_sum_value(df_flight_reservations_detailed)
    print("\n--- Grouped by Flight City, Month, Client, Sum of Value ---")
    df_grouped_by_flight_city_month_client.printSchema()
    df_grouped_by_flight_city_month_client.show(5, truncate=False)

    # 3. GroupBy mes Reserva, sum(valor) select 10 for Flights
    df_top_10_monthly_flight_reservations = group_by_month_reservation_sum_value_top_10(df_flight_reservations_detailed)
    print("\n--- Top 10 Monthly Flight Reservations by Total Value ---")
    df_top_10_monthly_flight_reservations.printSchema()
    df_top_10_monthly_flight_reservations.show(truncate=False)


    # --- Hotel Data Processing ---
    print("\n\n--- Starting Hotel Data Processing ---")

    # 1. Join hotel and reservation data (Join Hoteis)
    df_hotel_reservations_detailed = join_hotel_and_reservation_data(df_hoteis, df_reservas_hoteis)
    print("\n--- df_hotel_reservations_detailed (joined) Schema and Sample Data ---")
    df_hotel_reservations_detailed.printSchema()
    df_hotel_reservations_detailed.show(5, truncate=False)

    # 2. GroupBy (cidade mês, cliente) SUM (valor) for Hotels
    df_grouped_by_hotel_city_month_client = group_by_hotel_city_month_sum_value(df_hotel_reservations_detailed)
    print("\n--- Grouped by Hotel City, Month, Client, Sum of Value ---")
    df_grouped_by_hotel_city_month_client.printSchema()
    df_grouped_by_hotel_city_month_client.show(5, truncate=False)

    # 3. GroupBy mes Reserva, sum(valor) select 10 for Hotels
    df_top_10_monthly_hotel_reservations = group_by_month_hotel_reservation_sum_value_top_10(df_hotel_reservations_detailed)
    print("\n--- Top 10 Monthly Hotel Reservations by Total Value ---")
    df_top_10_monthly_hotel_reservations.printSchema()
    df_top_10_monthly_hotel_reservations.show(truncate=False)


    # Stop Spark Session
    spark.stop()