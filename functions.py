# spark_processor_functions.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, to_date, when, lit, date_format
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
    hotels_data = [] # Keeping hotels for completeness as it was in original code

    companies = ["CiaViagemA", "CiaVoosB", "AgenciaTurC"]

    # Updated DADOS_VOOS_FIXOS with cidade_origem and cidade_destino
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

    DADOS_HOTEIS_FIXOS = [
        {"id_hotel": 1, "cidade": "Rio de Janeiro", "estrelas": 5},
        {"id_hotel": 2, "cidade": "Salvador", "estrelas": 4},
        {"id_hotel": 3, "cidade": "Porto Alegre", "estrelas": 4},
        {"id_hotel": 4, "cidade": "São Paulo", "estrelas": 5},
        {"id_hotel": 5, "cidade": "Recife", "estrelas": 3},
    ]

    reserva_voo_id_counter = 1
    start_date = datetime(2025, 10, 1) # Start date for flight reservations
    for i in range(50): # Generate some mock data
        voo_selecionado = random.choice(DADOS_VOOS_FIXOS)
        
        company_id_flight_res = random.choice(companies)
        valor_reserva = round(random.uniform(300, 2500), 2)
        
        # Simulate reservations over a period
        current_date = start_date + timedelta(days=i % 10) # distribute reservations over 10 days
        data_reserva = current_date.isoformat()

        reservas_voos_data.append({
            "id_reserva_voo": reserva_voo_id_counter,
            "company_id": company_id_flight_res,
            "id_voo": voo_selecionado['id_voo'],
            "valor": valor_reserva,
            "data_reserva": data_reserva,
            "event_type": "flight_reservation"
        })
        reserva_voo_id_counter += 1

        company_id_hotel = random.choice(companies)
        hotel_selecionado = random.choice(DADOS_HOTEIS_FIXOS)
        reserved_date = datetime(
            2025,
            random.randint(1, 12),
            random.randint(1, 28),
            random.randint(10, 22),
            random.choice([0, 30])
        )
        hotel_payload = {
            "id_hotel": hotel_selecionado['id_hotel'],
            "valor": round(random.uniform(150, 2000), 2),
            "data_reservada": reserved_date.isoformat()
        }
        hotels_data.append({
            "company_id": company_id_hotel,
            "data": json.dumps(hotel_payload),
            "timestamp": datetime.now().isoformat(),
            "event_type": "hotel_reservation",
            "id_hotel": hotel_selecionado['id_hotel'],
            "hotel_city": hotel_selecionado['cidade'],
            "estrelas": hotel_selecionado['estrelas'],
            "cost": hotel_payload['valor'],
            "data_reservada": hotel_payload['data_reservada']
        })

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

    df_hotel_reservations = spark_session.createDataFrame(hotels_data).select(
        col("company_id").cast(StringType()),
        col("data").cast(StringType()),
        col("timestamp").cast(StringType()),
        col("event_type").cast(StringType()),
        col("id_hotel").cast(IntegerType()),
        col("hotel_city").cast(StringType()),
        col("estrelas").cast(IntegerType()),
        col("cost").cast(DoubleType()),
        col("data_reservada").cast(StringType())
    )
    return df_voos, df_reservas_voos, df_hotel_reservations

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

def filter_sao_paulo_data(df_flight_reservations_detailed: DataFrame) -> DataFrame:
    """
    Filters flight data relevant to São Paulo (origin or destination), assuming the flight
    and reservation data are already joined.
    """
    # Filter for flights where either 'cidade_origem' or 'cidade_destino' is 'São Paulo'
    df_sp_relevant_flights = df_flight_reservations_detailed.filter(
        (col("cidade_origem") == "São Paulo") | (col("cidade_destino") == "São Paulo")
    )
    return df_sp_relevant_flights

def get_daily_grouping_sp(df_sp_relevant_flights: DataFrame) -> DataFrame:
    """
    Counts daily flights arriving/departing São Paulo, segregated by company.
    This function expects a pre-filtered DataFrame containing only São Paulo relevant flights.
    """
    # Ensure 'event_date' is derived from 'data_reserva' for grouping
    df_with_event_date = df_sp_relevant_flights.withColumn(
        "event_date", to_date(col("data_reserva"))
    )

    # Group by event_date and company_id to count total flights
    return df_with_event_date.groupBy("event_date", "company_id") \
                             .agg(count("*").alias("total_flights_daily_sp"))

def get_monthly_grouping_sp(df_sp_relevant_flights: DataFrame) -> DataFrame:
    """
    Counts monthly flights arriving/departing São Paulo, segregated by company.
    """
    # Extract year-month string (e.g., '2025-10') from 'data_reserva' for monthly grouping
    df_with_month = df_sp_relevant_flights.withColumn(
        "year_month", date_format(to_date(col("data_reserva")), "yyyy-MM")
    )

    # Group by year_month and company_id to count total flights for the month
    return df_with_month.groupBy("year_month", "company_id") \
                        .agg(count("*").alias("total_flights_monthly_sp"))

def calculate_ponte_aerea_moving_average(df_flight_reservations_detailed: DataFrame) -> DataFrame:
    """
    Calculates the moving average price for "Ponte Aérea" flights (São Paulo <-> Rio de Janeiro), segregated by client.
    It takes the already joined flight and reservation details as input.
    """
    # Filter for "Ponte Aérea" flights from the detailed flight reservations DataFrame
    # Corrected filter to ensure consistency in "Rio de Janeiro" spelling
    df_ponte_aerea = df_flight_reservations_detailed.filter(
        ((col("cidade_origem") == "São Paulo") & (col("cidade_destino") == "Rio de Janeiro")) |
        ((col("cidade_origem") == "Rio de Janeiro") & (col("cidade_destino") == "São Paulo"))
    )

    # Use 'data_reserva' for ordering in the window function, as this is the event timestamp
    window_spec_ponte_aerea = Window.partitionBy("company_id", "cidade_origem", "cidade_destino").orderBy(to_date("data_reserva")).rowsBetween(-7, 0)
    
    moving_average_ponte_aerea = df_ponte_aerea.withColumn(
        "moving_avg_price_ponte_aerea",
        avg("valor").over(window_spec_ponte_aerea) # Use 'valor' from reservas_voos
    )
    return moving_average_ponte_aerea

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TravelDataProcessor").getOrCreate()

    # Create mock dataframes
    df_voos, df_reservas_voos, df_hotel_reservations = create_mock_data(spark)

    print("--- df_voos Schema and Sample Data ---")
    df_voos.show(5, truncate=False) # Changed printSchema() to show() for brevity in output

    print("\n--- df_reservas_voos Schema and Sample Data ---")
    df_reservas_voos.printSchema()
    df_reservas_voos.show(5, truncate=False)

    print("\n--- df_hotel_reservations Schema and Sample Data ---")
    df_hotel_reservations.printSchema()
    df_hotel_reservations.show(5, truncate=False)

    # 1. Join flight and reservation data
    df_flight_reservations_detailed = join_flight_and_reservation_data(df_voos, df_reservas_voos)
    print("\n--- df_flight_reservations_detailed (joined) Schema and Sample Data ---")
    df_flight_reservations_detailed.printSchema()
    df_flight_reservations_detailed.show(5, truncate=False)

    # 2. Filter São Paulo relevant flights using your simplified function
    df_sp_filtered_flights = filter_sao_paulo_data(df_flight_reservations_detailed)
    print("\n--- df_sp_filtered_flights (São Paulo only) Schema and Sample Data ---")
    df_sp_filtered_flights.printSchema()
    df_sp_filtered_flights.show(5, truncate=False)

    # 3. Daily Grouping for São Paulo Events
    df_daily_grouping_sp = get_daily_grouping_sp(df_sp_filtered_flights)

    print("\n--- Daily Grouping for São Paulo Flights (by client) ---")
    df_daily_grouping_sp.show(truncate=False)

    # 4. Monthly Grouping for São Paulo Events
    # Call the new monthly grouping function
    df_monthly_grouping_sp = get_monthly_grouping_sp(df_sp_filtered_flights)

    print("\n--- Monthly Grouping for São Paulo Flights (by client) ---")
    df_monthly_grouping_sp.show(truncate=False)

    # 5. Moving Average for Ponte Aérea
    # Use the df_flight_reservations_detailed as input for ponte aerea calculation
    # as it needs all flight reservation details, not just SP-filtered ones
    df_ponte_aerea_moving_average = calculate_ponte_aerea_moving_average(df_flight_reservations_detailed)

    print("\n--- Moving Average Price for Ponte Aérea Flights (by client) ---")
    df_ponte_aerea_moving_average.show(truncate=False)

    # Stop Spark Session
    spark.stop()