from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, date_format, count, sum, avg
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random

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


def group_by_flight_city_month_client_sum_value(df_flight_reservations_detailed: DataFrame) -> DataFrame:
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

def group_by_flight_month_reservation_sum_value_top_10(df_flight_reservations_detailed: DataFrame) -> DataFrame:
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

def calculate_hotel_reservations_by_stars_moving_average(df_hotel_reservations_detailed: DataFrame) -> DataFrame:
    """
    Calculates the moving average number of hotel reservations by hotel star rating.
    """
    # Cast data_reserva to date type for window ordering
    df_with_date = df_hotel_reservations_detailed.withColumn("data_reserva_date", to_date(col("data_reserva")))

    # Define window for moving average: partition by stars, order by reservation date, 7-day window
    window_spec = Window.partitionBy("estrelas").orderBy("data_reserva_date").rowsBetween(-7, 0)

    # Calculate moving average of total reservations for each star rating
    df_moving_avg_reservations = df_with_date.groupBy("estrelas", "data_reserva_date") \
                                            .agg(count("id_reserva_hotel").alias("daily_reservations")) \
                                            .withColumn("moving_avg_reservations_7_day", 
                                                        avg("daily_reservations").over(window_spec))
    return df_moving_avg_reservations

def get_hotel_reservations_by_month(df_hotel_reservations_detailed: DataFrame) -> DataFrame:
    """
    Counts monthly hotel reservations.
    """
    df_with_month = df_hotel_reservations_detailed.withColumn(
        "year_month", date_format(to_date(col("data_reserva")), "yyyy-MM")
    )
    return df_with_month.groupBy("year_month").agg(
        count("id_reserva_hotel").alias("total_hotel_reservations_monthly")
    )


def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("TravelDataProcessing") \
        .getOrCreate()

    # Create mock data
    df_voos, df_reservas_voos, df_hoteis, df_reservas_hoteis = create_mock_data(spark)


    # Join Voos and Res. Voos
    df_joined_flights = join_flight_and_reservation_data(df_voos, df_reservas_voos)
    print("Joined Flight and Reservation Data:")
    df_joined_flights.show(5)

    # GroupBy (cidade mês, cliente) SUM (valor)
    df_grouped_flight_value = group_by_flight_city_month_client_sum_value(df_joined_flights)
    print("Grouped Flight Value by City, Month, Client:")
    df_grouped_flight_value.show(5)

    # GroupBy mes Reserva, sum(valor) select 10
    df_top_10_flight_months = group_by_flight_month_reservation_sum_value_top_10(df_joined_flights)
    print("Top 10 Flight Reservation Months by Value:")
    df_top_10_flight_months.show(5)

    # Filter (São Paulo)
    df_sp_filtered_flights = filter_sao_paulo_data(df_joined_flights)
    print("São Paulo Filtered Flight Data:")
    df_sp_filtered_flights.show(5)

    # N° Reservas por mês (from São Paulo filtered data)
    df_sp_monthly_reservations = get_monthly_grouping_sp(df_sp_filtered_flights)
    print("Monthly Flight Reservations for São Paulo:")
    df_sp_monthly_reservations.show(5)

    # Filter (Voos e RJ) - This seems to imply "Ponte Aérea" logic from your diagram
    # This leads to "Média Móvel Ponte Aérea"
    df_ponte_aerea_moving_avg = calculate_ponte_aerea_moving_average(df_joined_flights)
    print("Moving Average Price for Ponte Aérea:")
    df_ponte_aerea_moving_avg.show(5)


    # --- Hotel Data Processing (Right side of the diagram, "Repete do outro lado") ---

    # Join Hoteis and Res. Hoteis
    df_joined_hoteis = join_hotel_and_reservation_data(df_hoteis, df_reservas_hoteis)
    print("Joined Hotel and Reservation Data:")
    df_joined_hoteis.show(5)

    # Agrupamento (Cidades) - Equivalent to GroupBy (cidade mês, cliente) SUM (valor) for hotels
    df_grouped_hotel_value = group_by_hotel_city_month_sum_value(df_joined_hoteis)
    print("Grouped Hotel Value by City, Month, Client:")
    df_grouped_hotel_value.show(5)

    # Mês + Reservado - Equivalent to GroupBy mes Reserva, sum(valor) select 10 for hotels
    df_top_10_hotel_months = group_by_month_hotel_reservation_sum_value_top_10(df_joined_hoteis)
    print("Top 10 Hotel Reservation Months by Value:")
    df_top_10_hotel_months.show(5)

    # N° Reservas por estrela -> Estrelas -> Média Móvel N° Reservas
    # This implies a calculation of reservations grouped by stars, then a moving average on that.
    df_moving_avg_hotel_reservations_by_stars = calculate_hotel_reservations_by_stars_moving_average(df_joined_hoteis)
    print("Moving Average of Hotel Reservations by Star Rating:")
    df_moving_avg_hotel_reservations_by_stars.show(5)

    # N° Reservas por mês (for Hotels)
    df_hotel_monthly_reservations = get_hotel_reservations_by_month(df_joined_hoteis)
    print("Monthly Hotel Reservations:")
    df_hotel_monthly_reservations.show(5)


    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()