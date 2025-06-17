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

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TravelDataProcessor").getOrCreate()

    # Create mock dataframes
    df_voos, df_reservas_voos, df_hotel_reservations = create_mock_data(spark)

    print("--- df_voos Schema and Sample Data ---")
    df_voos.show(5, truncate=False)

    print("\n--- df_reservas_voos Schema and Sample Data ---")
    df_reservas_voos.printSchema()
    df_reservas_voos.show(5, truncate=False)

    # 1. Join flight and reservation data (Join Voos)
    df_flight_reservations_detailed = join_flight_and_reservation_data(df_voos, df_reservas_voos)
    print("\n--- df_flight_reservations_detailed (joined) Schema and Sample Data ---")
    df_flight_reservations_detailed.printSchema()
    df_flight_reservations_detailed.show(5, truncate=False)

    # 2. GroupBy (cidade mês, cliente) SUM (valor)
    df_grouped_by_city_month_client = group_by_city_month_client_sum_value(df_flight_reservations_detailed)
    print("\n--- Grouped by City, Month, Client, Sum of Value ---")
    df_grouped_by_city_month_client.printSchema()
    df_grouped_by_city_month_client.show(5, truncate=False)

    # 3. GroupBy mes Reserva, sum(valor) select 10
    df_top_10_monthly_reservations = group_by_month_reservation_sum_value_top_10(df_flight_reservations_detailed)
    print("\n--- Top 10 Monthly Reservations by Total Value ---")
    df_top_10_monthly_reservations.printSchema()
    df_top_10_monthly_reservations.show(truncate=False)

    # Stop Spark Session
    spark.stop()