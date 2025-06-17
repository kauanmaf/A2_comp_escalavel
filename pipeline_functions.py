from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, date_format, count, sum, avg, month
from datetime import datetime, timedelta

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