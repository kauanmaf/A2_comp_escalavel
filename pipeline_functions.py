from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, to_timestamp

def join(df1, df2, id):
    df_joined = df1.join(df2, on = id)
    return df_joined

def groupby_stats_hotels(df_joined_hotel):
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

def groupby_city_hotels(df_stats_hotels):
    df_grouped_city = df_stats_hotels.groupBy(
        "company_id",
        "cidade",
    ).agg(
        sum("sum_valor").alias("sum_valor")
    ).orderBy(
        "company_id", "sum_valor"
    )

    return df_grouped_city

def groupby_month_hotels(df_stats_hotels):
    df_grouped_month = df_stats_hotels.groupBy(
        "company_id",
        "mes_reserva",
    ).agg(
        sum("sum_valor").alias("sum_valor")
    ).orderBy(
        "company_id", "sum_valor"
    )

    return df_grouped_month