# main.py
import os
import json
import redis
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, to_date, date_trunc, get_json_object, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

# Define o esquema para os dados brutos de viagem
# 'data' é uma string JSON, então a tratamos como StringType inicialmente
raw_data_schema = StructType([
    StructField("company_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("data", StringType(), True),  # Conteúdo JSON interno como string
    StructField("timestamp", TimestampType(), True)
])

# --- Funções Auxiliares Comuns (Integradas) ---
def extract_json_field(df: 'DataFrame', field_name: str, new_column_name: str, data_type: type) -> 'DataFrame':
    """Extrai um campo JSON da coluna 'data' e o adiciona como nova coluna."""
    return df.withColumn(new_column_name, get_json_object(col("data"), f"$.{field_name}").cast(data_type))

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TravelBatchProcessor") \
        .getOrCreate()

    print("SparkSession criada com sucesso para processamento em lote!")

    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

    # Obter e parsear as chaves das listas Redis da variável de ambiente
    # REDIS_LIST_KEYS="raw_travel_data_list_1,raw_travel_data_list_2,raw_travel_data_list_3,raw_travel_data_list_4"
    redis_keys_str = os.getenv('REDIS_LIST_KEYS', 'raw_travel_data_list')
    REDIS_LIST_KEYS = [key.strip() for key in redis_keys_str.split(',')]

    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    all_parsed_data = []
    print(f"Lendo dados das seguintes listas Redis: {REDIS_LIST_KEYS}")
    for current_list_key in REDIS_LIST_KEYS:
        raw_messages = r_client.lrange(current_list_key, 0, -1)
        print(f"Read {len(raw_messages)} messages from Redis list '{current_list_key}'.")
        if raw_messages:
            for msg in raw_messages:
                try:
                    parsed_msg = json.loads(msg)
                    # Handle Z suffix for Python 3.11+ compatibility with fromisoformat
                    # Note: datetime import is needed for fromisoformat
                    parsed_msg['timestamp'] = parsed_msg['timestamp'].replace('Z', '+00:00')
                    parsed_msg['timestamp'] = datetime.datetime.fromisoformat(parsed_msg['timestamp'])
                    all_parsed_data.append(parsed_msg)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from Redis ({current_list_key}): {msg} - {e}")
                except ValueError as e:
                    print(f"Error parsing timestamp from Redis ({current_list_key}): {msg} - {e}")

            # Remove os dados da lista atual após a leitura (processamento batch)
            r_client.ltrim(current_list_key, len(raw_messages), -1)
            print(f"Cleaned {len(raw_messages)} messages from Redis list '{current_list_key}'.")

    if not all_parsed_data:
        print("No new messages to process from any list. Encerrando Spark job.")
        spark.stop()
        exit()

    # Criar um único DataFrame a partir de todos os dados combinados
    df_raw = spark.createDataFrame(all_parsed_data, schema=raw_data_schema)
    print("DataFrame raw criado a partir do Redis.")
    df_raw.show(truncate=False)

    # --- Implementação da Pipeline Spark conforme o diagrama ---

    # 1. Separação de Voos e Hotéis
    df_flight_events = df_raw.filter(col("event_type").contains("flight")) # Inclui "flight_booking", etc.
    df_hotel_events = df_raw.filter(col("event_type").contains("hotel"))   # Inclui "hotel_reservation", "hotel_search", etc.

    print("DataFrame de Eventos de Voos:")
    df_flight_events.show(truncate=False)
    print("DataFrame de Eventos de Hotéis:")
    df_hotel_events.show(truncate=False)

    # Processamento de Voos
    df_flight_bookings = df_flight_events.filter(col("event_type") == "flight_booking") \
                                          .transform(lambda df: extract_json_field(df, "origin", "origin_city", StringType())) \
                                          .transform(lambda df: extract_json_field(df, "dest", "destination_city", StringType())) \
                                          .transform(lambda df: extract_json_field(df, "price", "price", DoubleType()))

    # Agrupamento (Voos)
    # Exemplo: Contagem total de reservas de voos por empresa e por tipo de evento
    flight_grouping_general = df_flight_events.groupBy("company_id", "event_type") \
                                              .agg(count("*").alias("total_events"))
    print("Agrupamento Geral de Voos:")
    flight_grouping_general.show()

    # Agrupamento (Cidades) - Voos
    flight_city_grouping = df_flight_bookings.groupBy("origin_city", "destination_city") \
                                             .agg(count("*").alias("total_flights_between_cities"),
                                                  avg("price").alias("avg_flight_price_route"))
    print("Agrupamento de Voos por Cidades (Rotas):")
    flight_city_grouping.show()

    # Taxa de Ocupação (Voos) - Placeholder
    # Necessitaria de um DataFrame com capacidade total de voos para join
    # df_flight_occupancy = df_flight_bookings.select("company_id", "event_type", "data", "timestamp")
    print("Placeholder para Taxa de Ocupação (Voos) - Lógica a ser implementada.")

    # Cidade + Reservada (Voos)
    most_booked_flight_city = df_flight_bookings.groupBy("destination_city") \
                                                 .agg(count("*").alias("total_bookings")) \
                                                 .orderBy(col("total_bookings").desc()) \
                                                 .limit(1)
    print("Cidade Mais Reservada (Voos):")
    most_booked_flight_city.show()

    # Processamento de Hotéis
    df_hotel_reservations = df_hotel_events.filter(col("event_type") == "hotel_reservation") \
                                           .transform(lambda df: extract_json_field(df, "city", "hotel_city", StringType())) \
                                           .transform(lambda df: extract_json_field(df, "cost", "cost", DoubleType()))

    # Agrupamento (Hotéis)
    hotel_grouping_general = df_hotel_events.groupBy("company_id", "event_type") \
                                            .agg(count("*").alias("total_events"))
    print("Agrupamento Geral de Hotéis:")
    hotel_grouping_general.show()

    # Agrupamento (Cidades) - Hotéis
    hotel_city_grouping = df_hotel_reservations.groupBy("hotel_city") \
                                               .agg(count("*").alias("total_reservations_by_city"),
                                                    avg("cost").alias("avg_hotel_cost_city"))
    print("Agrupamento de Hotéis por Cidades:")
    hotel_city_grouping.show()

    # Taxa de Ocupação (Hotéis) - Placeholder
    # Necessitaria de um DataFrame com capacidade total de hotéis/quartos para join
    # df_hotel_occupancy = df_hotel_reservations.select("company_id", "event_type", "data", "timestamp")
    print("Placeholder para Taxa de Ocupação (Hotéis) - Lógica a ser implementada.")

    # Cidade + Reservada (Hotéis)
    most_booked_hotel_city = df_hotel_reservations.groupBy("hotel_city") \
                                                  .agg(count("*").alias("total_reservations")) \
                                                  .orderBy(col("total_reservations").desc()) \
                                                  .limit(1)
    print("Cidade Mais Reservada (Hotéis):")
    most_booked_hotel_city.show()


    # Filtro (São Paulo) - Aplicado a Voos e Hotéis para caminhos posteriores
    df_sp_relevant_flights = df_flight_bookings.filter(
        (col("origin_city") == "GRU") | (col("destination_city") == "GRU") |
        (col("origin_city") == "CGH") | (col("destination_city") == "CGH")
    ).withColumn("extracted_city", when((col("origin_city") == "GRU") | (col("origin_city") == "CGH"), col("origin_city"))
                               .otherwise(col("destination_city")))

    df_sp_relevant_hotels = df_hotel_reservations.filter(col("hotel_city") == "São Paulo") \
                                                 .withColumn("extracted_city", col("hotel_city"))

    # Agrupamento (Dias)
    # Assumindo que este agrupamento é para dados de SP, e pode ser união de voos e hotéis
    # Você precisará ajustar os esquemas para unionByName se quiser combinar diretamente
    df_sp_all_events = df_sp_relevant_flights.select(
        col("company_id"),
        col("event_type"),
        col("timestamp"),
        col("extracted_city").alias("city"),
        col("price").alias("amount") # Usando 'amount' genérico para valores monetários
    ).unionByName(
        df_sp_relevant_hotels.select(
            col("company_id"),
            col("event_type"),
            col("timestamp"),
            col("extracted_city").alias("city"),
            col("cost").alias("amount")
        )
    )

    daily_grouping_sp = df_sp_all_events.withColumn("event_date", to_date(col("timestamp"))) \
                                        .groupBy("event_date", "city") \
                                        .agg(count("*").alias("total_events_daily_sp"),
                                             avg("amount").alias("avg_amount_daily_sp"))
    print("Agrupamento Diário (São Paulo):")
    daily_grouping_sp.show()

    # Ind. Correção - Placeholder
    # Aplicar alguma lógica de correção ao daily_grouping_sp ou outro DataFrame
    # df_corrected_data = daily_grouping_sp.withColumn("corrected_metric", col("total_events_daily_sp") * 1.05)
    print("Placeholder para Ind. Correção - Lógica a ser implementada.")

    # Média Móvel Ponte Aérea (Filtro Voos e RJ)
    df_ponte_aerea = df_flight_bookings.filter(
        ((col("origin_city") == "GRU") & (col("destination_city") == "SDU")) |
        ((col("origin_city") == "SDU") & (col("destination_city") == "GRU")) |
        ((col("origin_city") == "CGH") & (col("destination_city") == "GIG")) |
        ((col("origin_city") == "GIG") & (col("destination_city") == "CGH"))
    )

    # Calcular média móvel do preço na ponte aérea
    window_spec_ponte_aerea = Window.partitionBy("origin_city", "destination_city").orderBy("timestamp").rowsBetween(-7, 0)
    moving_average_ponte_aerea = df_ponte_aerea.withColumn(
        "moving_avg_price_ponte_aerea",
        avg("price").over(window_spec_ponte_aerea)
    )
    print("Média Móvel Ponte Aérea:")
    moving_average_ponte_aerea.show()

    # Agrupamento (Meses) para as métricas finais
    # Aqui, você precisará coletar as métricas de diferentes ramos e agrupá-las por mês.
    # Isso pode ser complexo e pode exigir junções ou agregação em um DataFrame de fatos.
    # Para simplificar o exemplo, vamos calcular algumas métricas gerais por mês do df_raw e da média móvel.

    df_raw_with_month = df_raw.withColumn("month", date_trunc("month", col("timestamp")))

    # Extrair preço/custo como 'amount' para agregação geral
    df_raw_with_amount = df_raw_with_month.withColumn(
        "amount",
        when(col("event_type").contains("flight"), get_json_object(col("data"), "$.price").cast(DoubleType()))
        .when(col("event_type").contains("hotel"), get_json_object(col("data"), "$.cost").cast(DoubleType()))
        .otherwise(None)
    )

    # Agregação para Ticket Médio Mês, Maior/Menor Ticket Médio
    monthly_metrics = df_raw_with_amount.groupBy("month") \
                                        .agg(
                                            avg("amount").alias("ticket_medio_mes"),
                                            max("amount").alias("maior_ticket_medio"),
                                            min("amount").alias("menor_ticket_medio")
                                        )
    print("Métricas Mensais:")
    monthly_metrics.show()

    # Taxa de Turismo Local - Placeholder
    # df_tourism_local_rate = df_raw_with_month.groupBy("month") ... (lógica de turismo local)
    print("Placeholder para Taxa de Turismo Local - Lógica a ser implementada.")

    # Você precisará de lógica para juntar moving_average_ponte_aerea e outros resultados
    # no dataframe mensal se quiser que tudo apareça na mesma tabela final.
    # Exemplo (se a média móvel fosse por mês):
    # moving_average_ponte_aerea_monthly = moving_average_ponte_aerea.withColumn("month", date_trunc("month", col("timestamp"))) \
    #                                                               .groupBy("month") \
    #                                                               .agg(avg("moving_avg_price_ponte_aerea").alias("avg_ponte_aerea_monthly"))

    # monthly_final_report = monthly_metrics.join(moving_average_ponte_aerea_monthly, on="month", how="left")

    # Armazenar estatísticas finais
    output_stats_path = "/tmp/processed_final_stats_batch"
    df_monthly_summary_output = monthly_metrics # Usando as métricas mensais como exemplo de saída
    df_monthly_summary_output.write.mode("append").parquet(output_stats_path)
    print(f"Estatísticas finais salvas em: {output_stats_path}")

    print("Spark Batch job concluído.")
    spark.stop()
    print("SparkSession encerrada.")