import os
import psycopg2
from psycopg2 import sql


def get_pg_connection():
    """Cria conexão com o PostgreSQL usando variáveis de ambiente"""
    return psycopg2.connect(
        host=os.getenv("PG_STATS_HOST", "postgres-stats"),
        port=os.getenv("PG_STATS_PORT", "5432"),
        dbname=os.getenv("PG_STATS_DB", "dados_stats"),
        user=os.getenv("PG_STATS_USER", "emap"),
        password=os.getenv("PG_STATS_PASSWORD", "emap123"),
    )


# Mapeamento de tabelas e suas configurações (agora mapeando para staging)
STAGING_TABLE_CONFIG = {
    "staging_stats_month_hotel": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["sum_valor"],
    },
    "staging_stats_city_hotel": {
        "key_columns": ["cidade"], 
        "value_columns": ["sum_valor"]
    },
    "staging_stats_month_voos": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["sum_valor"],
    },
    "staging_stats_city_voos": {
        "key_columns": ["cidade_destino"],
        "value_columns": ["sum_valor"],
    },
    "staging_stats_faturamentos_totais": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["total_valor"],
    },
    "staging_stats_ticket_medio": {
        "key_columns": ["cidade_destino"],
        "value_columns": ["sum_valor", "num_transacoes"],
    },
    "staging_stats_stars_hotel": {
        "key_columns": ["estrelas"],
        "value_columns": ["num_reservas"],
    },
    "staging_stats_estrelas_medias_mes": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["sum_estrelas", "num_reservas"],
    },
    "staging_stats_month_sp_voos": {
        "key_columns": ["mes"],
        "value_columns": ["num_voos_reservados"],
    },
    "staging_stats_day_sp_voos": {
        "key_columns": ["data_reserva"],
        "value_columns": ["num_reservas"],
    },
}

# Mapeamento das tabelas originais para staging
TABLE_TO_STAGING = {
    "stats_month_hotel": "staging_stats_month_hotel",
    "stats_city_hotel": "staging_stats_city_hotel", 
    "stats_month_voos": "staging_stats_month_voos",
    "stats_city_voos": "staging_stats_city_voos",
    "stats_faturamentos_totais": "staging_stats_faturamentos_totais",
    "stats_ticket_medio": "staging_stats_ticket_medio",
    "stats_stars_hotel": "staging_stats_stars_hotel",
    "stats_estrelas_medias_mes": "staging_stats_estrelas_medias_mes",
    "stats_month_sp_voos": "staging_stats_month_sp_voos",
    "stats_day_sp_voos": "staging_stats_day_sp_voos",
}


def save_stats_dataframe(df, table_name):
    """
    Recebe um DataFrame do Spark contendo estatísticas processadas e insere os dados
    na tabela de staging correspondente de forma otimizada usando inserção em lote.
    
    Args:
        df: DataFrame do Spark com os dados processados
        table_name: Nome da tabela original (ex: 'stats_month_hotel')
                   Será convertido automaticamente para staging (ex: 'staging_stats_month_hotel')
    """
    # Converte o nome da tabela original para staging
    staging_table_name = TABLE_TO_STAGING.get(table_name)
    if not staging_table_name:
        raise ValueError(f"Mapeamento não encontrado para tabela: {table_name}")
    
    # Obtém a configuração da tabela de staging
    config = STAGING_TABLE_CONFIG.get(staging_table_name)
    if not config:
        raise ValueError(f"Configuração não encontrada para tabela de staging: {staging_table_name}")

    # Coleta todos os dados do DataFrame para Python
    rows_data = df.collect()
    
    if not rows_data:
        # print(f"Nenhum dado para inserir na tabela {staging_table_name}", flush=True)
        return
    
    # Prepara os dados para inserção em lote
    all_columns = ["company_id"] + config["key_columns"] + config["value_columns"]
    batch_data = []
    
    for row in rows_data:
        data = row.asDict()
        company_id = data["company_id"]
        key_values = [data[col] for col in config["key_columns"]]
        value_values = [data[col] for col in config["value_columns"]]
        batch_data.append([company_id] + key_values + value_values)
    
    # Insere todos os dados em uma única transação
    conn = get_pg_connection()
    try:
        conn.autocommit = False
        
        with conn.cursor() as cursor:
            # Inserção em lote usando executemany para máxima performance
            insert_query = sql.SQL(
                """
                INSERT INTO {staging_table} ({fields})
                VALUES ({placeholders})
                """
            ).format(
                staging_table=sql.Identifier(staging_table_name),
                fields=sql.SQL(", ").join(map(sql.Identifier, all_columns)),
                placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(all_columns))
            )
            
            cursor.executemany(insert_query, batch_data)
            
        conn.commit()
        # print(f"Inseridos {len(batch_data)} registros na tabela {staging_table_name}", flush=True)
        
    except Exception as e:
        conn.rollback()
        # print(f"Erro ao inserir dados na staging {staging_table_name}: {str(e)}", flush=True)
        raise e
    finally:
        conn.close()


def get_staging_status():
    """
    Retorna o status das tabelas de staging - quantos registros estão pendentes de processamento.
    Útil para monitoramento e debug.
    
    Returns:
        dict: Dicionário com o nome da tabela e quantidade de registros não processados
    """
    conn = get_pg_connection()
    status = {}
    
    try:
        with conn.cursor() as cursor:
            for staging_table in STAGING_TABLE_CONFIG.keys():
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {table} WHERE processed = FALSE").format(
                        table=sql.Identifier(staging_table)
                    )
                )
                count = cursor.fetchone()[0]
                status[staging_table] = count
                
    except Exception as e:
        print(f"Erro ao verificar status das tabelas de staging: {str(e)}", flush=True)
        raise e
    finally:
        conn.close()
    
    return status


def force_process_staging():
    """
    Força o processamento das tabelas de staging executando a função 
    processar_todas_estatisticas() manualmente.
    
    Útil para testes e processamento imediato quando necessário.
    
    Returns:
        bool: True se executado com sucesso
    """
    conn = get_pg_connection()
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT processar_todas_estatisticas()")
            conn.commit()
            # print("Processamento manual das estatísticas executado com sucesso", flush=True)
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"Erro ao executar processamento manual: {str(e)}", flush=True)
        raise e
    finally:
        conn.close()


def save_pipeline_execution_log(
    start_time,
    end_time,
    rows_hotels,
    rows_flights,
    spark_duration_seconds,
    db_write_duration_seconds
):
    conn = get_pg_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_execution_log (
                    start_time, end_time, rows_hotels, rows_flights,
                    spark_duration_seconds, db_write_duration_seconds
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    start_time,
                    end_time,
                    rows_hotels,
                    rows_flights,
                    spark_duration_seconds,
                    db_write_duration_seconds,
                ),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro ao salvar log de execução da pipeline: {str(e)}", flush=True)
        raise e
    finally:
        conn.close()
