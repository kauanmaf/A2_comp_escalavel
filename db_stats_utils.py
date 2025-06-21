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


def insert_to_staging(conn, staging_table_name, key_columns, value_columns, row):
    """
    Insere dados na tabela de staging correspondente.
    
    Args:
        conn: Conexão PostgreSQL
        staging_table_name: Nome da tabela de staging (com prefixo 'staging_')
        key_columns: Lista de colunas chave (além de company_id)
        value_columns: Lista de colunas de valores
        row: Dicionário com dados da linha
    """
    company_id = row["company_id"]
    key_values = [row[col] for col in key_columns]
    value_values = [row[col] for col in value_columns]

    with conn.cursor() as cursor:
        try:
            # Preparar todas as colunas e valores para inserção
            all_columns = ["company_id"] + key_columns + value_columns
            all_values = [company_id] + key_values + value_values

            # Inserção simples na tabela de staging
            # A coluna 'processed' será FALSE por padrão
            # A coluna 'inserido_em' será preenchida automaticamente com now()
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
            
            cursor.execute(insert_query, all_values)
            
        except Exception as e:
            conn.rollback()
            raise e


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
    na tabela de staging correspondente. Os dados serão processados posteriormente
    pela função processar_todas_estatisticas() agendada via pg_cron.
    
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

    def _insert_partition(rows):
        """
        Função para inserir uma partição de dados na tabela de staging.
        Executa dentro de uma transação para garantir consistência.
        """
        conn = get_pg_connection()
        try:
            conn.autocommit = False  # Garantir que estamos em modo transacional
            
            for row in rows:
                data = row.asDict()
                insert_to_staging(
                    conn=conn,
                    staging_table_name=staging_table_name,
                    key_columns=config["key_columns"],
                    value_columns=config["value_columns"],
                    row=data,
                )

            conn.commit()
            
        except Exception as e:
            conn.rollback()
            # Log do erro para debugging
            print(f"Erro ao inserir dados na staging {staging_table_name}: {str(e)}")
            raise e
        finally:
            conn.close()

    # Reparticiona o DataFrame por company_id para otimizar inserções
    # Isso garante que dados de uma mesma empresa fiquem na mesma partição
    repart_df = df.repartition("company_id")
    
    # Executa inserção em cada partição de forma paralela pelo Spark
    repart_df.foreachPartition(_insert_partition)


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
        print(f"Erro ao verificar status das tabelas de staging: {str(e)}")
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
            print("Processamento manual das estatísticas executado com sucesso")
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"Erro ao executar processamento manual: {str(e)}")
        raise e
    finally:
        conn.close()
