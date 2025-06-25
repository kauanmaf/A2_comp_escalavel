import os
import psycopg2
from psycopg2 import sql


def get_pg_connection():
    """Cria conexão com o PostgreSQL usando variáveis de ambiente"""
    return psycopg2.connect(
        host=os.getenv("PG_STATS_HOST", "a2-comp-escalavel-dados-estatisticas.col2wfyf2csx.us-east-1.rds.amazonaws.com"),
        port=os.getenv("PG_STATS_PORT", "5432"),
        dbname=os.getenv("PG_STATS_DB", "postgres"),
        user=os.getenv("PG_STATS_USER", "A2CompEscalavel"),
        password=os.getenv("PG_STATS_PASSWORD", "euadoroaemap"),
    )


def upsert_statistics(conn, table_name, key_columns, value_columns, row):
    """
    Realiza UPSERT com lock por company_id

    Args:
        conn: Conexão PostgreSQL
        table_name: Nome da tabela
        key_columns: Lista de colunas chave (além de company_id)
        value_columns: Lista de colunas de valores para atualizar
        row: Dicionário com dados da linha
    """
    company_id = row["company_id"]
    key_values = [row[col] for col in key_columns]
    value_values = [row[col] for col in value_columns]

    # Adquire lock por company_id para evitar concorrência
    with conn.cursor() as cursor:
        try:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (company_id,))

            # Inserção com ON CONFLICT para upsert eficiente
            all_columns = ["company_id"] + key_columns + value_columns
            all_values = [company_id] + key_values + value_values

            insert_query = sql.SQL(
                """
                INSERT INTO {table} ({fields})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {update_assignments}
            """
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(", ").join(map(sql.Identifier, all_columns)),
                placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(all_columns)),
                conflict_cols=sql.SQL(", ").join(
                    map(sql.Identifier, ["company_id"] + key_columns)
                ),
                update_assignments=sql.SQL(", ").join(
                        sql.Identifier(col)
                        + sql.SQL(" = COALESCE(")
                        + sql.Identifier(table_name)  # <--- Re-add table_name qualifier here
                        + sql.SQL(".")
                        + sql.Identifier(col)
                        + sql.SQL(", 0) + EXCLUDED.")
                        + sql.Identifier(col)
                        for col in value_columns
                    ),
            )
            cursor.execute(insert_query, all_values)
        except Exception as e:
            conn.rollback()
            raise e


# Mapeamento de tabelas e suas configurações
TABLE_CONFIG = {
    "stats_month_hotel": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["sum_valor"],
    },
    "stats_city_hotel": {"key_columns": ["cidade"], "value_columns": ["sum_valor"]},
    "stats_month_voos": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["sum_valor"],
    },
    "stats_city_voos": {
        "key_columns": ["cidade_destino"],
        "value_columns": ["sum_valor"],
    },
    "stats_faturamentos_totais": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["total_valor"],
    },
    "stats_ticket_medio": {
        "key_columns": ["cidade_destino"],
        "value_columns": ["sum_valor", "num_transacoes"],
    },
    "stats_stars_hotel": {
        "key_columns": ["estrelas"],
        "value_columns": ["num_reservas"],
    },
    "stats_estrelas_medias_mes": {
        "key_columns": ["mes_reserva"],
        "value_columns": ["sum_estrelas", "num_reservas"],
    },
    "stats_month_sp_voos": {
        "key_columns": ["mes"],
        "value_columns": ["num_voos_reservados"],
    },
    "stats_day_sp_voos": {
        "key_columns": ["data_reserva"],
        "value_columns": ["num_reservas"],
    },
}


def save_stats_dataframe(df, table_name):
    """
    Recebe um DataFrame do Spark contendo estatísticas processadas e faz upsert na tabela Postgres indicada.
    Se a linha não existir, insere; caso exista, atualiza somando o novo valor ao existente.
    Usa Spark foreachPartition para execução eficiente e advisory lock por company_id.
    """
    config = TABLE_CONFIG.get(table_name)
    if not config:
        raise ValueError(f"Configuração não encontrada para tabela: {table_name}")

    def _upsert_partition(rows):
        conn = get_pg_connection()
        try:
            for row in rows:
                data = row.asDict()
                upsert_statistics(
                    conn=conn,
                    table_name=table_name,
                    key_columns=config["key_columns"],
                    value_columns=config["value_columns"],
                    row=data,
                )

            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    # Reparticiona o DataFrame por company_id para garantir que cada partição contenha dados de uma mesma company_id
    repart_df = df.repartition("company_id")
    # Executa upsert em cada partição de forma paralela pelo Spark
    repart_df.foreachPartition(_upsert_partition)
