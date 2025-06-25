import os
import threading

import pandas as pd
import psycopg2
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine
import time

# Configuração da aplicação FastAPI
app = FastAPI(
    title="Stats API",
    description="API para consulta de estatísticas de hotéis e voos",
    version="1.0.0",
)

# Configuração do banco de dados
DB_CONFIG = {
    "host": os.getenv("PG_STATS_HOST", "a2-comp-escalavel-dados-estatisticas.col2wfyf2csx.us-east-1.rds.amazonaws.com"),
    "port": int(os.getenv("PG_STATS_PORT", 5432)),
    "database": os.getenv("PG_STATS_DB", "postgres"),
    "user": os.getenv("PG_STATS_USER", "A2CompEscalavel"),
    "password": os.getenv("PG_STATS_PASSWORD", "euadoroaemap"),
}

def get_db_connection():
    """Cria conexão com o banco de dados PostgreSQL"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao conectar com o banco: {str(e)}"
        )

def get_sqlalchemy_engine():
    """Cria uma engine SQLAlchemy para o banco de dados PostgreSQL"""
    db_url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(db_url)

def query_to_dicts(query, params=None):
    engine = get_sqlalchemy_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params=params)
        return df.to_dict(orient="records")

@app.get("/health")
async def health_check():
    try:
        conn = get_db_connection()
        conn.close()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao conectar com o banco: {str(e)}"
        )
    return {"status": "healthy", "service": "stats-api"}


STATS_TABLES = {
    "month_hotel": "stats_month_hotel",
    "city_hotel": "stats_city_hotel",
    "month_voos": "stats_month_voos",
    "city_voos": "stats_city_voos",
    "faturamentos_totais": "stats_faturamentos_totais",
    "ticket_medio": "stats_ticket_medio",
    "month_hotel_count": "stats_month_hotel_count",
    "stars_hotel": "stats_stars_hotel",
    "estrelas_medias_mes": "stats_estrelas_medias_mes",
    "month_sp_voos": "stats_month_sp_voos",
    "day_sp_voos": "stats_day_sp_voos"
}

STAGING_TABLES = {
    "month_hotel": "staging_stats_month_hotel",
    "city_hotel": "staging_stats_city_hotel",
    "month_voos": "staging_stats_month_voos",
    "city_voos": "staging_stats_city_voos",
    "faturamentos_totais": "staging_stats_faturamentos_totais",
    "ticket_medio": "staging_stats_ticket_medio",
    "month_hotel_count": "staging_stats_month_hotel_count",
    "stars_hotel": "staging_stats_stars_hotel",
    "estrelas_medias_mes": "staging_stats_estrelas_medias_mes",
    "month_sp_voos": "staging_stats_month_sp_voos",
    "day_sp_voos": "staging_stats_day_sp_voos"
}

@app.get("/stats/{table_name}", tags=["stats"])
def get_stats_table(table_name: str):
    if table_name not in STATS_TABLES:
        raise HTTPException(status_code=404, detail="Tabela não encontrada")
    real_table = STATS_TABLES[table_name]
    query = f"SELECT * FROM {real_table}"
    return query_to_dicts(query)


@app.get("/stats/{table_name}/{company_id}", tags=["stats"])
def get_stats_table_by_company(table_name: str, company_id: str):
    if table_name not in STATS_TABLES:
        raise HTTPException(status_code=404, detail="Tabela não encontrada")
    real_table = STATS_TABLES[table_name]
    query = f"SELECT * FROM {real_table} WHERE company_id = %s"
    return query_to_dicts(query, (company_id,))


@app.get("/staging/{table_name}", tags=["staging"])
def get_staging_table(table_name: str):
    if table_name not in STAGING_TABLES:
        raise HTTPException(status_code=404, detail="Tabela de staging não encontrada")
    real_table = STAGING_TABLES[table_name]
    query = f"SELECT * FROM {real_table}"
    return query_to_dicts(query)

@app.get("/clientes", tags=["clientes"])
def get_clientes():
    tables = [
        "stats_month_hotel", "stats_month_voos"
    ]
    clientes = set()
    engine = get_sqlalchemy_engine()
    with engine.connect() as conn:
        for t in tables:
            df = pd.read_sql_query(f"SELECT DISTINCT company_id FROM {t}", conn)
            clientes.update(df["company_id"].dropna().tolist())
    return {"clientes": sorted(list(clientes))}

@app.get("/update_stats", tags=["admin"])
def update_stats():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT processar_todas_estatisticas();")
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar estatísticas: {str(e)}")
    finally:
        conn.close()
    return {"status": "success", "message": "Estatísticas atualizadas com sucesso"}

@app.get("/spark_logs", tags=["logs"])
def get_spark_logs(
    limit: int = Query(100, ge=1, le=1000, description="Limite de registros retornados"),
    order: str = Query("desc", pattern="^(asc|desc)$", description="Ordenação por data de início (asc/desc)")
):
    order_sql = "DESC" if order.lower() == "desc" else "ASC"
    query = f"""
        SELECT id, start_time, end_time, rows_hotels, rows_flights,
               spark_duration_seconds, db_write_duration_seconds, created_at
        FROM pipeline_execution_log
        ORDER BY start_time {order_sql}
        LIMIT %s
    """
    return query_to_dicts(query, (limit,))

def start_stats_processor():
    def run():
        interval = int(os.getenv("INTERVAL_SECONDS", 20))
        while True:
            try:
                print("[stats-processor] Executando processar_todas_estatisticas()...")
                conn = get_db_connection()
                with conn.cursor() as cursor:
                    cursor.execute("SELECT processar_todas_estatisticas();")
                    conn.commit()
                conn.close()
            except Exception as e:
                print(f"[stats-processor] Erro: {e}")
            time.sleep(interval)

    t = threading.Thread(target=run, daemon=True)
    t.start()

@app.on_event("startup")
def on_startup():
    if __name__ == "__main__" or os.getenv("RUN_STATS_PROCESSOR", "1") == "1":
        start_stats_processor()

if __name__ == "__main__":
    start_stats_processor()
    uvicorn.run(app, host="0.0.0.0", port=8000)
