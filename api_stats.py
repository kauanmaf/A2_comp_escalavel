from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
import os
from datetime import date
import uvicorn

# Configuração da aplicação FastAPI
app = FastAPI(
    title="Stats API",
    description="API para consulta de estatísticas de hotéis e voos",
    version="1.0.0",
)

# Configuração do banco de dados
DB_CONFIG = {
    "host": os.getenv("PG_STATS_HOST", "postgres-stats"),
    "port": int(os.getenv("PG_STATS_PORT", 5432)),
    "database": os.getenv("PG_STATS_DB", "dados_stats"),
    "user": os.getenv("PG_STATS_USER", "emap"),
    "password": os.getenv("PG_STATS_PASSWORD", "emap123"),
}

def get_db_connection():
    """Cria conexão com o banco de dados PostgreSQL"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao conectar com o banco: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """Endpoint para verificar se a API está funcionando"""
    return {"status": "healthy", "service": "stats-api"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
