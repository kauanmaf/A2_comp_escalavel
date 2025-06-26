#!/bin/bash
set -e

# Aguarda o PostgreSQL estar pronto
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  sleep 2
done

# Cria as tabelas básicas para dados gerais se não existirem
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Tabela para hotéis
    CREATE TABLE IF NOT EXISTS hoteis (
        id SERIAL PRIMARY KEY,
        company_id TEXT NOT NULL,
        hotel_name TEXT NOT NULL,
        cidade TEXT NOT NULL,
        valor DOUBLE PRECISION NOT NULL,
        data_reserva DATE NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Tabela para voos
    CREATE TABLE IF NOT EXISTS voos (
        id SERIAL PRIMARY KEY,
        company_id TEXT NOT NULL,
        airline_name TEXT NOT NULL,
        cidade_origem TEXT NOT NULL,
        cidade_destino TEXT NOT NULL,
        valor DOUBLE PRECISION NOT NULL,
        data_reserva DATE NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Índices para melhor performance
    CREATE INDEX IF NOT EXISTS idx_hoteis_company_id ON hoteis(company_id);
    CREATE INDEX IF NOT EXISTS idx_hoteis_data_reserva ON hoteis(data_reserva);
    CREATE INDEX IF NOT EXISTS idx_hoteis_cidade ON hoteis(cidade);
    
    CREATE INDEX IF NOT EXISTS idx_voos_company_id ON voos(company_id);
    CREATE INDEX IF NOT EXISTS idx_voos_data_reserva ON voos(data_reserva);
    CREATE INDEX IF NOT EXISTS idx_voos_cidade_destino ON voos(cidade_destino);

    -- Grant de permissões
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
EOSQL
