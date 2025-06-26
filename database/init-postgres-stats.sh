#!/bin/bash
set -e

# Aguarda o PostgreSQL estar pronto
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  sleep 2
done

# Executa o script SQL de inicialização se existir
if [ -f "/docker-entrypoint-initdb.d/01-dados_stats_ini.sql" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/01-dados_stats_ini.sql
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
EOSQL
