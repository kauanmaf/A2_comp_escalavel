#!/bin/bash
set -e

INTERVAL=${INTERVAL_SECONDS:-60}
PGHOST=${PGHOST:-localhost}

export PGPASSWORD="$POSTGRES_PASSWORD"

echo "[stats-processor] Rodando a cada $INTERVAL segundos."

until pg_isready -h "$PGHOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  echo "[stats-processor] Aguardando banco em $PGHOST..."; sleep 2;
done

while true; do
  echo "[stats-processor] Executando processar_todas_estatisticas()..."
  psql -v ON_ERROR_STOP=1 -h "$PGHOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -c "SELECT processar_todas_estatisticas();" > /dev/null 2>&1
  sleep "$INTERVAL"
done