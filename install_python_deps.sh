#!/bin/bash
set -euxo pipefail

echo "Instalando psycopg2-binary em todos os nós do cluster EMR..."
sudo python3 -m pip install redis psycopg2-binary

if [ $? -eq 0 ]; then
  echo "psycopg2-binary instalado com sucesso!"
else
  echo "ERRO: Falha ao instalar psycopg2-binary."
  exit 1
fi