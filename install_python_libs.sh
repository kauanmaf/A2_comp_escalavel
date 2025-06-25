#!/bin/bash
# Garante que o pip esteja atualizado
/usr/bin/pip3 install --upgrade pip

# Instala as bibliotecas diretamente no ambiente Python principal do EMR
# O EMR gerencia o PYTHONPATH para este ambiente.
/usr/bin/pip3 install redis psycopg2-binary
echo "Redis and Psycopg2 installed directly into EMR Python environment."