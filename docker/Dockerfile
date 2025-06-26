# Usa uma imagem base oficial do Spark
FROM bitnami/spark:3.5.1

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

USER root

# Instala as dependências
RUN /opt/bitnami/python/bin/pip install redis faker pyspark psycopg2-binary

# Copia o script do job Spark
COPY spark/main.py ./
COPY spark/pipeline_functions.py ./
COPY spark/db_stats_utils.py ./

# Copia e torna executável o entrypoint.sh
# COPY entrypoint.sh ./
# RUN chmod +x ./entrypoint.sh
