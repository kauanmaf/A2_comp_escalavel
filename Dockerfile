# Usa uma imagem base oficial do Spark
FROM bitnami/spark:3.5.1

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

USER root

# Instala as dependências
RUN /opt/bitnami/python/bin/pip install redis faker pyspark

# Copia o script do job Spark
COPY main.py ./
COPY pipeline_functions.py ./

# Copia e torna executável o entrypoint.sh
# COPY entrypoint.sh ./
# RUN chmod +x ./entrypoint.sh