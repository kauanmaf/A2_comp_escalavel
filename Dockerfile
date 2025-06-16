# Usa uma imagem base oficial do Spark
FROM bitnami/spark:3.5.1

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

USER root

# Copia os arquivos de configuração do gerenciador de dependências
COPY pyproject.toml ./

# Instala as dependências
RUN /opt/bitnami/python/bin/pip install requests redis faker pyspark

# Copia o script do job Spark
COPY main.py ./

# Copia e torna executável o entrypoint.sh, se ainda for usado para alguma inicialização
COPY entrypoint.sh ./
RUN chmod +x ./entrypoint.sh