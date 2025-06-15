# Usa uma imagem base oficial do Spark
FROM bitnami/spark:3.5.1

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Define um diretório de cache para o uv que o usuário do contêiner tem permissão de escrita
ENV UV_CACHE_DIR=/tmp/uv_cache

USER root

# Copia os arquivos de configuração do gerenciador de dependências
COPY pyproject.toml uv.lock ./
# Garante que os arquivos copiados tenham permissão de escrita para o root
RUN chmod +w pyproject.toml uv.lock

# Instala o 'uv' e usa 'uv sync' para instalar todas as dependências Python
RUN pip install uv && python -m uv sync

# Copia o script do servidor web Flask
COPY app.py ./
# Copia o script do job Spark
COPY main.py ./

COPY entrypoint.sh ./
RUN chmod +x ./entrypoint.sh

# Define o comando de entrada principal para o contêiner
ENTRYPOINT ["python", "app.py"]