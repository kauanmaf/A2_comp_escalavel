# Usa uma imagem base oficial do Spark
FROM bitnami/spark:3.5.1

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Define um diretório de cache para o uv que o usuário do contêiner tem permissão de escrita
ENV UV_CACHE_DIR=/tmp/uv_cache

# Instalação de Cron
USER root
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y cron && \
    rm -rf /var/lib/apt/lists/*

# Copia os arquivos de configuração do gerenciador de dependências
COPY pyproject.toml uv.lock ./

# Instala o 'uv' e usa 'uv sync' para instalar todas as dependências Python
RUN pip install uv && python -m uv sync

# Copia o script que submeterá o job Spark
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Copia o arquivo de configuração do cronjob
COPY my_cronjob /etc/cron.d/my_cronjob
RUN chmod 0644 /etc/cron.d/my_cronjob

# Copia o script Python principal que contém a lógica do job Spark
COPY main.py ./

# Define o comando de entrada principal para o contêiner
ENTRYPOINT ["cron", "-f"]