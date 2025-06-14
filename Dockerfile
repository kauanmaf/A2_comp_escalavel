# Usa uma imagem base oficial do Spark.
# É crucial escolher uma imagem que seja compatível com a sua versão de Python
# e tenha o Java (JRE) necessário.
FROM bitnami/spark:3.5.1

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Define um diretório de cache para o uv que o usuário do contêiner tem permissão de escrita
ENV UV_CACHE_DIR=/tmp/uv_cache

# Copia os arquivos de configuração do Poetry e de bloqueio de dependências
COPY pyproject.toml uv.lock ./

# Instala as dependências usando uv
RUN pip install uv && python -m uv sync

# COPIE O SEU SCRIPT ENTRYPOINT.SH AQUI!
COPY entrypoint.sh ./

# Copia seu script Python principal
COPY main.py ./

# O comando de entrada padrão para o contêiner.
# Este será o script que você usará para submeter seu job Spark.
ENTRYPOINT ["/app/entrypoint.sh"]