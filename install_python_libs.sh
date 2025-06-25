SPARK_PYTHON_DIR=$(find /usr/lib/spark/python/lib/ -name "py4j-*.zip" -type f -print -quit)
if [ -z "$SPARK_PYTHON_DIR" ]; then
    echo "Could not find Spark Python lib directory."
    exit 1
fi

# Adiciona o diretório Spark Python ao PYTHONPATH para garantir que o pip correto seja usado
export PYTHONPATH=$SPARK_PYTHON_DIR:$PYTHONPATH

# EMR Cluster
export EMR_CLUSTER_ID="j-9RSFF3HRM9NY"

# Configurações do Redis (Elasticache)
export REDIS_HOST="master.redis-brocker.slp5lw.use1.cache.amazonaws.com"
export REDIS_PORT="6379"

# Configurações do PostgreSQL (RDS) - Dados Fixos
export PG_DATA_HOST="a2-comp-escalavel-dados-fixos.col2wfyf2csx.us-east-1.rds.amazonaws.com"
export PG_DATA_PORT="5432"
export PG_DATA_DB="postgres"
export PG_DATA_USER="A2CompEscalavel"
export PG_DATA_PASSWORD="euadoroaemap"

# Configurações do PostgreSQL (RDS) - Estatísticas
export PG_STATS_HOST="a2-comp-escalavel-dados-estatisticas.col2wfyf2csx.us-east-1.rds.amazonaws.com"
export PG_STATS_PORT="5432"
export PG_STATS_DB="postgres"
export PG_STATS_USER="A2CompEscalavel"
export PG_STATS_PASSWORD="euadoroaemap"


# Outras configurações da Aplicação Spark
export MONITOR_INTERVAL_SECONDS="5"
export LIST_THRESHOLDS_JSON='{"raw_hotels": 50, "raw_flights": 50}'

# Caminho completo para o binário pip do Spark
SPARK_PIP_BIN=$(find /usr/bin /usr/local/bin -name "pip3" -exec file {} \; | grep "Python" | awk '{print $1}' | sed 's/://g' | head -n 1)

if [ -z "$SPARK_PIP_BIN" ]; then
    echo "Could not find Spark's pip binary. Trying alternative path."
    # Tentativa secundária para ambientes como o EMR 6.x
    SPARK_PIP_BIN="/usr/bin/pip3"
    if [ ! -f "$SPARK_PIP_BIN" ]; then
        echo "Alternative pip path also not found."
        exit 1
    fi
fi

echo "Using pip from: $SPARK_PIP_BIN"

# Baixa o arquivo requirements.txt do S3
aws s3 cp https://spark-processor3.s3.us-east-1.amazonaws.com/config/requirements.txt /tmp/requirements.txt

# Instala as dependências usando o pip do Spark
$SPARK_PIP_BIN install --upgrade pip
$SPARK_PIP_BIN install -r /tmp/requirements.txt

echo "Python dependencies installed successfully."