# Sistema de Análise de Dados de Viagens - A2 Computação Escalável

Sistema distribuído para processamento e análise de dados de reservas de voos e hotéis em tempo real usando **Apache Spark**, **Redis**, **PostgreSQL** e **FastAPI**.


### Componentes:

- **Redis**: Stream de dados em tempo real
- **Apache Spark**: Processamento distribuído de dados
- **PostgreSQL** (2 instâncias):
  - `postgres-data`: Dados mestres (hotéis, voos)
  - `postgres-stats`: Estatísticas processadas
- **FastAPI**: API REST para consulta de estatísticas
- **Gerador de Dados**: Simulador de reservas em tempo real

## Pré-requisitos

- **Docker**
- **Docker Compose**
- **Python**
- **8GB+ RAM**
- **16GB+ espaço em disco**

## Início Rápido

### 1. Clone o repositório
```bash
git clone https://github.com/kauanmaf/A2_comp_escalavel
cd A2_comp_escalavel
```

### 2. Crie um ambiente virtual (exemplo para linux)
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Execute o sistema completo
```bash
docker-compose up
```

### 4. Aguarde a inicialização (2-3 minutos)
```bash
# Monitore os logs (opicional)
docker-compose logs -f
```

### 5. Gere dados em tempo real (em um novo terminal)
```bash
# Em um novo terminal
python mock_realtime_generator.py
```

### 6. Acesse a interface (em um novo terminal)
```bash
# Em um novo terminal
streamlit run dashboard/dashboard.py
```

## Execução detalhada

### Execução Passo a Passo

```bash
# 1. Subir apenas os bancos de dados
docker-compose up -d postgres-data postgres-stats redis-server

# 2. Gerar dados fixos
docker-compose up data-generator

# 3. Subir Spark
docker-compose up -d spark-master spark-worker spark-batch-processor

# 4. Subir API
docker-compose up -d stats-api

# 5. Gerar dados em tempo real
python mock_realtime_generator.py
```

## Geração de Dados

### Dados Fixos (Executado Automaticamente)

Os dados mestres são gerados automaticamente pelo serviço `data-generator`:

- **50 cidades** brasileiras
- **~7.000 hotéis** (50-250 por cidade, 1-5 estrelas)
- **~1.330.000 voos** (conectando todas as cidades e todos os dias)

### Dados em Tempo Real

Execute o gerador de dados em um terminal separado:

```bash
python mock_realtime_generator.py
```

**Configurações do Gerador:**
- **50.000 reservas de voos/minuto**
- **50.000 reservas de hotéis/minuto**
- **3 empresas**: CiaViagemA, CiaVoosB, AgenciaTurC

**Variáveis de Ambiente:**
```bash
# Personalizar quantidade de dados
export FLIGHTS_PER_MINUTE=30000
export HOTELS_PER_MINUTE=30000
python mock_realtime_generator.py
```

## APIs Disponíveis

### FastAPI - Estatísticas (Porta 8000)

**Base URL**: `http://localhost:8000`

#### Endpoints Principais (no navegador):

```bash
# Estatísticas de hotéis por estrelas
http://localhost:8000/stats/stars_hotel

# Estatísticas de média de estrelas por mês
http://localhost:8000/stats/estrelas_medias_mes

# Estatísticas de voos de SP por mês
http://localhost:8000/stats/month_sp_voos

# Estatísticas de voos de SP por dia
http://localhost:8000/stats/day_sp_voos

# Estatísticas de faturamento por mês (hotéis)
http://localhost:8000/stats/month_hotel

# Estatísticas de faturamento por cidade (hotéis)
http://localhost:8000/stats/city_hotel

# Estatísticas de faturamento por mês (voos)
http://localhost:8000/stats/month_voos

# Estatísticas de faturamento por cidade (voos)
http://localhost:8000/stats/city_voos

# Estatísticas de faturamento total
http://localhost:8000/stats/faturamentos_totais

# Estatísticas de ticket médio
http://localhost:8000/stats/ticket_medio

# Contagem de hotéis reservados por mês
http://localhost:8000/stats/month_hotel_count
```

## Como Regenerar Dados Fixos

### Opção 1: Regenerar os Dados Fixos
```bash
# Parar tudo
docker-compose down -v

# Remover dados antigos
sudo rm -rf pgdata_total pgdata_stats

# Subir gearador (dados serão gerados)
docker-compose build --no-cache data-generator

# Subir tudo
docker-compose up -d
```

## Monitoramento

### Logs dos Serviços
```bash
# Todos os serviços
docker-compose logs -f

# Serviço específico
docker-compose logs -f spark-batch-processor
docker-compose logs -f stats-api
docker-compose logs -f postgres-stats

# Acompanhar o progresso da geração (opcional)
docker-compose logs -f data-generator
```

### Monitorar Dados no Redis
```bash
# Conectar ao Redis
docker exec -it a2_comp_escalavel-redis-server-1 redis-cli

# Verificar listas
LLEN raw_hotels
LLEN raw_flights

# Ver dados
LRANGE raw_hotels 0 5
```

### Verificar Dados no PostgreSQL
### **Conectar ao PostgreSQL:**
```bash
# Via Docker
docker exec -it postgres-data-db psql -U emap -d dados_gerais

# Via cliente local (se tiver psql instalado)
psql -h localhost -p 5432 -U emap -d dados_gerais
```

### **Exemplo de Consultas:**
```sql
-- Verificar se os dados foram carregados
SELECT COUNT(*) FROM hoteis;
SELECT COUNT(*) FROM voos;
```
