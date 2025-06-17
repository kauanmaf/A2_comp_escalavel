# Setup do Banco de Dados Automático

## Como Usar

### **Primeira vez (dados serão gerados):**
```bash
# Subir toda a infraestrutura
docker-compose up -d

# Acompanhar o progresso da geração (opcional)
docker-compose logs -f data-generator
```

### **Próximas vezes (dados já existem):**
```bash
# Subir normalmente - o gerador detecta dados existentes
docker-compose up -d
```

### **Para regenerar dados:**
```bash
# Parar tudo
docker-compose down

# Remover dados antigos
sudo rm -rf pgdata_total/*

# Subir novamente (dados serão gerados)
docker-compose up -d
```

## Verificando os Dados

### **Conectar ao PostgreSQL:**
```bash
# Via Docker
docker exec -it postgres-data-db psql -U emap -d dados_gerais

# Via cliente local (se tiver psql instalado)
psql -h localhost -p 5432 -U emap -d dados_gerais
```

### **Consultas úteis:**
```sql
-- Verificar se os dados foram carregados
SELECT COUNT(*) FROM hoteis;
SELECT COUNT(*) FROM voos;

-- Estatísticas de hotéis por estrelas
SELECT estrelas, COUNT(*) as total,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentual
FROM hoteis
GROUP BY estrelas
ORDER BY estrelas;

-- Top 10 cidades com mais hotéis
SELECT cidade, COUNT(*) as total_hoteis
FROM hoteis
GROUP BY cidade
ORDER BY total_hoteis DESC
LIMIT 10;

-- Voos por mês
SELECT mes, COUNT(*) as total_voos
FROM voos
GROUP BY mes
ORDER BY mes;
```

## Importantes

### **Tempo de Geração:**
- **Primeira vez:** ~3-5 minutos (depende do hardware)
- **Execuções seguintes:** ~10 segundos (só verifica se dados existem)

### **Persistência:**
- Os dados ficam salvos em `./pgdata_total/`
- **Não apague** esta pasta se quiser manter os dados

## Troubleshooting

### **Se der erro na geração:**
```bash
# Ver logs detalhados
docker-compose logs data-generator

# Forçar rebuild do container
docker-compose build --no-cache data-generator
docker-compose up -d
```

### **Se quiser regenerar dados:**
```bash
# Parar tudo
docker-compose down

# Limpar volumes (CUIDADO: apaga todos os dados)
docker-compose down -v

# Ou apenas limpar a pasta de dados
sudo rm -rf pgdata_total/*

# Subir novamente
docker-compose up -d
```
