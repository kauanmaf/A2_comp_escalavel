-- Estatísticas de hotéis por mês e companhia
CREATE TABLE IF NOT EXISTS stats_month_hotel (
    company_id TEXT,
    mes_reserva INT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_hotel_company ON stats_month_hotel(company_id);

-- Estatísticas de hotéis por cidade e companhia
CREATE TABLE IF NOT EXISTS stats_city_hotel (
    company_id TEXT,
    cidade TEXT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade)
);
CREATE INDEX IF NOT EXISTS idx_stats_city_hotel_company ON stats_city_hotel(company_id);

-- Estatísticas de voos por mês e companhia
CREATE TABLE IF NOT EXISTS stats_month_voos (
    company_id TEXT,
    mes_reserva INT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_voos_company ON stats_month_voos(company_id);

-- Estatísticas de voos por cidade de destino e companhia
CREATE TABLE IF NOT EXISTS stats_city_voos (
    company_id TEXT,
    cidade_destino TEXT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade_destino)
);
CREATE INDEX IF NOT EXISTS idx_stats_city_voos_company ON stats_city_voos(company_id);

-- Estatísticas de faturamento total por mês e companhia
CREATE TABLE IF NOT EXISTS stats_faturamentos_totais (
    company_id TEXT,
    mes_reserva INT,
    total_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_faturamentos_totais_company ON stats_faturamentos_totais(company_id);

-- Estatísticas de ticket médio por cidade e companhia
CREATE TABLE IF NOT EXISTS stats_ticket_medio (
    company_id TEXT,
    cidade_destino TEXT,
    sum_valor DOUBLE PRECISION,
    num_transacoes BIGINT,
    PRIMARY KEY(company_id, cidade_destino)
);
CREATE INDEX IF NOT EXISTS idx_stats_ticket_medio_company ON stats_ticket_medio(company_id);

-- Estatísticas por estrelas de hotel e companhia
CREATE TABLE IF NOT EXISTS stats_stars_hotel (
    company_id TEXT,
    estrelas INT,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, estrelas)
);
CREATE INDEX IF NOT EXISTS idx_stats_stars_hotel_company ON stats_stars_hotel(company_id);

-- Estatísticas de estrelas médias dos hotéis por mês e companhia
CREATE TABLE IF NOT EXISTS stats_estrelas_medias_mes (
    company_id TEXT,
    mes_reserva INT,
    sum_estrelas BIGINT,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_estrelas_medias_mes_company ON stats_estrelas_medias_mes(company_id);

-- Estatísticas de voos de/para São Paulo por mês e companhia
CREATE TABLE IF NOT EXISTS stats_month_sp_voos (
    company_id TEXT,
    mes INT,
    num_voos_reservados BIGINT,
    PRIMARY KEY(company_id, mes)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_sp_voos_company ON stats_month_sp_voos(company_id);

-- Estatísticas de voos de/para São Paulo por dia e companhia
CREATE TABLE IF NOT EXISTS stats_day_sp_voos (
    company_id TEXT,
    data_reserva DATE,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, data_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_day_sp_voos_company ON stats_day_sp_voos(company_id);
