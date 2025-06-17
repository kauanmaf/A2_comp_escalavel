-- Estatísticas de hotéis por mês e companhia
-- Retorna: company_id, cidade, mes_reserva, sum_valor
CREATE TABLE IF NOT EXISTS stats_month_hotel (
    company_id TEXT,
    cidade TEXT,
    mes_reserva INT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_hotel_company ON stats_month_hotel(company_id);

-- Estatísticas de hotéis por cidade e companhia
-- Retorna: company_id, cidade, sum_valor
CREATE TABLE IF NOT EXISTS stats_city_hotel (
    company_id TEXT,
    cidade TEXT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade)
);
CREATE INDEX IF NOT EXISTS idx_stats_city_hotel_company ON stats_city_hotel(company_id);

-- Estatísticas de voos por mês e companhia
-- Retorna: company_id, cidade_destino, mes_reserva, sum_valor
CREATE TABLE IF NOT EXISTS stats_month_voo (
    company_id TEXT,
    cidade_destino TEXT,
    mes_reserva INT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade_destino, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_voo_company ON stats_month_voo(company_id);

-- Estatísticas de voos por cidade de destino e companhia
-- Retorna: company_id, cidade_destino, sum_valor
CREATE TABLE IF NOT EXISTS stats_city_voo (
    company_id TEXT,
    cidade_destino TEXT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade_destino)
);
CREATE INDEX IF NOT EXISTS idx_stats_city_voo_company ON stats_city_voo(company_id);

-- Estatísticas de faturamento total por mês e companhia (hotéis + voos)
-- Retorna: company_id, mes_reserva, total_valor
CREATE TABLE IF NOT EXISTS stats_faturamentos_totais (
    company_id TEXT,
    mes_reserva INT,
    total_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_faturamentos_totais_company ON stats_faturamentos_totais(company_id);

-- Estatísticas por estrelas de hotel e companhia (número de reservas)
-- Retorna: company_id, estrelas, num_reservas
CREATE TABLE IF NOT EXISTS stats_stars_hotel (
    company_id TEXT,
    estrelas INT,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, estrelas)
);
CREATE INDEX IF NOT EXISTS idx_stats_stars_hotel_company ON stats_stars_hotel(company_id);

-- Estatísticas de voos de/para São Paulo por mês e companhia
-- Retorna: mes, company_id, num_voos_reservados
CREATE TABLE IF NOT EXISTS stats_month_sp_voos (
    company_id TEXT,
    mes INT,
    num_voos_reservados BIGINT,
    PRIMARY KEY(company_id, mes)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_sp_voos_company ON stats_month_sp_voos(company_id);

-- Estatísticas de voos de/para São Paulo por dia e companhia
-- Retorna: data_reserva, company_id, num_reservas
CREATE TABLE IF NOT EXISTS stats_day_sp_voos (
    company_id TEXT,
    data_reserva DATE,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, data_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_day_sp_voos_company ON stats_day_sp_voos(company_id);
