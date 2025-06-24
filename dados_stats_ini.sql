-- 0. Criação da tabela de log de execução do pipeline
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    rows_hotels INT NOT NULL,
    rows_flights INT NOT NULL,
    spark_duration_seconds DOUBLE PRECISION NOT NULL,
    db_write_duration_seconds DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ==== 1. Tabelas originais de estatísticas ====
CREATE TABLE IF NOT EXISTS stats_month_hotel (
    company_id TEXT,
    mes_reserva INT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_hotel_company ON stats_month_hotel(company_id);

CREATE TABLE IF NOT EXISTS stats_city_hotel (
    company_id TEXT,
    cidade TEXT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade)
);
CREATE INDEX IF NOT EXISTS idx_stats_city_hotel_company ON stats_city_hotel(company_id);

CREATE TABLE IF NOT EXISTS stats_month_voos (
    company_id TEXT,
    mes_reserva INT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_voos_company ON stats_month_voos(company_id);

CREATE TABLE IF NOT EXISTS stats_city_voos (
    company_id TEXT,
    cidade_destino TEXT,
    sum_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, cidade_destino)
);
CREATE INDEX IF NOT EXISTS idx_stats_city_voos_company ON stats_city_voos(company_id);

CREATE TABLE IF NOT EXISTS stats_faturamentos_totais (
    company_id TEXT,
    mes_reserva INT,
    total_valor DOUBLE PRECISION,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_faturamentos_totais_company ON stats_faturamentos_totais(company_id);

CREATE TABLE IF NOT EXISTS stats_ticket_medio (
    company_id TEXT,
    cidade_destino TEXT,
    sum_valor DOUBLE PRECISION,
    num_transacoes BIGINT,
    PRIMARY KEY(company_id, cidade_destino)
);
CREATE INDEX IF NOT EXISTS idx_stats_ticket_medio_company ON stats_ticket_medio(company_id);

CREATE TABLE IF NOT EXISTS stats_stars_hotel (
    company_id TEXT,
    estrelas INT,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, estrelas)
);
CREATE INDEX IF NOT EXISTS idx_stats_stars_hotel_company ON stats_stars_hotel(company_id);

CREATE TABLE IF NOT EXISTS stats_estrelas_medias_mes (
    company_id TEXT,
    mes_reserva INT,
    sum_estrelas BIGINT,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, mes_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_estrelas_medias_mes_company ON stats_estrelas_medias_mes(company_id);

CREATE TABLE IF NOT EXISTS stats_month_sp_voos (
    company_id TEXT,
    mes INT,
    num_voos_reservados BIGINT,
    PRIMARY KEY(company_id, mes)
);
CREATE INDEX IF NOT EXISTS idx_stats_month_sp_voos_company ON stats_month_sp_voos(company_id);

CREATE TABLE IF NOT EXISTS stats_day_sp_voos (
    company_id TEXT,
    data_reserva DATE,
    num_reservas BIGINT,
    PRIMARY KEY(company_id, data_reserva)
);
CREATE INDEX IF NOT EXISTS idx_stats_day_sp_voos_company ON stats_day_sp_voos(company_id);

-- ==== 2. Tabelas de staging com coluna processed ====
CREATE TABLE IF NOT EXISTS staging_stats_month_hotel (
    company_id TEXT NOT NULL,
    mes_reserva INT NOT NULL,
    sum_valor DOUBLE PRECISION NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_month_hotel_processed ON staging_stats_month_hotel(processed);

CREATE TABLE IF NOT EXISTS staging_stats_city_hotel (
    company_id TEXT NOT NULL,
    cidade TEXT NOT NULL,
    sum_valor DOUBLE PRECISION NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_city_hotel_processed ON staging_stats_city_hotel(processed);

CREATE TABLE IF NOT EXISTS staging_stats_month_voos (
    company_id TEXT NOT NULL,
    mes_reserva INT NOT NULL,
    sum_valor DOUBLE PRECISION NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_month_voos_processed ON staging_stats_month_voos(processed);

CREATE TABLE IF NOT EXISTS staging_stats_city_voos (
    company_id TEXT NOT NULL,
    cidade_destino TEXT NOT NULL,
    sum_valor DOUBLE PRECISION NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_city_voos_processed ON staging_stats_city_voos(processed);

CREATE TABLE IF NOT EXISTS staging_stats_faturamentos_totais (
    company_id TEXT NOT NULL,
    mes_reserva INT NOT NULL,
    total_valor DOUBLE PRECISION NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_faturamentos_totais_processed ON staging_stats_faturamentos_totais(processed);

CREATE TABLE IF NOT EXISTS staging_stats_ticket_medio (
    company_id TEXT NOT NULL,
    cidade_destino TEXT NOT NULL,
    sum_valor DOUBLE PRECISION NOT NULL,
    num_transacoes BIGINT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_ticket_medio_processed ON staging_stats_ticket_medio(processed);

CREATE TABLE IF NOT EXISTS staging_stats_stars_hotel (
    company_id TEXT NOT NULL,
    estrelas INT NOT NULL,
    num_reservas BIGINT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_stars_hotel_processed ON staging_stats_stars_hotel(processed);

CREATE TABLE IF NOT EXISTS staging_stats_estrelas_medias_mes (
    company_id TEXT NOT NULL,
    mes_reserva INT NOT NULL,
    sum_estrelas BIGINT NOT NULL,
    num_reservas BIGINT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_estrelas_medias_mes_processed ON staging_stats_estrelas_medias_mes(processed);

CREATE TABLE IF NOT EXISTS staging_stats_month_sp_voos (
    company_id TEXT NOT NULL,
    mes INT NOT NULL,
    num_voos_reservados BIGINT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_month_sp_voos_processed ON staging_stats_month_sp_voos(processed);

CREATE TABLE IF NOT EXISTS staging_stats_day_sp_voos (
    company_id TEXT NOT NULL,
    data_reserva DATE NOT NULL,
    num_reservas BIGINT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    inserido_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_day_sp_voos_processed ON staging_stats_day_sp_voos(processed);

-- ==== 3. Função PL/pgSQL para agregação e remoção de staging ====
CREATE OR REPLACE FUNCTION processar_todas_estatisticas() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_limit INTEGER := 10000;
BEGIN
  -- 1. stats_month_hotel
  IF EXISTS (SELECT 1 FROM staging_stats_month_hotel WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_month_hotel
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_month_hotel WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, mes_reserva, sum_valor
    ), agg AS (
      SELECT company_id, mes_reserva, SUM(sum_valor) AS soma
      FROM marked
      GROUP BY company_id, mes_reserva
    )
    INSERT INTO stats_month_hotel(company_id, mes_reserva, sum_valor)
    SELECT company_id, mes_reserva, soma FROM agg
    ON CONFLICT (company_id, mes_reserva)
    DO UPDATE SET sum_valor = stats_month_hotel.sum_valor + EXCLUDED.sum_valor;
    DELETE FROM staging_stats_month_hotel WHERE processed = TRUE;
  END IF;

  -- 2. stats_city_hotel
  IF EXISTS (SELECT 1 FROM staging_stats_city_hotel WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_city_hotel
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_city_hotel WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, cidade, sum_valor
    ), agg AS (
      SELECT company_id, cidade, SUM(sum_valor) AS soma
      FROM marked
      GROUP BY company_id, cidade
    )
    INSERT INTO stats_city_hotel(company_id, cidade, sum_valor)
    SELECT company_id, cidade, soma FROM agg
    ON CONFLICT (company_id, cidade)
    DO UPDATE SET sum_valor = stats_city_hotel.sum_valor + EXCLUDED.sum_valor;
    DELETE FROM staging_stats_city_hotel WHERE processed = TRUE;
  END IF;

  -- 3. stats_month_voos
  IF EXISTS (SELECT 1 FROM staging_stats_month_voos WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_month_voos
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_month_voos WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, mes_reserva, sum_valor
    ), agg AS (
      SELECT company_id, mes_reserva, SUM(sum_valor) AS soma
      FROM marked
      GROUP BY company_id, mes_reserva
    )
    INSERT INTO stats_month_voos(company_id, mes_reserva, sum_valor)
    SELECT company_id, mes_reserva, soma FROM agg
    ON CONFLICT (company_id, mes_reserva)
    DO UPDATE SET sum_valor = stats_month_voos.sum_valor + EXCLUDED.sum_valor;
    DELETE FROM staging_stats_month_voos WHERE processed = TRUE;
  END IF;

  -- 4. stats_city_voos
  IF EXISTS (SELECT 1 FROM staging_stats_city_voos WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_city_voos
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_city_voos WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, cidade_destino, sum_valor
    ), agg AS (
      SELECT company_id, cidade_destino, SUM(sum_valor) AS soma
      FROM marked
      GROUP BY company_id, cidade_destino
    )
    INSERT INTO stats_city_voos(company_id, cidade_destino, sum_valor)
    SELECT company_id, cidade_destino, soma FROM agg
    ON CONFLICT (company_id, cidade_destino)
    DO UPDATE SET sum_valor = stats_city_voos.sum_valor + EXCLUDED.sum_valor;
    DELETE FROM staging_stats_city_voos WHERE processed = TRUE;
  END IF;

  -- 5. stats_faturamentos_totais
  IF EXISTS (SELECT 1 FROM staging_stats_faturamentos_totais WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_faturamentos_totais
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_faturamentos_totais WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, mes_reserva, total_valor
    ), agg AS (
      SELECT company_id, mes_reserva, SUM(total_valor) AS soma
      FROM marked
      GROUP BY company_id, mes_reserva
    )
    INSERT INTO stats_faturamentos_totais(company_id, mes_reserva, total_valor)
    SELECT company_id, mes_reserva, soma FROM agg
    ON CONFLICT (company_id, mes_reserva)
    DO UPDATE SET total_valor = stats_faturamentos_totais.total_valor + EXCLUDED.total_valor;
    DELETE FROM staging_stats_faturamentos_totais WHERE processed = TRUE;
  END IF;

  -- 6. stats_ticket_medio
  IF EXISTS (SELECT 1 FROM staging_stats_ticket_medio WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_ticket_medio
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_ticket_medio WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, cidade_destino, sum_valor, num_transacoes
    ), agg AS (
      SELECT company_id, cidade_destino, SUM(sum_valor) AS soma_valor, SUM(num_transacoes) AS soma_transacoes
      FROM marked
      GROUP BY company_id, cidade_destino
    )
    INSERT INTO stats_ticket_medio(company_id, cidade_destino, sum_valor, num_transacoes)
    SELECT company_id, cidade_destino, soma_valor, soma_transacoes FROM agg
    ON CONFLICT (company_id, cidade_destino)
    DO UPDATE SET
      sum_valor = stats_ticket_medio.sum_valor + EXCLUDED.sum_valor,
      num_transacoes = stats_ticket_medio.num_transacoes + EXCLUDED.num_transacoes;
    DELETE FROM staging_stats_ticket_medio WHERE processed = TRUE;
  END IF;

  -- 7. stats_stars_hotel
  IF EXISTS (SELECT 1 FROM staging_stats_stars_hotel WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_stars_hotel
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_stars_hotel WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, estrelas, num_reservas
    ), agg AS (
      SELECT company_id, estrelas, SUM(num_reservas) AS soma_reservas
      FROM marked
      GROUP BY company_id, estrelas
    )
    INSERT INTO stats_stars_hotel(company_id, estrelas, num_reservas)
    SELECT company_id, estrelas, soma_reservas FROM agg
    ON CONFLICT (company_id, estrelas)
    DO UPDATE SET num_reservas = stats_stars_hotel.num_reservas + EXCLUDED.num_reservas;
    DELETE FROM staging_stats_stars_hotel WHERE processed = TRUE;
  END IF;

  -- 8. stats_estrelas_medias_mes
  IF EXISTS (SELECT 1 FROM staging_stats_estrelas_medias_mes WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_estrelas_medias_mes
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_estrelas_medias_mes WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, mes_reserva, sum_estrelas, num_reservas
    ), agg AS (
      SELECT company_id, mes_reserva, SUM(sum_estrelas) AS soma_estrelas, SUM(num_reservas) AS soma_reservas
      FROM marked
      GROUP BY company_id, mes_reserva
    )
    INSERT INTO stats_estrelas_medias_mes(company_id, mes_reserva, sum_estrelas, num_reservas)
    SELECT company_id, mes_reserva, soma_estrelas, soma_reservas FROM agg
    ON CONFLICT (company_id, mes_reserva)
    DO UPDATE SET
      sum_estrelas = stats_estrelas_medias_mes.sum_estrelas + EXCLUDED.sum_estrelas,
      num_reservas = stats_estrelas_medias_mes.num_reservas + EXCLUDED.num_reservas;
    DELETE FROM staging_stats_estrelas_medias_mes WHERE processed = TRUE;
  END IF;

  -- 9. stats_month_sp_voos
  IF EXISTS (SELECT 1 FROM staging_stats_month_sp_voos WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_month_sp_voos
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_month_sp_voos WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, mes, num_voos_reservados
    ), agg AS (
      SELECT company_id, mes, SUM(num_voos_reservados) AS soma_voos
      FROM marked
      GROUP BY company_id, mes
    )
    INSERT INTO stats_month_sp_voos(company_id, mes, num_voos_reservados)
    SELECT company_id, mes, soma_voos FROM agg
    ON CONFLICT (company_id, mes)
    DO UPDATE SET num_voos_reservados = stats_month_sp_voos.num_voos_reservados + EXCLUDED.num_voos_reservados;
    DELETE FROM staging_stats_month_sp_voos WHERE processed = TRUE;
  END IF;

  -- 10. stats_day_sp_voos
  IF EXISTS (SELECT 1 FROM staging_stats_day_sp_voos WHERE processed = FALSE) THEN
    WITH marked AS (
      UPDATE staging_stats_day_sp_voos
      SET processed = TRUE
      WHERE ctid IN (
        SELECT ctid FROM staging_stats_day_sp_voos WHERE processed = FALSE LIMIT v_limit
      )
      RETURNING company_id, data_reserva, num_reservas
    ), agg AS (
      SELECT company_id, data_reserva, SUM(num_reservas) AS soma_reservas
      FROM marked
      GROUP BY company_id, data_reserva
    )
    INSERT INTO stats_day_sp_voos(company_id, data_reserva, num_reservas)
    SELECT company_id, data_reserva, soma_reservas FROM agg
    ON CONFLICT (company_id, data_reserva)
    DO UPDATE SET num_reservas = stats_day_sp_voos.num_reservas + EXCLUDED.num_reservas;
    DELETE FROM staging_stats_day_sp_voos WHERE processed = TRUE;
  END IF;

END;
$$;
