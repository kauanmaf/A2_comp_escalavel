import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

# Função para conexão com o PostgreSQL
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_STATS_HOST", "localhost"),
        port=os.getenv("PG_STATS_PORT", "5433"),
        dbname=os.getenv("PG_STATS_DB", "dados_stats"),
        user=os.getenv("PG_STATS_USER", "emap"),
        password=os.getenv("PG_STATS_PASSWORD", "emap123"),
    )

# Função para carregar uma tabela específica filtrando por company_id
def load_table(table_name, company_id):
    conn = get_pg_connection()
    query = f"SELECT * FROM {table_name} WHERE company_id = %s"
    df = pd.read_sql(query, conn, params=(company_id,))
    conn.close()
    return df

# Nome das tabelas que vamos exibir
TABLES = [
    "stats_month_hotel",
    "stats_city_hotel",
    "stats_month_voos",
    "stats_city_voos",
    "stats_faturamentos_totais",
    "stats_ticket_medio",
    "stats_stars_hotel",
    "stats_estrelas_medias_mes",
    "stats_month_sp_voos",
    "stats_day_sp_voos",
]

st.title("📊 Dashboard de Estatísticas por Cliente")

# Conexão rápida só pra pegar os company_ids disponíveis
with get_pg_connection() as conn:
    company_ids = pd.read_sql("SELECT DISTINCT company_id FROM stats_month_hotel", conn)["company_id"].tolist()

selected_company = st.selectbox("Selecione o Company ID", company_ids)

# Para cada tabela, vamos ler os dados filtrados
for table in TABLES:
    st.subheader(f"Tabela: {table}")
    df = load_table(table, selected_company)
    
    if df.empty:
        st.info("Nenhum dado encontrado para este cliente nesta tabela.")
    else:
        st.dataframe(df)

        # Exemplo de gráfico se tiver colunas de valor
        numeric_cols = df.select_dtypes(include='number').columns.tolist()
        if len(numeric_cols) >= 1:
            x_col = df.columns[1] if len(df.columns) > 1 else None  # Tenta pegar uma coluna de categoria
            if x_col:
                fig = px.bar(df, x=x_col, y=numeric_cols[0], title=f"{numeric_cols[0]} por {x_col}")
                st.plotly_chart(fig, use_container_width=True, key=f"{table}_plot")


