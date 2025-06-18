import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px
import calendar

# --- Funções de Conexão e Carregamento de Dados ---

def get_pg_connection():
    """Retorna uma conexão com o banco de dados PostgreSQL."""
    return psycopg2.connect(
        host=os.getenv("PG_STATS_HOST", "localhost"),
        port=os.getenv("PG_STATS_PORT", "5433"),
        dbname=os.getenv("PG_STATS_DB", "dados_stats"),
        user=os.getenv("PG_STATS_USER", "emap"),
        password=os.getenv("PG_STATS_PASSWORD", "emap123"),
    )

@st.cache_data(ttl=600) # Cache data for 10 minutes
def load_table(table_name, company_id):
    """Carrega dados de uma tabela específica filtrando por company_id."""
    conn = get_pg_connection()
    query = f"SELECT * FROM {table_name} WHERE company_id = %s"
    df = pd.read_sql(query, conn, params=(company_id,))
    conn.close()
    return df

def get_month_name(month_num):
    """Retorna o nome abreviado do mês a partir do número."""
    try:
        return calendar.month_abbr[month_num]
    except IndexError:
        return None

# --- Funções de Plotagem Específicas para Cada Tabela ---

def plot_stats_month_hotel(df, config):
    """Plota os valores de hotéis por mês de reserva."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['sum_valor'] = df['sum_valor'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.bar(df, x='month_name', y='sum_valor', title=config["title"])
    fig.update_layout(xaxis_title="Mês de Reserva")
    return fig

def plot_stats_city_hotel(df, config):
    """Plota o top 10 de cidades com maior faturamento em hotéis."""
    df_sorted = df.sort_values(by='sum_valor', ascending=False).head(config["top_n"])
    fig = px.bar(df_sorted, x='cidade', y='sum_valor', title=config["title"])
    return fig

def plot_stats_month_voos(df, config):
    """Plota os maiores faturamentos de voos por mês de reserva."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['sum_valor'] = df['sum_valor'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.bar(df, x='month_name', y='sum_valor', title=config["title"])
    fig.update_layout(xaxis_title="Mês de Reserva")
    return fig

def plot_stats_city_voos(df, config):
    """Plota as cidades com maior faturamento em voos."""
    df_sorted = df.sort_values(by='sum_valor', ascending=False).head(config["top_n"])
    fig = px.bar(df_sorted, x='cidade_destino', y='sum_valor', title=config["title"])
    return fig

def plot_stats_faturamentos_totais(df, config):
    """Plota os faturamentos totais (voos e hotéis) por mês de reserva."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['total_valor'] = df['total_valor'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.bar(df, x='month_name', y='total_valor', title=config["title"])
    fig.update_layout(xaxis_title="Mês de Reserva")
    return fig

def plot_stats_ticket_medio(df, config):
    """Plota o ticket médio por cidade de destino."""
    col1, col2 = config["value_columns"]
    df[col1] = df[col1].fillna(0)
    df[col2] = df[col2].fillna(0)
    df['ticket_medio'] = df.apply(
        lambda row: row[col1] / row[col2] if row[col2] != 0 else 0,
        axis=1
    )
    fig = px.bar(df, x='cidade_destino', y='ticket_medio', title=config["title"])
    return fig

def plot_stats_stars_hotel(df, config):
    """Plota o número de reservas por estrelas de hotel."""
    star_order = [1, 2, 3, 4, 5]
    df['estrelas'] = pd.Categorical(df['estrelas'], categories=star_order, ordered=True)
    fig = px.bar(df, x='estrelas', y='num_reservas', title=config["title"])
    return fig

def plot_stats_estrelas_medias_mes(df, config):
    """Plota a média de estrelas de hotéis por mês de reserva."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['sum_estrelas'] = df['sum_estrelas'].fillna(0)
    df['num_reservas'] = df['num_reservas'].fillna(0)
    df['media_estrelas'] = df.apply(
        lambda row: row['sum_estrelas'] / row['num_reservas'] if row['num_reservas'] != 0 else 0,
        axis=1
    )
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.bar(df, x='month_name', y='media_estrelas', title=config["title"])
    fig.update_layout(xaxis_title="Mês de Reserva")
    return fig

def plot_stats_month_sp_voos(df, config):
    """Plota a quantidade de voos reservados por mês em SP."""
    df['mes'] = pd.to_numeric(df['mes'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes', how='left')
    df['num_voos_reservados'] = df['num_voos_reservados'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.bar(df, x='month_name', y='num_voos_reservados', title=config["title"])
    fig.update_layout(xaxis_title="Mês de Reserva")
    return fig

def plot_stats_day_sp_voos(df, config):
    """Plota os top 10 dias com mais reservas de voos em SP."""
    df_sorted = df.sort_values(by='num_reservas', ascending=False).head(config["top_n"])
    fig = px.bar(df_sorted, x='data_reserva', y='num_reservas', title=config["title"])
    return fig


# Mapeamento de tabelas e suas configurações (agora com referência direta às funções)
TABLE_CONFIG = {
    "stats_month_hotel": {
        "title": "Valores de Hotéis por Mês de Reserva", "section": "Hoteis",
        "plot_func": plot_stats_month_hotel
    },
    "stats_city_hotel": {
        "title": "Top 10 Cidades com Maior Faturamento em Hotéis", "section": "Hoteis",
        "top_n": 10, "plot_func": plot_stats_city_hotel
    },
    "stats_month_voos": {
        "title": "Maiores Faturamentos de Voos por Mês de Reserva", "section": "Voos",
        "plot_func": plot_stats_month_voos
    },
    "stats_city_voos": {
        "title": "Cidades com Maior Faturamento em Voos", "section": "Voos",
        "top_n": 10, "plot_func": plot_stats_city_voos
    },
    "stats_faturamentos_totais": {
        "title": "Faturamentos Totais (Voos e Hotéis) por Mês de Reserva", "section": "Faturamento Total",
        "plot_func": plot_stats_faturamentos_totais
    },
    "stats_ticket_medio": {
        "title": "Ticket Médio por Cidade de Destino", "section": "Faturamento Total",
        "value_columns": ["sum_valor", "num_transacoes"], "plot_func": plot_stats_ticket_medio
    },
    "stats_stars_hotel": {
        "title": "Número de Reservas por Estrelas de Hotel", "section": "Hoteis",
        "plot_func": plot_stats_stars_hotel
    },
    "stats_estrelas_medias_mes": {
        "title": "Média de Estrelas de Hotéis por Mês de Reserva", "section": "Hoteis",
        "value_columns": ["sum_estrelas", "num_reservas"], "plot_func": plot_stats_estrelas_medias_mes
    },
    "stats_month_sp_voos": {
        "title": "Quantidade de Voos Reservados por Mês em SP", "section": "Voos",
        "plot_func": plot_stats_month_sp_voos
    },
    "stats_day_sp_voos": {
        "title": "Top 10 Dias com Mais Reservas de Voos em SP", "section": "Voos",
        "top_n": 10, "plot_func": plot_stats_day_sp_voos
    },
}

# --- Configuração da Página Streamlit ---

st.set_page_config(layout="wide")
st.title("📊 Dashboard de Estatísticas por Cliente")

# --- Seleção de Company ID ---

try:
    with get_pg_connection() as conn:
        company_ids_df = pd.read_sql("SELECT DISTINCT company_id FROM stats_month_hotel", conn)
        company_ids = company_ids_df["company_id"].tolist()
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados para buscar Company IDs: {e}")
    st.stop()

selected_company = st.selectbox("Selecione o Company ID", company_ids)

# --- Agrupamento por Seção ---

sections_order = ["Hoteis", "Voos", "Faturamento Total"]
sections_content = {section: [] for section in sections_order}

for table_name, config in TABLE_CONFIG.items():
    sections_content[config["section"]].append(table_name)

# --- Exibição do Dashboard ---

for section_name in sections_order:
    st.header(section_name)
    st.markdown("---")

    for table in sections_content[section_name]:
        config = TABLE_CONFIG[table]
        
        df = load_table(table, selected_company)

        if df.empty:
            st.subheader(config["title"])
            st.info("Nenhum dado encontrado para este cliente nesta tabela.")
        else:
            # Chama a função de plotagem específica para a tabela
            fig = config["plot_func"](df.copy(), config) # Passa uma cópia para evitar modificar o cached df

            if fig:
                st.plotly_chart(fig, use_container_width=True, key=f"{table}_plot_{selected_company}")
            else:
                st.warning(f"Não foi possível gerar o gráfico para a tabela: {table}")