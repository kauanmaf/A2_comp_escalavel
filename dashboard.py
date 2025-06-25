import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px
import calendar
import unicodedata

# --- Funções de Conexão e Carregamento de Dados ---

def get_pg_connection():
    """Retorna uma conexão com o banco de dados PostgreSQL."""
    # Configuração para AWS (padrão)
    return psycopg2.connect(
        host=os.getenv("PG_STATS_HOST", "a2-comp-escalavel-dados-estatisticas.col2wfyf2csx.us-east-1.rds.amazonaws.com"),
        port=os.getenv("PG_STATS_PORT", "5432"),
        dbname=os.getenv("PG_STATS_DB", "postgres"),
        user=os.getenv("PG_STATS_USER", "A2CompEscalavel"),
        password=os.getenv("PG_STATS_PASSWORD", "euadoroaemap"),
    )

    # Configuração para ambiente local (comentada)
    # return psycopg2.connect(
    #     host=os.getenv("PG_STATS_HOST", "localhost"),
    #     port=os.getenv("PG_STATS_PORT", "5433"),
    #     dbname=os.getenv("PG_STATS_DB", "dados_stats"),
    #     user=os.getenv("PG_STATS_USER", "emap"),
    #     password=os.getenv("PG_STATS_PASSWORD", "emap123"),
    # )

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

# --- Paleta de Cores Reduzida ---
# Definimos as cores que serão usadas em todo o dashboard
COLOR_PALETTE = {
    "primary_blue": "#3498DB", # Azul Escuro
    "emerald_green": "#2ECC71", # Verde Esmeralda
    "vibrant_orange": "#E67E22", # Laranja Vibrante
    "soft_red": "#E74C3C",   # Vermelho Suave
    "neutral_grey": "#BDC3C7" # Cinza Neutro
}

# --- Funções de Plotagem Específicas para Cada Tabela ---

def plot_stats_month_hotel(df, config):
    """Plota os valores de hotéis por mês de reserva como gráfico de linhas."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['sum_valor'] = df['sum_valor'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.line(df, x='month_name', y='sum_valor', title=config["title"], markers=True)
    fig.update_traces(line_color=COLOR_PALETTE["vibrant_orange"]) # Laranja
    fig.update_layout(xaxis_title="Mês de Reserva", yaxis_title="Valor Total")
    return fig

def plot_stats_month_hotel_count(df, config):
    """Plota a quantidade de hotéis reservados por mês como gráfico de barras."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['num_hoteis_reservados'] = df['num_hoteis_reservados'].fillna(0).astype(int)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.bar(df, x='month_name', y='num_hoteis_reservados', title=config["title"])
    fig.update_traces(marker_color=COLOR_PALETTE["emerald_green"]) # Verde
    fig.update_layout(xaxis_title="Mês de Reserva", yaxis_title="Número de Hotéis Reservados")
    return fig

def plot_stats_city_hotel(df, config):
    """Plota até 10 cidades com maior faturamento em hotéis (barras horizontais, sempre ao menos 4 cidades: SP, RJ, BH, Salvador)."""
    def normalize(s):
        return unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII').lower()

    top_n = min(config.get("top_n", 10), 10)
    cidades_extras = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Salvador"]

    cidades_presentes_norm = [normalize(c) for c in df['cidade'].unique()]
    cidades_faltando = [c for c in cidades_extras if normalize(c) not in cidades_presentes_norm]
    df_extra = pd.DataFrame({'cidade': cidades_faltando, 'sum_valor': [0] * len(cidades_faltando)})
    df = pd.concat([df, df_extra], ignore_index=True)

    df_sorted = df.sort_values(by='sum_valor', ascending=True).head(top_n)

    for cidade in cidades_extras:
        if normalize(cidade) not in [normalize(c) for c in df_sorted['cidade']]:
            df_sorted = pd.concat([df_sorted, pd.DataFrame({'cidade': [cidade], 'sum_valor': [0]})], ignore_index=True)

    seen = set()
    rows = []
    for _, row in df_sorted.iterrows():
        norm = normalize(row['cidade'])
        if norm not in seen:
            seen.add(norm)
            rows.append(row)
    df_sorted = pd.DataFrame(rows)
    df_sorted = df_sorted.head(top_n)

    fig = px.bar(
        df_sorted,
        x='sum_valor',
        y='cidade',
        orientation='h',
        title=config["title"],
        color_discrete_sequence=[COLOR_PALETTE["primary_blue"]] # Usa uma única cor para as barras
    )
    fig.update_layout(
        xaxis_title="Faturamento",
        yaxis_title="Cidade"
    )
    return fig

def plot_stats_month_voos(df, config):
    """Plota os maiores faturamentos de voos por mês de reserva."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['sum_valor'] = df['sum_valor'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.line(df, x='month_name', y='sum_valor', title=config["title"])
    fig.update_traces(line_color=COLOR_PALETTE["primary_blue"]) # Azul
    fig.update_layout(xaxis_title="Mês de Reserva" , yaxis_title = "Faturamento")
    return fig

def plot_stats_city_voos(df, config):
    """Plota até 10 cidades com maior faturamento em voos (barras horizontais)."""
    df_sorted = df.sort_values(by='sum_valor', ascending=True).head(10)
    fig = px.bar(
        df_sorted,
        x='sum_valor',
        y='cidade_destino',
        orientation='h',
        title=config["title"],
        color_discrete_sequence=[COLOR_PALETTE["primary_blue"]] # Usa uma única cor para as barras
    )
    fig.update_layout(
        xaxis_title="Faturamento",
        yaxis_title="Cidade de Destino"
    )
    return fig

def plot_stats_faturamentos_totais(df, config):
    """Plota os faturamentos totais (voos e hotéis) por mês de reserva."""
    df['mes_reserva'] = pd.to_numeric(df['mes_reserva'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes_reserva', how='left')
    df['total_valor'] = df['total_valor'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.line(df, x='month_name', y='total_valor', title=config["title"])
    fig.update_traces(line_color=COLOR_PALETTE["emerald_green"]) # Verde
    fig.update_layout(xaxis_title="Mês de Reserva")
    return fig

def plot_stats_stars_hotel(df, config):
    """Plota o número de reservas por estrelas de hotel (sempre mostra 1 a 5 no eixo X)."""
    star_order = [1, 2, 3, 4, 5]
    full_stars_df = pd.DataFrame({'estrelas': star_order})
    df = pd.merge(full_stars_df, df, on='estrelas', how='left')
    df['num_reservas'] = df['num_reservas'].fillna(0).astype(int)
    df['estrelas'] = pd.Categorical(df['estrelas'], categories=star_order, ordered=True)
    fig = px.bar(df, x='estrelas', y='num_reservas', title=config["title"],
                 color='estrelas', # Color by number of stars
                 color_discrete_map={ # Mapeamento específico para estrelas
                     1: COLOR_PALETTE["soft_red"],
                     2: COLOR_PALETTE["soft_red"],
                     3: COLOR_PALETTE["vibrant_orange"],
                     4: COLOR_PALETTE["primary_blue"],
                     5: COLOR_PALETTE["emerald_green"]
                 })
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=star_order)
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
    fig = px.line(df, x='month_name', y='media_estrelas', title=config["title"])
    fig.update_traces(line_color=COLOR_PALETTE["soft_red"]) # Vermelho
    fig.update_layout(xaxis_title="Mês de Reserva", yaxis_title = "Média de Estrelas")
    return fig

def plot_stats_month_sp_voos(df, config):
    """Plota a quantidade de voos reservados por mês em SP."""
    df['mes'] = pd.to_numeric(df['mes'], errors='coerce').fillna(0).astype(int)
    full_months_df = pd.DataFrame({'month_num': range(1, 13)})
    df = pd.merge(full_months_df, df, left_on='month_num', right_on='mes', how='left')
    df['num_voos_reservados'] = df['num_voos_reservados'].fillna(0)
    df['month_name'] = df['month_num'].apply(get_month_name)
    df = df.sort_values(by='month_num')
    fig = px.line(df, x='month_name', y='num_voos_reservados', title=config["title"])
    fig.update_traces(line_color=COLOR_PALETTE["primary_blue"]) # Azul
    fig.update_layout(xaxis_title="Mês de Reserva")
    return fig

def plot_stats_day_sp_voos(df, config):
    """Plota a média móvel dos últimos 7 dias para o número de reservas de voos em SP."""
    df = df.copy()

    # Ensure 'data_reserva' is a datetime object
    df['data_reserva'] = pd.to_datetime(df['data_reserva'])

    # Sort by date for correct rolling average calculation
    df = df.sort_values(by='data_reserva')

    # Calculate the 7-day rolling average
    # min_periods=1 ensures that we get a value for the first 6 days, even if there aren't 7 full days yet
    df['media_movel_7dias'] = df['num_reservas'].rolling(window=7, min_periods=1).mean()

    # Plotting the rolling average as a line chart
    fig = px.line(
        df,
        x='data_reserva',
        y='media_movel_7dias',
        title=config["title"],
        markers=False # Typically, rolling averages don't need markers
    )
    fig.update_traces(line_color=COLOR_PALETTE["primary_blue"]) # Consistent blue for flight trends
    fig.update_layout(
        xaxis_title="Data da Reserva",
        yaxis_title="Média Móvel (7 dias) de Reservas",
        hovermode="x unified" # Improves tooltip experience
    )
    return fig


# Mapeamento de tabelas e suas configurações (agora com referência direta às funções)
TABLE_CONFIG = {
    "stats_month_hotel": {
        "title": "Valores de Hotéis por Mês de Reserva", "section": "Hoteis",
        "plot_func": plot_stats_month_hotel
    },
    "stats_month_hotel_count": {
        "title": "Número de Hotéis Reservados por Mês", "section": "Hoteis",
        "plot_func": plot_stats_month_hotel_count
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
        "title": "Média Móvel de 7 Dias de Reservas de Voos em SP", "section": "Voos", # Updated title for clarity
        "top_n": 10, "plot_func": plot_stats_day_sp_voos
    },
}

# --- Configuração da Página Streamlit ---

st.set_page_config(layout="wide")
st.title("📊 Dashboard de Estatísticas por Cliente")

# --- Seleção de Company ID ---

try:
    conn = get_pg_connection()
    company_ids_df = pd.read_sql("SELECT DISTINCT company_id FROM stats_month_hotel", conn)
    company_ids = company_ids_df["company_id"].tolist()
    conn.close()
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
