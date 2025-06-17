import random
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from faker import Faker
import os
import sys
import time
from typing import List, Tuple

# Configuração
fake = Faker('pt_BR')
random.seed(42)  # Para resultados reproduzíveis

# Configurações da base de dados
DB_CONFIG = {
    'host': os.getenv('PG_DATA_HOST', 'localhost'),
    'port': int(os.getenv('PG_DATA_PORT', 5432)),
    'database': os.getenv('PG_DATA_DB', 'dados_gerais'),
    'user': os.getenv('PG_DATA_USER', 'emap'),
    'password': os.getenv('PG_DATA_PASSWORD', 'emap123')
}

# 50 cidades brasileiras principais
CIDADES_BRASILEIRAS = [
    "São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Belo Horizonte",
    "Fortaleza", "Manaus", "Curitiba", "Recife", "Goiânia",
    "Belém", "Porto Alegre", "Guarulhos", "Campinas", "São Luís",
    "São Gonçalo", "Maceió", "Duque de Caxias", "Natal", "Teresina",
    "Campo Grande", "Nova Iguaçu", "São Bernardo do Campo", "João Pessoa", "Santo André",
    "Osasco", "Jaboatão dos Guararapes", "São José dos Campos", "Ribeirão Preto", "Uberlândia",
    "Sorocaba", "Contagem", "Aracaju", "Feira de Santana", "Cuiabá",
    "Joinville", "Aparecida de Goiânia", "Londrina", "Juiz de Fora", "Ananindeua",
    "Porto Velho", "Serra", "Niterói", "Caxias do Sul", "Campos dos Goytacazes",
    "Vila Velha", "Florianópolis", "Mauá", "Carapicuíba", "Olinda"
]

class DatabaseGenerator:
    def __init__(self):
        self.hoteis_df = None
        self.voos_df = None

    def wait_for_database(self, max_attempts=30, delay=5):
        """Aguarda o banco de dados ficar disponível"""
        print(f"Aguardando conexão com PostgreSQL em {DB_CONFIG['host']}:{DB_CONFIG['port']}...")

        for attempt in range(max_attempts):
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                conn.close()
                print("✅ Conexão com PostgreSQL estabelecida!")
                return True
            except psycopg2.Error as e:
                print(f"⏳ Tentativa {attempt + 1}/{max_attempts} falhou: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(delay)

        print("❌ Falha ao conectar com PostgreSQL após todas as tentativas!")
        return False

    def check_tables_exist(self):
        """Verifica se as tabelas já existem com dados"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Verificar se tabela de hotéis existe e tem dados
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'hoteis'
                );
            """)
            hoteis_exists = cursor.fetchone()[0]

            if hoteis_exists:
                cursor.execute("SELECT COUNT(*) FROM hoteis;")
                hoteis_count = cursor.fetchone()[0]
            else:
                hoteis_count = 0

            # Verificar se tabela de voos existe e tem dados
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'voos'
                );
            """)
            voos_exists = cursor.fetchone()[0]

            if voos_exists:
                cursor.execute("SELECT COUNT(*) FROM voos;")
                voos_count = cursor.fetchone()[0]
            else:
                voos_count = 0

            cursor.close()
            conn.close()

            if hoteis_count > 0 and voos_count > 0:
                print(f"📊 Dados já existem: {hoteis_count:,} hotéis e {voos_count:,} voos")
                return True

            return False

        except psycopg2.Error as e:
            print(f"Erro ao verificar tabelas: {e}")
            return False

    def generate_hoteis(self) -> pd.DataFrame:
        """Gera dados para a tabela de hotéis"""
        print("Gerando dados de hotéis...")

        hoteis_data = []
        id_hotel = 1

        # Distribuição de estrelas conforme especificado:
        # 1 estrela: 10%, 2 estrelas: 15%, 3 estrelas: 35%, 4 estrelas: 25%, 5 estrelas: 15%
        estrelas_opcoes = [1, 2, 3, 4, 5]
        estrelas_pesos = [10, 15, 35, 25, 15]  # Percentuais

        for cidade in CIDADES_BRASILEIRAS:
            # 30-50 hotéis por cidade
            num_hoteis = random.randint(50, 250)

            for i in range(num_hoteis):
                # Escolher estrelas baseado na distribuição desejada
                estrelas = random.choices(estrelas_opcoes, weights=estrelas_pesos, k=1)[0]

                hotel = {
                    'id_hotel': id_hotel,
                    'cidade': cidade,
                    'estrelas': estrelas
                }
                hoteis_data.append(hotel)
                id_hotel += 1

        self.hoteis_df = pd.DataFrame(hoteis_data)
        print(f"Gerados {len(self.hoteis_df)} hotéis em {len(CIDADES_BRASILEIRAS)} cidades")
        return self.hoteis_df

    def is_weekend(self, date: datetime) -> bool:
        """Verifica se a data é fim de semana (sábado=5, domingo=6)"""
        return date.weekday() >= 5

    def generate_voos(self) -> pd.DataFrame:
        """Gera dados para a tabela de voos"""
        print("Gerando dados de voos...")

        voos_data = []
        id_voo = 1

        # Gerar voos para todos os dias de 2025
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 12, 28)

        current_date = start_date
        while current_date <= end_date:
            # Para cada par de cidades
            for cidade_ida in CIDADES_BRASILEIRAS:
                for cidade_volta in CIDADES_BRASILEIRAS:
                    if cidade_ida != cidade_volta:  # Não criar voos para a mesma cidade

                        # Número de voos por rota
                        num_voos = random.randint(1, 2)

                        # Criar voos para esta rota neste dia
                        for _ in range(num_voos):
                            voo = {
                                'id_voo': id_voo,
                                'cidade_ida': cidade_ida,
                                'cidade_volta': cidade_volta,
                                'dia': current_date.day,
                                'mes': current_date.month,
                                'ano': current_date.year
                            }
                            voos_data.append(voo)
                            id_voo += 1

            current_date += timedelta(days=1)

            # Progresso a cada 30 dias
            if current_date.day == 1:
                print(f"Processando {current_date.strftime('%B %Y')}...")

        self.voos_df = pd.DataFrame(voos_data)
        print(f"Gerados {len(self.voos_df)} voos para o ano de 2025")
        return self.voos_df

    def create_tables_sql(self) -> str:
        """Gera SQL para criar as tabelas"""
        sql = """
        -- Criar tabela de hotéis
        DROP TABLE IF EXISTS hoteis CASCADE;
        CREATE TABLE hoteis (
            id_hotel INTEGER PRIMARY KEY,
            cidade VARCHAR(100) NOT NULL,
            estrelas INTEGER CHECK (estrelas >= 1 AND estrelas <= 5)
        );

        -- Criar índice para cidade na tabela hotéis
        CREATE INDEX idx_hoteis_cidade ON hoteis(cidade);

        -- Criar tabela de voos
        DROP TABLE IF EXISTS voos CASCADE;
        CREATE TABLE voos (
            id_voo INTEGER PRIMARY KEY,
            cidade_ida VARCHAR(100) NOT NULL,
            cidade_volta VARCHAR(100) NOT NULL,
            dia INTEGER CHECK (dia >= 1 AND dia <= 31),
            mes INTEGER CHECK (mes >= 1 AND mes <= 12),
            ano INTEGER CHECK (ano > 0)
        );

        -- Criar índices para otimizar consultas na tabela voos
        CREATE INDEX idx_voos_cidade_ida ON voos(cidade_ida);
        CREATE INDEX idx_voos_cidade_volta ON voos(cidade_volta);
        CREATE INDEX idx_voos_data ON voos(ano, mes, dia);
        CREATE INDEX idx_voos_rota ON voos(cidade_ida, cidade_volta);
        """
        return sql

    def insert_to_database(self):
        """Insere os dados diretamente no PostgreSQL"""
        try:
            print("Conectando ao PostgreSQL...")
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Criar tabelas
            print("Criando tabelas...")
            cursor.execute(self.create_tables_sql())
            conn.commit()

            # Inserir hotéis
            if self.hoteis_df is not None:
                print("Inserindo dados de hotéis...")
                hoteis_tuples = [tuple(row) for row in self.hoteis_df.to_numpy()]
                cursor.executemany(
                    "INSERT INTO hoteis (id_hotel, cidade, estrelas) VALUES (%s, %s, %s)",
                    hoteis_tuples
                )
                conn.commit()
                print(f"✅ Inseridos {len(hoteis_tuples):,} hotéis")

            # Inserir voos (em lotes para performance)
            if self.voos_df is not None:
                print("Inserindo dados de voos...")
                voos_tuples = [tuple(row) for row in self.voos_df.to_numpy()]

                # Inserir em lotes de 10000 registros
                batch_size = 10000
                for i in range(0, len(voos_tuples), batch_size):
                    batch = voos_tuples[i:i + batch_size]
                    cursor.executemany(
                        "INSERT INTO voos (id_voo, cidade_ida, cidade_volta, dia, mes, ano) VALUES (%s, %s, %s, %s, %s, %s)",
                        batch
                    )
                    conn.commit()
                    print(f"⏳ Inseridos {min(i + batch_size, len(voos_tuples)):,} de {len(voos_tuples):,} voos")

                print(f"✅ Todos os {len(voos_tuples):,} voos inseridos com sucesso!")

            cursor.close()
            conn.close()
            print("🎉 Dados inseridos com sucesso no PostgreSQL!")
            return True

        except psycopg2.Error as e:
            print(f"❌ Erro ao conectar ou inserir no PostgreSQL: {e}")
            return False

    def generate_statistics(self):
        """Gera estatísticas dos dados gerados"""
        if self.hoteis_df is not None:
            print("\n=== ESTATÍSTICAS DOS HOTÉIS ===")
            print(f"Total de hotéis: {len(self.hoteis_df):,}")
            print(f"Cidades com hotéis: {self.hoteis_df['cidade'].nunique()}")
            print("\nDistribuição por estrelas:")
            estrelas_stats = self.hoteis_df['estrelas'].value_counts().sort_index()
            total_hoteis = len(self.hoteis_df)
            for estrela, count in estrelas_stats.items():
                percentual = (count / total_hoteis) * 100
                print(f"{estrela}★: {count:,} ({percentual:.1f}%)")
            print(f"\nMédia de hotéis por cidade: {len(self.hoteis_df) / len(CIDADES_BRASILEIRAS):.1f}")

        if self.voos_df is not None:
            print("\n=== ESTATÍSTICAS DOS VOOS ===")
            print(f"Total de voos: {len(self.voos_df):,}")
            print(f"Rotas únicas: {len(self.voos_df[['cidade_ida', 'cidade_volta']].drop_duplicates()):,}")
            print(f"Voos por dia (média): {len(self.voos_df) / 362:.1f}")  # 362 dias

def main():
    """Função principal"""
    print("🚀 === GERADOR DE BASE DE DADOS FIXA ===")
    print("Gerando dados para sistema de viagens...\n")

    generator = DatabaseGenerator()

    # Aguardar banco de dados ficar disponível
    if not generator.wait_for_database():
        print("❌ Falha ao conectar com o banco de dados. Saindo...")
        sys.exit(1)

    # Verificar se os dados já existem
    if generator.check_tables_exist():
        print("📊 Os dados já foram carregados anteriormente. Nada a fazer.")
        return

    print("📝 Gerando novos dados...")

    # Gerar dados
    generator.generate_hoteis()
    generator.generate_voos()

    # Mostrar estatísticas
    generator.generate_statistics()

    # Inserir no PostgreSQL
    print("\n=== INSERINDO NO POSTGRESQL ===")
    success = generator.insert_to_database()

    if success:
        print("\n🎉 === PROCESSO CONCLUÍDO COM SUCESSO ===")
        print("Os dados estão prontos para uso!")
    else:
        print("\n❌ === PROCESSO FALHOU ===")
        sys.exit(1)

if __name__ == "__main__":
    main()
