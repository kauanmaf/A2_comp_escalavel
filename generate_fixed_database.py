import random
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from faker import Faker
import os
import sys
import time
from typing import List, Tuple
# Importar create_engine para pandas to_sql
from sqlalchemy import create_engine
import boto3 # Manter para o S3, caso ainda queira salvar lá ou para credenciais

# Configurações S3 (Manter, mas o foco agora é RDS)
S3_BUCKET = os.getenv('S3_MASTER_DATA_BUCKET', 'a2-comp-escalavel-dados-fixos') #
S3_PREFIX_HOTEIS = os.getenv('S3_PREFIX_HOTEIS', 'master_data/hoteis/') #
S3_PREFIX_VOOS = os.getenv('S3_PREFIX_VOOS', 'master_data/voos/') #
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1') #

# Configuração (Manter)
fake = Faker('pt_BR') #
random.seed(42)  # Para resultados reproduzíveis #

# Configurações da base de dados (RDS)
DB_CONFIG = {
    'host': os.getenv('PG_DATA_HOST', 'localhost'), #
    'port': int(os.getenv('PG_DATA_PORT', 5432)), #
    'database': os.getenv('PG_DATA_DB', 'dados_gerais'), #
    'user': os.getenv('PG_DATA_USER', 'emap'), #
    'password': os.getenv('PG_DATA_PASSWORD', 'emap123') #
}

# CIDADES_BRASILEIRAS (Manter)
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
    def __init__(self): #
        self.hoteis_df = None #
        self.voos_df = None #
        # Adicionar a engine de conexão com o banco de dados
        self.pg_engine = create_engine(
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

    def generate_hoteis(self) -> pd.DataFrame: #
        """Gera dados para a tabela de hotéis"""
        print("Gerando dados de hotéis...") #

        hoteis_data = [] #
        id_hotel = 1 #

        estrelas_opcoes = [1, 2, 3, 4, 5] #
        estrelas_pesos = [10, 15, 35, 25, 15]  # Percentuais

        for cidade in CIDADES_BRASILEIRAS: #
            num_hoteis = random.randint(50, 250) #

            for i in range(num_hoteis): #
                estrelas = random.choices(estrelas_opcoes, weights=estrelas_pesos, k=1)[0] #

                hotel = {
                    'id_hotel': id_hotel, #
                    'cidade': cidade, #
                    'estrelas': estrelas #
                }
                hoteis_data.append(hotel) #
                id_hotel += 1 #

        self.hoteis_df = pd.DataFrame(hoteis_data) #
        print(f"Gerados {len(self.hoteis_df)} hotéis em {len(CIDADES_BRASILEIRAS)} cidades") #
        return self.hoteis_df #

    def is_weekend(self, date: datetime) -> bool: #
        """Verifica se a data é fim de semana (sábado=5, domingo=6)"""
        return date.weekday() >= 5 #

    def generate_voos(self) -> pd.DataFrame: #
        """Gera dados para a tabela de voos"""
        print("Gerando dados de voos...") #

        voos_data = [] #
        id_voo = 1 #

        start_date = datetime(2025, 1, 1) #
        end_date = datetime(2025, 12, 28) #

        current_date = start_date #
        while current_date <= end_date: #
            for cidade_origem in CIDADES_BRASILEIRAS: #
                for cidade_destino in CIDADES_BRASILEIRAS: #
                    if cidade_origem != cidade_destino: #

                        num_voos = random.randint(1, 2) #

                        for _ in range(num_voos): #
                            voo = {
                                'id_voo': id_voo, #
                                'cidade_origem': cidade_origem, #
                                'cidade_destino': cidade_destino, #
                                'dia': current_date.day, #
                                'mes': current_date.month, #
                                'ano': current_date.year #
                            }
                            voos_data.append(voo) #
                            id_voo += 1 #

            current_date += timedelta(days=1) #

            if current_date.day == 1: #
                print(f"Processando {current_date.strftime('%B %Y')}...") #

        self.voos_df = pd.DataFrame(voos_data) #
        print(f"Gerados {len(self.voos_df)} voos para o ano de 2025") #
        return self.voos_df #

    def generate_statistics(self): #
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

        if self.voos_df is not None: #
            print("\n=== ESTATÍSTICAS DOS VOOS ===") #
            print(f"Total de voos: {len(self.voos_df):,}") #
            print(f"Rotas únicas: {len(self.voos_df[['cidade_origem', 'cidade_destino']].drop_duplicates()):,}") #
            print(f"Voos por dia (média): {len(self.voos_df) / 362:.1f}") # 362 dias

    def save_to_s3(self): #
        """Salva os DataFrames gerados no S3."""
        # Salva Hoteis
        if self.hoteis_df is not None: #
            print(f"Salvando dados de hotéis no S3: s3://{S3_BUCKET}/{S3_PREFIX_HOTEIS}hoteis.parquet...") #
            self.hoteis_df.to_parquet(f"s3://{S3_BUCKET}/{S3_PREFIX_HOTEIS}hoteis.parquet", index=False) #
            print(f"Hoteis salvos em s3://{S3_BUCKET}/{S3_PREFIX_HOTEIS}hoteis.parquet") #

        # Salva Voos
        if self.voos_df is not None: #
            print(f"Salvando dados de voos no S3: s3://{S3_BUCKET}/{S3_PREFIX_VOOS}voos.parquet...") #
            self.voos_df.to_parquet(f"s3://{S3_BUCKET}/{S3_PREFIX_VOOS}voos.parquet", index=False) #
            print(f"Voos salvos em s3://{S3_BUCKET}/{S3_PREFIX_VOOS}voos.parquet") #

    def save_to_rds(self):
        """Salva os DataFrames gerados nas tabelas do RDS."""
        print("\n=== SALVANDO NO RDS ===")
        try:
            if self.hoteis_df is not None:
                print("Inserindo dados de hotéis no RDS...")
                self.hoteis_df.to_sql('hoteis', self.pg_engine, if_exists='replace', index=False)
                print(f"{len(self.hoteis_df)} registros de hotéis inseridos no RDS.")

            if self.voos_df is not None:
                print("Inserindo dados de voos no RDS...")
                self.voos_df.to_sql('voos', self.pg_engine, if_exists='replace', index=False)
                print(f"{len(self.voos_df)} registros de voos inseridos no RDS.")
            print("Dados salvos no RDS com sucesso!")
        except Exception as e:
            print(f"Erro ao salvar dados no RDS: {e}")
            raise # Levanta a exceção para que o bloco try/except em main capture

    def create_tables_if_not_exists(self):
        """Cria as tabelas hoteis e voos no RDS se não existirem."""
        conn = None
        try:
            # Estabelece uma conexão direta com psycopg2 para executar DDL
            conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            cur = conn.cursor()

            print("Verificando e criando tabela 'hoteis'...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hoteis (
                    id_hotel INT PRIMARY KEY,
                    cidade VARCHAR(255),
                    estrelas INT
                );
            """)
            print("Tabela 'hoteis' verificada/criada.")

            print("Verificando e criando tabela 'voos'...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS voos (
                    id_voo INT PRIMARY KEY,
                    cidade_origem VARCHAR(255),
                    cidade_destino VARCHAR(255),
                    dia INT,
                    mes INT,
                    ano INT
                );
            """)
            print("Tabela 'voos' verificada/criada.")

            conn.commit()
            cur.close()
        except Exception as e:
            print(f"Erro ao criar tabelas no RDS: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

def main(): #
    print("🚀 === GERADOR DE BASE DE DADOS FIXA PARA S3 ===") #
    print("Gerando dados para sistema de viagens e salvando no S3...\n") #

    generator = DatabaseGenerator() #

    print("Gerando novos dados...") #
    generator.generate_hoteis() #
    generator.generate_voos() #

    # Mostrar estatísticas dos dados gerados (opcional)
    generator.generate_statistics() #

    # --- NOVO: Salvar no RDS em vez de S3 ---
    print("\n=== SALVANDO NO RDS ===")
    try:
        generator.create_tables_if_not_exists()
        generator.save_to_rds()
        print("\n=== PROCESSO CONCLUÍDO COM SUCESSO ===")
        print("Os dados estão prontos para uso no RDS!")
    except Exception as e:
        print(f"=== PROCESSO FALHOU AO SALVAR NO RDS: {e} ===")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__": #
    main() #