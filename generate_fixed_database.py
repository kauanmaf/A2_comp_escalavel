import random
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from faker import Faker
import os
import sys
import time
from typing import List, Tuple
import boto3

# Configurações S3
S3_BUCKET = os.getenv('S3_MASTER_DATA_BUCKET', 'a2-comp-escalavel-dados-brutos')
S3_PREFIX_HOTEIS = os.getenv('S3_PREFIX_HOTEIS', 'master_data/hoteis/')
S3_PREFIX_VOOS = os.getenv('S3_PREFIX_VOOS', 'master_data/voos/')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

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
            for cidade_origem in CIDADES_BRASILEIRAS:
                for cidade_destino in CIDADES_BRASILEIRAS:
                    if cidade_origem != cidade_destino:  # Não criar voos para a mesma cidade

                        # Número de voos por rota
                        num_voos = random.randint(1, 2)

                        # Criar voos para esta rota neste dia
                        for _ in range(num_voos):
                            voo = {
                                'id_voo': id_voo,
                                'cidade_origem': cidade_origem,
                                'cidade_destino': cidade_destino,
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
            print(f"Rotas únicas: {len(self.voos_df[['cidade_origem', 'cidade_destino']].drop_duplicates()):,}")
            print(f"Voos por dia (média): {len(self.voos_df) / 362:.1f}")  # 362 dias

    def save_to_s3(self):
        """Salva os DataFrames gerados no S3."""
        # Salva Hoteis
        if self.hoteis_df is not None:
            print(f"Salvando dados de hotéis no S3: s3://{S3_BUCKET}/{S3_PREFIX_HOTEIS}hoteis.parquet...")
            self.hoteis_df.to_parquet(f"s3://{S3_BUCKET}/{S3_PREFIX_HOTEIS}hoteis.parquet", index=False)
            print(f"Hoteis salvos em s3://{S3_BUCKET}/{S3_PREFIX_HOTEIS}hoteis.parquet")

        # Salva Voos
        if self.voos_df is not None:
            print(f"Salvando dados de voos no S3: s3://{S3_BUCKET}/{S3_PREFIX_VOOS}voos.parquet...")
            self.voos_df.to_parquet(f"s3://{S3_BUCKET}/{S3_PREFIX_VOOS}voos.parquet", index=False)
            print(f"Voos salvos em s3://{S3_BUCKET}/{S3_PREFIX_VOOS}voos.parquet")


def main():
    print("🚀 === GERADOR DE BASE DE DADOS FIXA PARA S3 ===")
    print("Gerando dados para sistema de viagens e salvando no S3...\n")

    generator = DatabaseGenerator()

    print("Gerando novos dados...")
    generator.generate_hoteis()
    generator.generate_voos()

    # Mostrar estatísticas dos dados gerados (opcional)
    generator.generate_statistics()

    # Salvar no S3
    print("\n=== SALVANDO NO S3 ===")
    try:
        generator.save_to_s3()
        print("\n=== PROCESSO CONCLUÍDO COM SUCESSO ===")
        print("Os dados estão prontos para uso no S3!")
    except Exception as e:
        print(f"=== PROCESSO FALHOU AO SALVAR NO S3: {e} ===")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
