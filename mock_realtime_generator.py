#!/usr/bin/env python3
"""
Gerador de dados em tempo real para reservas de voos e hotéis
Usa threading para gerar dados independentemente em duas threads
Baseado nos dados reais da base de dados PostgreSQL
VERSÃO CSV: Salva dados em arquivo CSV
"""

import threading
import time
import random
import csv
from datetime import datetime, timedelta
from typing import Dict, Any
import uuid
import os

# Configuração de quantidade por minuto
FLIGHTS_PER_MINUTE = 240  # Voos por minuto
HOTELS_PER_MINUTE = 240    # Hotéis por minuto (na verdade é metade do total de hotéis) =)

# Ranges baseados nos dados gerados pelo generate_fixed_database.py
# Como foram geradas 50 cidades com 50-250 hotéis cada, temos aproximadamente 7000 hotéis
HOTEL_ID_RANGE = (1, 7000)  # Range de IDs de hotéis
VOO_ID_RANGE = (1, 1340000)  # Range de IDs de voos (aproximadamente 1.34M voos)

# Arquivos CSV de saída
CSV_FLIGHTS_FILE = "reservas_voos.csv"
CSV_HOTELS_FILE = "reservas_hoteis.csv"

class CSVDataGenerator:
    def __init__(self):
        """Inicializa o gerador de dados CSV"""
        print("🚀 === GERADOR CSV DE DADOS ===")
        print("📁 Salvando em arquivos CSV")

        self.running = True
        self.stats = {
            'voos_gerados': 0,
            'hoteis_gerados': 0,
            'start_time': datetime.now()
        }

        # Lock para escrita thread-safe nos CSVs
        self.csv_lock = threading.Lock()

        # Calcular intervalos baseados na quantidade por minuto
        self.flight_interval = 60.0 / FLIGHTS_PER_MINUTE  # segundos entre voos
        self.hotel_interval = 60.0 / HOTELS_PER_MINUTE    # segundos entre hotéis

        # Inicializar arquivos CSV
        self.init_csv_files()

    def init_csv_files(self):
        """Inicializa os arquivos CSV com cabeçalhos"""
        # CSV de voos
        with open(CSV_FLIGHTS_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'id_voo', 'id_reserva_voo', 'valor', 'data_reserva'])

        # CSV de hotéis
        with open(CSV_HOTELS_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'id_hotel', 'id_reserva_hotel', 'valor', 'data_reservada', 'data_reserva'])

        print(f"✅ Arquivos CSV inicializados:")
        print(f"   🛫 {CSV_FLIGHTS_FILE}")
        print(f"   🏨 {CSV_HOTELS_FILE}")

    def generate_flight_reservation(self) -> Dict[str, Any]:
        """Gera dados de uma reserva de voo"""
        # Seleciona um ID de voo aleatório do range
        id_voo = random.randint(*VOO_ID_RANGE)

        # Gera ID único para a reserva
        id_reserva_voo = f"RV-{uuid.uuid4().hex[:8].upper()}"

        # Valor da reserva
        valor = round(random.uniform(300, 2500), 2)

        # Data da reserva (quando foi feita a reserva)
        # Pode ser hoje ou nos últimos 30 dias
        dias_atras = random.randint(0, 30)
        data_reserva = datetime.now() - timedelta(days=dias_atras)

        return {
            "id_voo": id_voo,
            "id_reserva_voo": id_reserva_voo,
            "valor": valor,
            "data_reserva": data_reserva.isoformat()
        }

    def generate_hotel_reservation(self) -> list:
        """Gera dados de uma reserva de hotel - RETORNA LISTA DE DATAS"""
        # Seleciona um ID de hotel aleatório do range
        id_hotel = random.randint(*HOTEL_ID_RANGE)

        # Valor da reserva
        valor = round(random.uniform(150, 2000), 2)

        # Data da reserva (quando foi feita a reserva)
        # Pode ser hoje ou nos últimos 15 dias
        dias_atras = random.randint(0, 15)
        data_reserva = datetime.now() - timedelta(days=dias_atras)

        # Data(s) reservada(s) - pode ser de 1 a 3 dias consecutivos
        num_dias = random.randint(1, 3)

        # Data inicial da estadia (entre hoje e próximos 90 dias)
        dias_futuros = random.randint(0, 90)
        data_inicial = datetime.now() + timedelta(days=dias_futuros)

        # Gerar lista de registros, um para cada data
        registros = []
        for i in range(num_dias):
            # ID único para cada data reservada
            id_reserva_hotel = f"RH-{uuid.uuid4().hex[:8].upper()}"
            data_estadia = data_inicial + timedelta(days=i)

            registros.append({
                "id_hotel": id_hotel,
                "id_reserva_hotel": id_reserva_hotel,
                "valor": valor,  # Mesmo valor para todas as datas da mesma reserva
                "data_reservada": data_estadia.strftime('%Y-%m-%d'),
                "data_reserva": data_reserva.isoformat()
            })

        return registros

    def save_flight_to_csv(self, data_payload: Dict[str, Any]):
        """Salva dados de voo no CSV"""
        timestamp = datetime.now().isoformat()

        with self.csv_lock:
            with open(CSV_FLIGHTS_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp,
                    data_payload['id_voo'],
                    data_payload['id_reserva_voo'],
                    data_payload['valor'],
                    data_payload['data_reserva']
                ])

    def save_hotels_to_csv(self, registros: list):
        """Salva múltiplos registros de hotel no CSV (um por data)"""
        timestamp = datetime.now().isoformat()

        with self.csv_lock:
            with open(CSV_HOTELS_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for registro in registros:
                    writer.writerow([
                        timestamp,
                        registro['id_hotel'],
                        registro['id_reserva_hotel'],
                        registro['valor'],
                        registro['data_reservada'],  # Agora é uma data única
                        registro['data_reserva']
                    ])

    def flight_generator_thread(self):
        """Thread para gerar dados de reservas de voos"""
        print("🛫 Thread de voos iniciada")

        while self.running:
            try:
                # Gera dados da reserva de voo
                flight_data = self.generate_flight_reservation()

                # Salva no CSV
                self.save_flight_to_csv(flight_data)

                # Incrementa contador
                self.stats['voos_gerados'] += 1

                # Intervalo fixo baseado na quantidade por minuto
                time.sleep(self.flight_interval)

            except Exception as e:
                print(f"❌ Erro na thread de voos: {e}")
                time.sleep(1)

    def hotel_generator_thread(self):
        """Thread para gerar dados de reservas de hotéis"""
        print("🏨 Thread de hotéis iniciada")

        while self.running:
            try:
                # Gera dados da reserva de hotel (lista de registros)
                hotel_registros = self.generate_hotel_reservation()

                # Salva no CSV (múltiplas linhas)
                self.save_hotels_to_csv(hotel_registros)

                # Incrementa contador (número de linhas criadas)
                self.stats['hoteis_gerados'] += len(hotel_registros)

                # Intervalo fixo baseado na quantidade por minuto
                time.sleep(self.hotel_interval)

            except Exception as e:
                print(f"❌ Erro na thread de hotéis: {e}")
                time.sleep(1)

    def stats_thread(self):
        """Thread para exibir estatísticas periodicamente"""
        while self.running:
            time.sleep(60)  # A cada 1 minuto

            if not self.running:
                break

            elapsed = datetime.now() - self.stats['start_time']
            elapsed_minutes = elapsed.total_seconds() / 60

            print(f"\n📊 === ESTATÍSTICAS ({elapsed_minutes:.1f} min) ===")
            print(f"🛫 Reservas de voos: {self.stats['voos_gerados']}")
            print(f"🏨 Linhas de hotéis: {self.stats['hoteis_gerados']}")
            print(f"📈 Total: {self.stats['voos_gerados'] + self.stats['hoteis_gerados']}")

            # Taxa por minuto
            if elapsed_minutes > 0:
                voos_per_min = self.stats['voos_gerados'] / elapsed_minutes
                hoteis_per_min = self.stats['hoteis_gerados'] / elapsed_minutes
                print(f"🚀 Taxa: {voos_per_min:.1f} voos/min, {hoteis_per_min:.1f} linhas hotel/min")

            # Tamanho dos arquivos
            try:
                flight_size = os.path.getsize(CSV_FLIGHTS_FILE) / 1024  # KB
                hotel_size = os.path.getsize(CSV_HOTELS_FILE) / 1024   # KB
                print(f"📁 Arquivos: {flight_size:.1f}KB (voos), {hotel_size:.1f}KB (hotéis)")
            except:
                pass

            print("=" * 70)

    def run(self):
        """Executa o gerador de dados CSV"""
        print("🚀 === GERADOR CSV DE DADOS ===")
        print(f"🏨 Range Hotéis: {HOTEL_ID_RANGE[0]:,} - {HOTEL_ID_RANGE[1]:,}")
        print(f"🛫 Range Voos: {VOO_ID_RANGE[0]:,} - {VOO_ID_RANGE[1]:,}")
        print(f"⚡ Taxa: {FLIGHTS_PER_MINUTE} voos/min, {HOTELS_PER_MINUTE} linhas hotel/min")
        print(f"⏱️ Intervalos: {self.flight_interval:.3f}s (voos), {self.hotel_interval:.3f}s (hotéis)")
        print("\n💡 Pressione Ctrl+C para parar\n")

        try:
            # Criar e iniciar threads
            flight_thread = threading.Thread(target=self.flight_generator_thread, daemon=True)
            hotel_thread = threading.Thread(target=self.hotel_generator_thread, daemon=True)
            stats_thread = threading.Thread(target=self.stats_thread, daemon=True)

            flight_thread.start()
            hotel_thread.start()
            stats_thread.start()

            print("✅ Threads iniciadas. Gerando dados...\n")

            # Manter programa rodando
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n🛑 Parando gerador de dados...")
            self.running = False

            # Aguardar threads terminarem
            flight_thread.join(timeout=2)
            hotel_thread.join(timeout=2)
            stats_thread.join(timeout=2)

            # Estatísticas finais
            elapsed = datetime.now() - self.stats['start_time']
            print(f"\n📊 === ESTATÍSTICAS FINAIS ===")
            print(f"⏱️ Tempo execução: {elapsed}")
            print(f"🛫 Reservas de voos: {self.stats['voos_gerados']}")
            print(f"🏨 Linhas de hotéis: {self.stats['hoteis_gerados']}")
            print(f"📈 Total gerado: {self.stats['voos_gerados'] + self.stats['hoteis_gerados']}")

            # Tamanho final dos arquivos
            try:
                flight_size = os.path.getsize(CSV_FLIGHTS_FILE) / 1024  # KB
                hotel_size = os.path.getsize(CSV_HOTELS_FILE) / 1024   # KB
                print(f"📁 Arquivos finais: {flight_size:.1f}KB (voos), {hotel_size:.1f}KB (hotéis)")
            except:
                pass

            print(f"\n✅ Dados salvos em:")
            print(f"   🛫 {CSV_FLIGHTS_FILE}")
            print(f"   🏨 {CSV_HOTELS_FILE}")
            print("\n✅ Gerador encerrado!")

def main():
    """Função principal"""
    try:
        generator = CSVDataGenerator()
        generator.run()
    except KeyboardInterrupt:
        print("\n👋 Tchau!")
    except Exception as e:
        print(f"❌ Erro fatal: {e}")

if __name__ == "__main__":
    main()
