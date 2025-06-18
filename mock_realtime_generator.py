#!/usr/bin/env python3
"""
Gerador de dados em tempo real para reservas de voos e hotéis
Usa threading para gerar dados independentemente em duas threads
Publica dados no Redis via Pub/Sub
"""

import threading
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any
import uuid
import json
import redis

# Configuração de quantidade por minuto
FLIGHTS_PER_MINUTE = 240  # Voos por minuto
HOTELS_PER_MINUTE = 240    # Hotéis por minuto (na verdade é metade do total de hotéis) =)

# Ranges baseados nos dados gerados pelo generate_fixed_database.py
# Como foram geradas 50 cidades com 50-250 hotéis cada, temos aproximadamente 7000 hotéis
HOTEL_ID_RANGE = (1, 7000)  # Range de IDs de hotéis
VOO_ID_RANGE = (1, 1340000)  # Range de IDs de voos (aproximadamente 1.34M voos)

# Configurações Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_CHANNEL_FLIGHTS = 'raw_flights'
REDIS_CHANNEL_HOTELS = 'raw_hotels'
REDIS_STATS_REQUEST_CHANNEL = 'stats_request'

class RedisDataGenerator:
    def __init__(self):
        """Inicializa o gerador de dados para Redis"""
        print("🚀 === GERADOR DE DADOS PARA REDIS ===")
        print(f"📡 Publicando em canais Redis: {REDIS_CHANNEL_FLIGHTS}, {REDIS_CHANNEL_HOTELS}")

        self.running = True
        self.stats = {
            'voos_gerados': 0,
            'hoteis_gerados': 0,
            'start_time': datetime.now()
        }

        # Conexão Redis
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        try:
            self.redis_client.ping()
            print(f"✅ Conectado ao Redis em {REDIS_HOST}:{REDIS_PORT}")
        except redis.exceptions.ConnectionError as e:
            print(f"❌ Não foi possível conectar ao Redis em {REDIS_HOST}:{REDIS_PORT}. Erro: {e}")
            print("Certifique-se de que o servidor Redis está em execução.")
            exit(1)

        # Calcular intervalos baseados na quantidade por minuto
        self.flight_interval = 60.0 / FLIGHTS_PER_MINUTE  # segundos entre voos
        self.hotel_interval = 60.0 / HOTELS_PER_MINUTE    # segundos entre hotéis

    def generate_flight_reservation(self) -> Dict[str, Any]:
        """Gera dados de uma reserva de voo"""
        uuid_hex = uuid.uuid4().hex
    
        # Convert the full hexadecimal string to a large integer
        id_reserva_voo = int(uuid_hex, 16) 
        id_voo = random.randint(*VOO_ID_RANGE)
        valor = round(random.uniform(300, 2500), 2)
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
        id_hotel = random.randint(*HOTEL_ID_RANGE)
        valor = round(random.uniform(150, 2000), 2)
        dias_atras = random.randint(0, 15)
        data_reserva = datetime.now() - timedelta(days=dias_atras)

        num_dias = random.randint(1, 3)
        dias_futuros = random.randint(0, 90)
        data_inicial = datetime.now() + timedelta(days=dias_futuros)

        registros = []
        for i in range(num_dias):
            uuid_hex = uuid.uuid4().hex
    
            # Convert the full hexadecimal string to a large integer
            id_reserva_hotel = int(uuid_hex, 16) 
            data_estadia = data_inicial + timedelta(days=i)
            
            registros.append({
                "id_hotel": id_hotel,
                "id_reserva_hotel": id_reserva_hotel,
                "valor": valor,
                "data_reservada": data_estadia.strftime('%Y-%m-%d'),
                "data_reserva": data_reserva.isoformat()
            })

            
        return registros

    def publish_data_to_redis(self, list_key: str, data_payload: Dict[str, Any]):
        """
        Adiciona os dados gerados a uma lista Redis.
        Adiciona um company_id para simular a origem da reserva.
        """
        company_id = random.choice(["CiaViagemA", "CiaVoosB", "AgenciaTurC"])
        
        message = {
            "company_id": company_id,
            "data": data_payload,
            "timestamp": datetime.now().isoformat()
        }
        self.redis_client.rpush(list_key, json.dumps(message)) 

    def flight_generator_thread(self):
        """Thread para gerar dados de reservas de voos e publicar no canal Redis"""
        print(f"🛫 Thread de voos iniciada. Publicando no canal '{REDIS_CHANNEL_FLIGHTS}'")
        while self.running:
            try:
                flight_data = self.generate_flight_reservation()
                self.publish_data_to_redis(REDIS_CHANNEL_FLIGHTS, flight_data)
                self.stats['voos_gerados'] += 1
                time.sleep(self.flight_interval)
            except Exception as e:
                print(f"❌ Erro na thread de voos: {e}")
                time.sleep(1)

    def hotel_generator_thread(self):
        """Thread para gerar dados de reservas de hotéis e publicar no canal Redis"""
        print(f"🏨 Thread de hotéis iniciada. Publicando no canal '{REDIS_CHANNEL_HOTELS}'")
        while self.running:
            try:
                hotel_registros = self.generate_hotel_reservation()
                for registro in hotel_registros:
                    self.publish_data_to_redis(REDIS_CHANNEL_HOTELS, registro)
                    self.stats['hoteis_gerados'] += 1
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
            print(f"🛫 Reservas de voos publicadas: {self.stats['voos_gerados']}")
            print(f"🏨 Linhas de hotéis publicadas: {self.stats['hoteis_gerados']}")
            print(f"📈 Total publicado: {self.stats['voos_gerados'] + self.stats['hoteis_gerados']}")
            print("=" * 70)

    def run(self):
        """Executa o gerador de dados para Redis"""
        print(f"🏨 Range Hotéis: {HOTEL_ID_RANGE[0]:,} - {HOTEL_ID_RANGE[1]:,}")
        print(f"🛫 Range Voos: {VOO_ID_RANGE[0]:,} - {VOO_ID_RANGE[1]:,}")
        print(f"⚡ Taxa: {FLIGHTS_PER_MINUTE} voos/min, {HOTELS_PER_MINUTE} linhas hotel/min")
        print(f"⏱️ Intervalos: {self.flight_interval:.3f}s (voos), {self.hotel_interval:.3f}s (hotéis)")
        print("\n💡 Pressione Ctrl+C para parar\n")

        try:
            flight_thread = threading.Thread(target=self.flight_generator_thread, daemon=True)
            hotel_thread = threading.Thread(target=self.hotel_generator_thread, daemon=True)
            stats_thread = threading.Thread(target=self.stats_thread, daemon=True)

            flight_thread.start()
            hotel_thread.start()
            stats_thread.start()

            print("✅ Threads iniciadas. Gerando e publicando dados no Redis...\n")

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n🛑 Parando gerador de dados...")
            self.running = False

            flight_thread.join(timeout=2)
            hotel_thread.join(timeout=2)
            stats_thread.join(timeout=2)

            elapsed = datetime.now() - self.stats['start_time']
            print(f"\n📊 === ESTATÍSTICAS FINAIS ===")
            print(f"⏱️ Tempo execução: {elapsed}")
            print(f"🛫 Reservas de voos publicadas: {self.stats['voos_gerados']}")
            print(f"🏨 Linhas de hotéis publicadas: {self.stats['hoteis_gerados']}")
            print(f"📈 Total publicado: {self.stats['voos_gerados'] + self.stats['hoteis_gerados']}")
            print("\n✅ Gerador encerrado!")

def main():
    """Função principal"""
    try:
        generator = RedisDataGenerator()
        generator.run()
    except KeyboardInterrupt:
        print("\n👋 Tchau!")
    except Exception as e:
        print(f"❌ Erro fatal: {e}")

if __name__ == "__main__":
    main()