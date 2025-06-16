# test_client.py
import redis
import json
import time
import datetime
from faker import Faker
import random

# Configurar Faker para português
faker = Faker('pt_BR')

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_LIST_KEY_FLIGHTS = 'raw_flights'
REDIS_LIST_KEY_HOTELS = 'raw_hotels'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

print(f"Conectando ao Redis em {REDIS_HOST}:{REDIS_PORT} para adicionar às listas...")

def add_data_to_list(list_key, company_id, event_type, data_payload):
    message = {
        "company_id": company_id,
        "event_type": event_type,
        "data": json.dumps(data_payload),
        "timestamp": datetime.datetime.now().isoformat()
    }
    r.rpush(list_key, json.dumps(message))
    print(f"Adicionado à lista Redis '{list_key}': {message['event_type']} para {company_id}")

def generate_common_time_fields():
    random_datetime = faker.date_time_this_month()
    hora_str = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}"
    return {
        "ano": 2025,
        "mes": random_datetime.month,
        "dia": random_datetime.day,
        "hora": hora_str
    }

def generate_flight_data():
    base_data = {
        "flight_id": f"FL{random.randint(100, 999)}",
        "origin": faker.city(),
        "dest": faker.city(),
        "price": round(random.uniform(300.0, 2000.0), 2)
    }
    base_data.update(generate_common_time_fields())
    return base_data

def generate_hotel_data():
    base_data = {
        "hotel_id": f"HTL{random.randint(100, 999)}",
        "city": faker.city(),
        "stars": random.randint(1, 5),
        "price": round(random.uniform(150.0, 3000.0), 2)
    }
    base_data.update(generate_common_time_fields())
    return base_data

import redis
import json
import random
import time
from datetime import datetime
from faker import Faker

# Config Redis e Faker
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_LIST_KEY_FLIGHTS = 'raw_flights'
REDIS_LIST_KEY_HOTELS = 'raw_hotels'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
fake = Faker('pt_BR')
random.seed(42)

companies = ["CiaViagemA", "CiaVoosB", "AgenciaTurC"]
cidades = [ ... ]  # sua lista de cidades aqui

def add_data_to_list(redis_list, company_id, event_type, data_payload):
    message = {
        "company_id": company_id,
        "event_type": event_type,
        "data": json.dumps(data_payload),
        "timestamp": datetime.now().isoformat()
    }
    r.rpush(redis_list, json.dumps(message))

def generate_flight_data():
    origem = random.choice(cidades)
    destino = random.choice([c for c in cidades if c != origem])
    assentos_totais = random.choice([100, 200])
    assentos_ocupados = random.randint(0, assentos_totais)
    data_payload = {
        "cidade_origem": origem,
        "cidade_destino": destino,
        "assentos_ocupados": assentos_ocupados,
        "assentos_totais": assentos_totais,
        "assentos_disponiveis": assentos_totais - assentos_ocupados,
        "dia": random.randint(1, 28),
        "mes": random.randint(1, 12),
        "ano": 2025
    }
    return data_payload

def generate_hotel_data():
    nome_hotel = f"Hotel {fake.last_name()} {random.choice(['Palace', 'Resort', 'Inn', 'Suites', 'Plaza'])}"
    cidade = random.choice(cidades)
    tipo_quarto = random.choice(["Standard", "Deluxe", "Suíte", "Executivo", "Presidencial"])
    numero_quarto = random.randint(1, 50)
    capacidade = random.randint(1, 4)
    preco = round(random.uniform(100, 1000), 2)
    ocupado = random.choice([0, 1])
    data_payload = {
        "tipo_quarto": tipo_quarto,
        "nome_hotel": nome_hotel,
        "cidade_destino": cidade,
        "numero_quarto": numero_quarto,
        "quantidade_pessoas": capacidade,
        "preco": preco,
        "ocupado": ocupado,
        "dia": random.randint(1, 28),
        "mes": random.randint(1, 12),
        "ano": 2025
    }
    return data_payload

if __name__ == "__main__":
    print("Iniciando cliente produtor de eventos reais (voos e hotéis) para listas Redis...")
    try:
        while True:
            add_data_to_list(REDIS_LIST_KEY_FLIGHTS, random.choice(companies), "compra_voo", generate_flight_data())
            time.sleep(0.1)
            add_data_to_list(REDIS_LIST_KEY_HOTELS, random.choice(companies), "compra_hotel", generate_hotel_data())
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nCliente produtor encerrado.")
        print(f"Tamanhos finais - Voos: {r.llen(REDIS_LIST_KEY_FLIGHTS)}, Hotéis: {r.llen(REDIS_LIST_KEY_HOTELS)}")

