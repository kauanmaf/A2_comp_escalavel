# test_client.py
import redis
import json
import time
import datetime
import itertools # Para ciclar através das chaves Redis

REDIS_HOST = 'localhost' # O Docker Desktop mapeia para localhost
REDIS_PORT = 6379

# Definir as 4 chaves das listas Redis para onde os dados serão enviados
REDIS_LIST_KEYS = [
    'raw_travel_data_list_1',
    'raw_travel_data_list_2',
    'raw_travel_data_list_3',
    'raw_travel_data_list_4'
]

# Inicializa o cliente Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} for adding to Lists...")

def add_data_to_list(redis_list_key, company_id, event_type, data_payload):
    """
    Adiciona uma mensagem formatada JSON a uma lista Redis específica.
    """
    message = {
        "company_id": company_id,
        "event_type": event_type,
        "data": json.dumps(data_payload), # Payload interno como JSON string
        "timestamp": datetime.datetime.now().isoformat()
    }
    # Adiciona no final da lista especificada
    r.rpush(redis_list_key, json.dumps(message))
    print(f"Added to Redis List '{redis_list_key}': {message['event_type']} for {company_id}")

if __name__ == "__main__":
    print("Starting client data producer to Redis Lists...")
    # Cria um iterador cíclico para as chaves das listas Redis
    key_cycle = itertools.cycle(REDIS_LIST_KEYS)

    try:
        while True:
            # Pega a próxima chave na sequência cíclica
            current_key = next(key_cycle)
            add_data_to_list(current_key, "CompanyA", "flight_booking", {"flight_id": "FL123", "origin": "GRU", "dest": "JFK", "price": 800.50})
            time.sleep(0.5)

            current_key = next(key_cycle)
            add_data_to_list(current_key, "CompanyB", "hotel_reservation", {"hotel_id": "HTL001", "city": "NYC", "nights": 3, "cost": 350.00})
            time.sleep(0.5)

            current_key = next(key_cycle)
            add_data_to_list(current_key, "CompanyA", "flight_booking", {"flight_id": "FL124", "origin": "SDU", "dest": "PTO", "price": 750.40})
            time.sleep(0.5)

            current_key = next(key_cycle)
            add_data_to_list(current_key, "CompanyC", "flight_booking", {"flight_id": "FL456", "origin": "RIO", "dest": "LIS", "price": 1200.00})
            time.sleep(0.5)

            current_key = next(key_cycle)
            add_data_to_list(current_key, "CompanyB", "hotel_search", {"hotel_id": "HTL002", "city": "RIO", "nights": 2, "cost": 1500.00})
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nClient producer stopped. Current list sizes:")
        for key in REDIS_LIST_KEYS:
            print(f"  '{key}': {r.llen(key)} messages")