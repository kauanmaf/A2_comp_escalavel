# test_client.py
import redis
import json
import time
import datetime

REDIS_HOST = 'localhost' # O Docker Desktop mapeia para localhost
REDIS_PORT = 6379
REDIS_LIST_KEY = 'raw_travel_data_list' # A chave da lista no Redis

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} for adding to List...")

def add_data_to_list(company_id, event_type, data_payload):
    message = {
        "company_id": company_id,
        "event_type": event_type,
        "data": json.dumps(data_payload), # Payload interno como JSON string
        "timestamp": datetime.datetime.now().isoformat()
    }
    # Adiciona no final da lista
    r.rpush(REDIS_LIST_KEY, json.dumps(message))
    print(f"Added to Redis List '{REDIS_LIST_KEY}': {message['event_type']} for {company_id}")

if __name__ == "__main__":
    print("Starting client data producer to Redis List...")
    try:
        while True:
            add_data_to_list("CompanyA", "flight_booking", {"flight_id": "FL123", "origin": "GRU", "dest": "JFK", "price": 800.50})
            time.sleep(0.5) # Mais rápido para acumular
            add_data_to_list("CompanyB", "hotel_reservation", {"hotel_id": "HTL001", "city": "NYC", "nights": 3, "cost": 350.00})
            time.sleep(0.5)
            add_data_to_list("CompanyA", "flight_booking", {"flight_id": "FL124", "origin": "SDU", "dest": "PTO", "price": 750.40})
            time.sleep(0.5)
            add_data_to_list("CompanyC", "flight_booking", {"flight_id": "FL456", "origin": "RIO", "dest": "LIS", "price": 1200.00})
            time.sleep(0.5)
            add_data_to_list("CompanyB", "hotel_search", {"hotel_id": "HTL002", "city": "RIO", "nights": 2, "cost": 1500.00})
            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"\nClient producer stopped. Current list size: {r.llen(REDIS_LIST_KEY)}")