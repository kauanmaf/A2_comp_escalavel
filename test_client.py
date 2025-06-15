# test_client.py
import redis
import json
import time
import datetime

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_LIST_KEY_FLIGHTS = 'raw_flights'
REDIS_LIST_KEY_HOTELS = 'raw_hotels'
REDIS_LIST_KEY_RESERVATIONS = 'raw_travel_data_list'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} for adding to Lists...")

def add_data_to_list(list_key, company_id, event_type, data_payload):
    message = {
        "company_id": company_id,
        "event_type": event_type,
        "data": json.dumps(data_payload),
        "timestamp": datetime.datetime.now().isoformat()
    }
    r.rpush(list_key, json.dumps(message))
    print(f"Added to Redis List '{list_key}': {message['event_type']} for {company_id}")

if __name__ == "__main__":
    print("Starting client data producer to Redis Lists...")
    try:
        while True:
            add_data_to_list(REDIS_LIST_KEY_FLIGHTS, "CompanyA", "flight_search", {"origin": "LAX", "dest": "SFO"})
            time.sleep(0.1)
            add_data_to_list(REDIS_LIST_KEY_RESERVATIONS, "CompanyB", "hotel_reservation", {"hotel_id": "HTL001", "city": "NYC", "nights": 3, "cost": 350.00})
            time.sleep(0.1)
            add_data_to_list(REDIS_LIST_KEY_FLIGHTS, "CompanyC", "flight_booking", {"flight_id": "FL456", "origin": "RIO", "dest": "LIS", "price": 1200.00})
            time.sleep(0.1)
            add_data_to_list(REDIS_LIST_KEY_HOTELS, "CompanyA", "hotel_search", {"city": "MIA", "stars": 4})
            time.sleep(0.1)
            add_data_to_list(REDIS_LIST_KEY_RESERVATIONS, "CompanyA", "flight_booking", {"flight_id": "FL123", "origin": "GRU", "dest": "JFK", "price": 800.50})
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nClient producer stopped.")
        print(f"Final sizes - Flights: {r.llen(REDIS_LIST_KEY_FLIGHTS)}, Hotels: {r.llen(REDIS_LIST_KEY_HOTELS)}, Reservations: {r.llen(REDIS_LIST_KEY_RESERVATIONS)}")