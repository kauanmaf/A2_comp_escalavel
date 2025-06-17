import redis
import json
import random
import time
from datetime import datetime

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_LIST_KEY_FLIGHTS = 'raw_flights'
REDIS_LIST_KEY_HOTELS = 'raw_hotels'

SLEEPTIME = 0.5

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
random.seed(42)

# --- DADOS FIXOS SIMULANDO AS TABELAS DO BANCO ---
DADOS_VOOS_FIXOS = [
    {"id_voo": 1, "cidade_ida": "São Paulo", "cidade_volta": "Rio de Janeiro", "data": "2025-10-15T10:00:00"},
    {"id_voo": 2, "cidade_ida": "Rio de Janeiro", "cidade_volta": "Salvador", "data": "2025-10-16T12:30:00"},
    {"id_voo": 3, "cidade_ida": "Belo Horizonte", "cidade_volta": "Porto Alegre", "data": "2025-11-05T08:45:00"},
    {"id_voo": 4, "cidade_ida": "Nova York", "cidade_volta": "São Paulo", "data": "2025-11-20T22:00:00"},
    {"id_voo": 5, "cidade_ida": "Lisboa", "cidade_volta": "Recife", "data": "2025-12-01T15:10:00"},
]

DADOS_HOTEIS_FIXOS = [
    {"id_hotel": 1, "cidade": "Rio de Janeiro", "estrelas": 5},
    {"id_hotel": 2, "cidade": "Salvador", "estrelas": 4},
    {"id_hotel": 3, "cidade": "Porto Alegre", "estrelas": 4},
    {"id_hotel": 4, "cidade": "São Paulo", "estrelas": 5},
    {"id_hotel": 5, "cidade": "Recife", "estrelas": 3},
]

# Dados de exemplo que podem ser usados para o contexto do evento
companies = ["CiaViagemA", "CiaVoosB", "AgenciaTurC"]

def add_data_to_list(redis_list, company_id, data_payload):
    """
    Cria a mensagem do evento e a insere na lista especificada do Redis.
    O 'timestamp' pode ser usado como a 'data_reserva'.
    """
    message = {
        "company_id": company_id,
        "data": json.dumps(data_payload),
        "timestamp": datetime.now().isoformat()
    }
    r.rpush(redis_list, json.dumps(message))

def generate_flight_reservation_data():
    """
    Gera um payload de dados para uma reserva de voo, selecionando
    um voo aleatório da lista de dados fixos.
    """
    # Seleciona um voo da lista fixa
    voo_selecionado = random.choice(DADOS_VOOS_FIXOS)

    data_payload = {
        # Usa o ID do voo selecionado
        "id_voo": voo_selecionado['id_voo'],
        # Gera um preço aleatório para a reserva
        "valor": round(random.uniform(300, 2500), 2)
    }
    return data_payload

def generate_hotel_reservation_data():
    """
    Gera um payload de dados para uma reserva de hotel, selecionando
    um hotel aleatório da lista de dados fixos.
    """
    # Seleciona um hotel da lista fixa
    hotel_selecionado = random.choice(DADOS_HOTEIS_FIXOS)

    # Cria uma data e hora aleatória para a reserva do hotel
    reserved_date = datetime(
        2025,
        random.randint(1, 12),
        random.randint(1, 28),
        random.randint(10, 22), # Check-in time
        random.choice([0, 30]) # Minutes
    )

    data_payload = {
        # Usa o ID do hotel selecionado
        "id_hotel": hotel_selecionado['id_hotel'],
        # Gera um preço aleatório para a reserva
        "valor": round(random.uniform(150, 2000), 2),
        # Data para a qual o hotel foi reservado
        "data_reservada": reserved_date.isoformat()
    }
    return data_payload

if __name__ == "__main__":
    print("Iniciando produtor de eventos de RESERVAS (baseado em dados fixos) para listas Redis...")
    try:
        while True:
            # Gera e adiciona um evento de reserva de voo
            add_data_to_list(
                REDIS_LIST_KEY_FLIGHTS,
                random.choice(companies),
                generate_flight_reservation_data()
            )

            # Gera e adiciona um evento de reserva de hotel
            add_data_to_list(
                REDIS_LIST_KEY_HOTELS,
                random.choice(companies),
                generate_hotel_reservation_data()
            )
            time.sleep(SLEEPTIME) # Aumentado o sleep para melhor visualização

    except KeyboardInterrupt:
        print("\nCliente produtor encerrado.")
        print(f"Tamanhos finais - Reservas Voos: {r.llen(REDIS_LIST_KEY_FLIGHTS)}, Reservas Hotéis: {r.llen(REDIS_LIST_KEY_HOTELS)}")