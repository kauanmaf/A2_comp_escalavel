import redis
import json
import random
import time
from datetime import datetime
import uuid

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_LIST_KEY_FLIGHTS = 'raw_flights'
REDIS_LIST_KEY_HOTELS = 'raw_hotels'
REDIS_STATS_REQUEST_CHANNEL = 'stats_request'

SLEEPTIME = 0.5

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
random.seed(42)

# --- DADOS FIXOS SIMULANDO AS TABELAS DO BANCO ---
DADOS_VOOS_FIXOS = [
    {"id_voo": 1, "cidade_origem": "São Paulo", "cidade_destino": "Rio de Janeiro", "data": "2025-10-15T10:00:00"},
    {"id_voo": 2, "cidade_origem": "Rio de Janeiro", "cidade_destino": "Salvador", "data": "2025-10-16T12:30:00"},
    {"id_voo": 3, "cidade_origem": "Belo Horizonte", "cidade_destino": "Porto Alegre", "data": "2025-11-05T08:45:00"},
    {"id_voo": 4, "cidade_origem": "Nova York", "cidade_destino": "São Paulo", "data": "2025-11-20T22:00:00"},
    {"id_voo": 5, "cidade_origem": "Lisboa", "cidade_destino": "Recife", "data": "2025-12-01T15:10:00"},
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

def request_statistics(company_id):
    """
    Publica uma mensagem no canal de solicitação de estatísticas.
    """
    request_message = {
        "company_id": company_id,
        "timestamp": datetime.now().isoformat(),
        "request_id": str(uuid.uuid4()),
    }
    r.publish(REDIS_STATS_REQUEST_CHANNEL, json.dumps(request_message))
    print(f"[{company_id}] - Solicitação de estatísticas enviada para o canal '{REDIS_STATS_REQUEST_CHANNEL}'.")

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
        "id_reserva_voo": round(random.uniform(300, 2500), 2),
        # Gera um preço aleatório para a reserva
        "valor": round(random.uniform(300, 2500), 2),
        "data_reserva": datetime(
            2025,
            random.randint(1, 12),
            random.randint(1, 28),
            random.randint(10, 22), # Check-in time
            random.choice([0, 30]) # Minutes
        ).isoformat()
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
        "id_reserva_hotel": round(random.uniform(150, 2000)),
        # Gera um preço aleatório para a reserva
        "valor": round(random.uniform(150, 2000), 2),
        # Data para a qual o hotel foi reservado
        "data_reservada": reserved_date.isoformat(),
        "data_reserva": reserved_date.isoformat()
    }
    return data_payload

if __name__ == "__main__":
    print("Iniciando produtor de eventos de RESERVAS (baseado em dados fixos) para listas Redis...")
    try:
        counter = 0
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

            counter += 1
            if counter % 50 == 0:
                request_statistics(random.choice(companies))

            time.sleep(SLEEPTIME) # Aumentado o sleep para melhor visualização

    except KeyboardInterrupt:
        print("\nCliente produtor encerrado.")
        print(f"Tamanhos finais - Reservas Voos: {r.llen(REDIS_LIST_KEY_FLIGHTS)}, Reservas Hotéis: {r.llen(REDIS_LIST_KEY_HOTELS)}")