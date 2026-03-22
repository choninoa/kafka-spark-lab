"""
Producer de Weather Data para Kafka
====================================
Obtiene datos del clima desde WeatherAPI y los envia al topic 'weather-data' en Kafka.

Requisitos (pip install):
    pip install kafka-python requests

Uso:
    python producer.py
"""

import json
import os
import random
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

# ============================================
# CONFIGURACION - Variables de entorno (.env)
# ============================================
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "TU_API_KEY_AQUI")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-data")
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "10"))

CITIES = [
    "Trujillo",
    "Lima",
    "Machala",
    "Santa Cruz de la Sierra",
    "Madrid",
    "Barcelona",
    "Buenos Aires",
    "Bogota",
    "Santiago",
    "La Paz",
    "New York",
    "London",
    "Tokyo",
    "Sydney",
    "La Havana",
    
]

# ============================================
# FUNCIONES
# ============================================
def get_weather(city):
    """Obtiene datos del clima desde WeatherAPI para una ciudad"""
    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        weather = {
            "city": data["location"]["name"],
            "region": data["location"]["region"],
            "country": data["location"]["country"],
            "temperature_c": data["current"]["temp_c"],
            "humidity": data["current"]["humidity"],
            "wind_kph": data["current"]["wind_kph"],
            "condition": data["current"]["condition"]["text"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
        return weather
    except Exception as e:
        print(f"Error obteniendo clima de {city}: {e}")
        return None


def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Productor conectado a Kafka en {KAFKA_BOOTSTRAP}")
    print(f"Ciudades: {', '.join(CITIES)}")
    print(f"Intervalo: cada {INTERVAL_SECONDS} segundos")
    print("-" * 60)

    while True:
        city = random.choice(CITIES)
        weather = get_weather(city)
        if weather:
            producer.send(KAFKA_TOPIC, value=weather)
            producer.flush()
            print(f"[{weather['timestamp']}] {weather['city']}, {weather['country']} | "
                  f"{weather['temperature_c']}°C | {weather['humidity']}% | "
                  f"{weather['wind_kph']} kph | {weather['condition']}")
        else:
            print(f"Sin datos para {city}, continuando...")

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
