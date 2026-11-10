"""Exercice 3 : producteur meteo Open-Meteo -> weather_stream.
Lancement :  docker exec -d pyspark_notebook python work/exo3.py
Arret :      docker exec pyspark_notebook pkill -f exo3.py
"""
import json
import time

import requests
from kafka import KafkaProducer

TOPIC = "weather_stream"
BROKER = "kafka:9092"
INTERVALLE_S = 30

VILLES = [
    ("Paris", "France", 48.8566, 2.3522),
    ("Seville", "Spain", 37.3891, -5.9845),
    ("Dubai", "United Arab Emirates", 25.2048, 55.2708),
    ("Reykjavik", "Iceland", 64.1466, -21.9426),
    ("Wellington", "New Zealand", -41.2865, 174.7762),
]

URL = ("https://api.open-meteo.com/v1/forecast"
       "?latitude={lat}&longitude={lon}"
       "&current=temperature_2m,wind_speed_10m,weather_code&wind_speed_unit=ms")

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    for city, country, lat, lon in VILLES:
        try:
            data = requests.get(URL.format(lat=lat, lon=lon), timeout=10).json()
        except requests.RequestException as e:
            print(f"echec API {city}: {e}", flush=True)
            continue
        data["city"] = city
        data["country"] = country
        producer.send(TOPIC, data)
    producer.flush()
    print(f"{len(VILLES)} messages envoyes", flush=True)
    time.sleep(INTERVALLE_S)
