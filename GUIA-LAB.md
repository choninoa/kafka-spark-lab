# Guia del Lab: Procesamiento de Datos en Tiempo Real con Docker

## Arquitectura

```
WeatherAPI.com --> producer --> [KAFKA topic: weather-data] --> spark-consumer
  (Internet)      (Docker)          (Docker)                    (Docker - Spark Streaming)
```

Todo corre dentro de Docker en la misma red (`lab-network`). No necesitas Java ni PySpark en tu maquina.

## Estructura de archivos

```
kafka-spark-lab/
├── docker-compose.yml           # Todos los servicios
├── Dockerfile.producer          # Imagen Alpine para el producer
├── Dockerfile.chart             # Imagen Alpine para el chart-consumer
├── requirements.producer.txt    # Deps del producer (sin PySpark)
├── requirements.chart.txt       # Deps del chart-consumer (Flask, Plotly, plotext)
├── .env.example                 # Plantilla de variables
└── scripts/
    ├── producer.py              # Envia datos del clima de 15 ciudades aleatorias a Kafka
    ├── spark_consumer.py        # Lee stream de Kafka con Spark
    └── chart_consumer.py        # Dashboard web Plotly.js + bar charts en terminal
```

> Los scripts estan montados como bind volume: editas el .py y el cambio esta disponible sin rebuild.

## Servicios en Docker

| Servicio | Imagen | Puerto | Descripcion |
|---|---|---|---|
| kafka | apache/kafka:3.7.0 | 9092/9093 | Broker Kafka (KRaft, sin Zookeeper) |
| kafka-init | apache/kafka:3.7.0 | - | Crea el topic weather-data y termina |
| producer | Dockerfile.producer (Alpine) | - | Envia datos del clima a Kafka |
| spark-consumer | apache/spark:3.5.3-python3 | - | Lee el stream con Spark Structured Streaming |
| spark-master | apache/spark:3.5.3-python3 | 8080, 7077, 4040 | Spark Master (Web UI) |
| spark-worker | apache/spark:3.5.3-python3 | - | Spark Worker (2 cores, 2GB) |
| kafka-ui | provectuslabs/kafka-ui | 8090 | UI para ver topics y mensajes |
| chart-consumer | Dockerfile.chart (Alpine) | 8050 | Dashboard Plotly.js + charts en terminal |

---

## Paso 1: Obtener API Key de WeatherAPI

1. Ve a https://www.weatherapi.com/ y crea una cuenta gratuita
2. Copia tu API Key del dashboard

## Paso 2: Configurar variables de entorno

```bash
cp .env.example .env
# Edita .env y pon tu WEATHER_API_KEY real
# La variable CITY ya no se usa — el producer rota entre 15 ciudades automaticamente
```

## Paso 3: Levantar todos los servicios

```bash
docker compose up -d --build
```

Espera ~30 segundos a que Kafka arranque. Verifica:

```bash
docker compose logs kafka-init
# Debe mostrar: Topic weather-data creado exitosamente!
```

## Paso 4: Ver los datos en tiempo real

```bash
# Producer rotando entre 15 ciudades del mundo
docker compose logs -f producer
# [2026-03-15 17:26] Tokyo, Japan | 10.4°C | 76% | 16.2 kph | Light rain
# [2026-03-15 17:27] Madrid, Spain | 15.1°C | 41% | 15.5 kph | Sunny

# Spark Consumer procesando el stream
# (la primera vez tarda ~1 min descargando el paquete Kafka)
docker compose logs -f spark-consumer

# Chart Consumer (web + terminal simultaneo)
docker logs -f chart-consumer        # Bar charts en terminal con plotext (se actualiza cada 3s)
# Abrir en browser: http://localhost:8050  (graficos Plotly, se refresca cada 1s)
```

## Paso 5: Verificar en Kafka UI

Abre http://localhost:8090 → Topics → weather-data → Messages

## Paso 6: Ver Spark UI

- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040 (disponible mientras corre el consumer)

---

## Editar scripts en caliente

```bash
# Edita el script
vim scripts/producer.py

# Aplica el cambio sin rebuild
docker compose restart producer
```

---

## Comandos utiles

```bash
docker compose ps                         # Estado de todos los contenedores
docker compose logs -f producer           # Logs del producer
docker compose logs -f spark-consumer    # Logs del consumer
docker compose down                       # Parar todo
docker compose down -v                    # Parar y borrar volumenes
docker compose down -v && docker compose up -d --build  # Recrear desde cero
```

### Kafka

```bash
# Ver mensajes en consola
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --topic weather-data \
    --from-beginning \
    --bootstrap-server localhost:9092
```
