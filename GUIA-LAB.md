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
├── requirements.producer.txt    # Deps del producer (sin PySpark)
├── .env.example                 # Plantilla de variables
└── scripts/
    ├── producer.py              # Envia datos del clima a Kafka
    └── spark_consumer.py        # Lee stream de Kafka con Spark
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

---

## Paso 1: Obtener API Key de WeatherAPI

1. Ve a https://www.weatherapi.com/ y crea una cuenta gratuita
2. Copia tu API Key del dashboard

## Paso 2: Configurar variables de entorno

```bash
cp .env.example .env
# Edita .env y pon tu WEATHER_API_KEY real
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
# Producer enviando datos
docker compose logs -f producer

# Spark Consumer procesando el stream
# (la primera vez tarda ~1 min descargando el paquete Kafka)
docker compose logs -f spark-consumer
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
