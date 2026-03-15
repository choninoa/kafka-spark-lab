# Guia del Lab: Procesamiento y Manejo de Datos con Docker

## Arquitectura

```
WeatherAPI.com --> producer.py --> [KAFKA topic: weather-data] --> spark_consumer.py (Spark Streaming)
                  (tu maquina)     (Docker)                       (Docker o tu maquina)
```

## Estructura de archivos

```
kafka-spark-lab/
├── docker-compose.yml
├── GUIA-LAB.md
└── scripts/
    ├── producer.py                # Lo corres manual en tu PC
    ├── spark_consumer.py          # Para correr DENTRO del contenedor Spark
    └── spark_consumer_local.py    # Para correr desde tu PC con pyspark local
```

## Servicios en Docker

| Servicio | Imagen | Puerto | Descripcion |
|----------|--------|--------|-------------|
| kafka | apache/kafka:3.7.0 | 9092 | Broker Kafka (KRaft, sin Zookeeper) |
| kafka-init | apache/kafka:3.7.0 | - | Crea el topic weather-data y termina |
| spark-master | apache/spark:3.5.3-python3 | 8080, 7077, 4040 | Spark Master |
| spark-worker | apache/spark:3.5.3-python3 | - | Spark Worker |
| kafka-ui | provectuslabs/kafka-ui | 8090 | UI para ver topics y mensajes (opcional) |

---

## Paso 1: Obtener API Key de WeatherAPI

1. Ve a https://www.weatherapi.com/ y crea una cuenta gratuita
2. Copia tu API Key del dashboard
3. Editala en scripts/producer.py en la variable WEATHER_API_KEY

## Paso 2: Levantar los servicios

```bash
cd kafka-spark-lab
docker compose up -d
```

Espera unos 30 segundos a que Kafka arranque. Verifica con:

```bash
docker compose logs kafka-init
```

Deberias ver: Topic weather-data creado exitosamente!

## Paso 3: Verificar que Kafka esta corriendo

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Paso 4: Instalar dependencias Python en tu maquina

```bash
pip install kafka-python requests pyspark
```

## Paso 5: Ejecutar el Producer (en tu PC)

```bash
cd kafka-spark-lab/scripts
python producer.py
```

Dejalo corriendo mientras ejecutas el consumer.

## Paso 6: Ejecutar el Spark Consumer

### Opcion A: Desde el contenedor Spark (recomendado)

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark-scripts/spark_consumer.py
```

### Opcion B: Desde tu PC (requiere pyspark + Java)

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_consumer_local.py
```

## Paso 7: Verificar mensajes (opcional)

### Con Kafka UI
Abre http://localhost:8090 -> Topics -> weather-data -> Messages

### Con consumer de consola de Kafka
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --topic weather-data \
    --from-beginning \
    --bootstrap-server localhost:9092
```

## Paso 8: Ver Spark UI

- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040

---

## Comandos utiles

```bash
docker compose logs -f kafka          # Logs de Kafka
docker compose logs -f spark-master   # Logs de Spark
docker compose down                   # Parar todo
docker compose down -v                # Parar y borrar volumenes
docker compose down -v && docker compose up -d  # Recrear desde cero
```
