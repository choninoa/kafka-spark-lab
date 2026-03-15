# Kafka + Spark Streaming Lab

Laboratorio práctico de procesamiento de datos en tiempo real con **Apache Kafka** y **Apache Spark Structured Streaming**, usando Docker.

Se obtienen datos meteorológicos reales desde [WeatherAPI](https://www.weatherapi.com/), se envían a un topic de Kafka y se procesan con Spark Streaming. **Todo corre dentro de Docker** — no se necesita Java, PySpark ni nada instalado en tu máquina más allá de Docker.

## Arquitectura

```
                    ┌─────────────────────────────────────────────────────┐
                    │                  lab-network (Docker)               │
                    │                                                     │
 WeatherAPI.com ──► │  producer ──► KAFKA ──► spark-consumer             │
   (Internet)       │  (Alpine)     topic:    (Spark Streaming)          │
                    │               weather-data                          │
                    │                    │                                │
                    │                    ▼                                │
                    │                 Kafka UI                            │
                    │              (localhost:8090)                       │
                    └─────────────────────────────────────────────────────┘
```

**Flujo de datos:**
1. `producer` consulta el clima desde WeatherAPI cada N segundos y envía JSON a Kafka
2. `spark-consumer` lee el stream de Kafka con Spark Structured Streaming y muestra los datos en consola
3. Todos los servicios comparten la misma red Docker (`lab-network`)

---

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) y Docker Compose
- Cuenta gratuita en [WeatherAPI](https://www.weatherapi.com/) (para obtener una API Key)

> No se necesita Python, Java ni PySpark instalados en tu máquina.

---

## Estructura del proyecto

```
kafka-spark-lab/
├── docker-compose.yml          # Define todos los servicios Docker
├── Dockerfile.producer         # Imagen Alpine para el producer (Python ligero)
├── requirements.producer.txt   # Dependencias del producer (sin PySpark)
├── requirements.txt            # Dependencias completas (para desarrollo local)
├── .env.example                # Plantilla de variables de entorno
├── GUIA-LAB.md                 # Guía paso a paso del laboratorio
├── README.md                   # Este archivo
└── scripts/
    ├── producer.py             # Productor: envía datos del clima a Kafka
    └── spark_consumer.py       # Consumer: Spark Structured Streaming
```

> Los scripts se montan como bind volume en todos los contenedores Spark y producer. Edita un `.py` y el cambio está disponible al instante sin rebuild.

---

## Servicios Docker

| Servicio | Imagen | Puerto | Descripción |
|---|---|---|---|
| **kafka** | `apache/kafka:3.7.0` | `9092` (host), `9093` (interno) | Broker Kafka en modo KRaft (sin Zookeeper) |
| **kafka-init** | `apache/kafka:3.7.0` | — | Crea el topic `weather-data` y termina |
| **producer** | `Dockerfile.producer` (Alpine) | — | Envía datos del clima a Kafka |
| **spark-consumer** | `apache/spark:3.5.3-python3` | — | Lee el stream de Kafka con Spark |
| **spark-master** | `apache/spark:3.5.3-python3` | `8080`, `7077`, `4040` | Nodo maestro Spark (Web UI) |
| **spark-worker** | `apache/spark:3.5.3-python3` | — | Worker Spark (2 cores, 2 GB RAM) |
| **kafka-ui** | `provectuslabs/kafka-ui` | `8090` | Interfaz web para inspeccionar Kafka |

---

## Guía de instalación y uso

### 1. Clonar el repositorio

```bash
git clone https://github.com/choninoa/kafka-spark-lab.git
cd kafka-spark-lab
```

### 2. Obtener API Key de WeatherAPI

1. Ve a [https://www.weatherapi.com/](https://www.weatherapi.com/) y crea una cuenta gratuita
2. En el dashboard, copia tu **API Key**

### 3. Configurar variables de entorno

```bash
cp .env.example .env
```

Edita `.env` con tu API Key y ciudad:

```env
WEATHER_API_KEY=abc123tuapikeyreal
CITY=Trujillo
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=weather-data
INTERVAL_SECONDS=10
```

| Variable | Descripción | Valor por defecto |
|---|---|---|
| `WEATHER_API_KEY` | API Key de WeatherAPI.com **(obligatoria)** | `TU_API_KEY_AQUI` |
| `CITY` | Ciudad de la que se obtiene el clima | `Trujillo` |
| `INTERVAL_SECONDS` | Segundos entre cada consulta | `10` |

### 4. Build y levantar todos los servicios

```bash
docker compose up -d --build
```

Esto construye la imagen del producer y levanta todos los servicios. Espera ~30 segundos a que Kafka arranque.

Verifica que el topic se creó:

```bash
docker compose logs kafka-init
```

Deberías ver: `>>> Topic weather-data creado exitosamente!`

### 5. Ver los datos en tiempo real

**Producer** (enviando datos a Kafka):

```bash
docker compose logs -f producer
```

**Spark Consumer** (procesando el stream — tarda ~1 min en descargar el paquete Kafka la primera vez):

```bash
docker compose logs -f spark-consumer
```

Verás los datos del clima llegando en formato tabla:

```
+----------+-----------+-------+-------------+--------+--------+-------------+----------------+
|city      |region     |country|temperature_c|humidity|wind_kph|condition    |timestamp       |
+----------+-----------+-------+-------------+--------+--------+-------------+----------------+
|Trujillo  |La Libertad|Peru   |22.0         |78      |15.1    |Partly cloudy|2026-03-15 10:30|
+----------+-----------+-------+-------------+--------+--------+-------------+----------------+
```

### 6. Interfaces web

| Interfaz | URL | Descripción |
|---|---|---|
| Kafka UI | [http://localhost:8090](http://localhost:8090) | Topics, mensajes, particiones |
| Spark Master UI | [http://localhost:8080](http://localhost:8080) | Estado del clúster Spark |
| Spark Application UI | [http://localhost:4040](http://localhost:4040) | Job en ejecución (disponible mientras corre el consumer) |

---

## Editar scripts en caliente

Los scripts están montados como bind volume en todos los contenedores. Para aplicar cambios:

```bash
# Edita el script en tu editor
vim scripts/producer.py

# Reinicia solo el servicio afectado (sin rebuild)
docker compose restart producer
```

Para `spark-consumer`, al reiniciar descarga el paquete Kafka desde el caché de Ivy (volumen `spark-ivy-cache`), por lo que es rápido a partir de la segunda vez.

---

## Comandos útiles

### Gestión de servicios

```bash
# Ver estado de todos los contenedores
docker compose ps

# Logs en tiempo real
docker compose logs -f producer
docker compose logs -f spark-consumer

# Parar todo
docker compose down

# Parar y borrar datos (volúmenes)
docker compose down -v

# Recrear desde cero
docker compose down -v && docker compose up -d --build
```

### Kafka

```bash
# Listar topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --list --bootstrap-server localhost:9092

# Ver mensajes del topic en consola
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --topic weather-data \
    --from-beginning \
    --bootstrap-server localhost:9092

# Describir el topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --describe --topic weather-data \
    --bootstrap-server localhost:9092
```

---

## Troubleshooting

### El producer no muestra logs o dice "Error obteniendo clima"
- Verifica que `WEATHER_API_KEY` en `.env` es válida
- Comprueba conectividad: `docker exec producer wget -q -O- http://api.weatherapi.com/v1/current.json?key=test&q=London`

### El producer no conecta a Kafka
- Espera 30 segundos después de `docker compose up -d`
- Verifica: `docker compose ps` — kafka debe estar `(healthy)`
- Reinicia el producer: `docker compose restart producer`

### Spark consumer no muestra datos
- La primera vez tarda ~1 min descargando el paquete Kafka
- Verifica que hay mensajes en Kafka UI ([http://localhost:8090](http://localhost:8090))
- Consulta los logs completos: `docker compose logs spark-consumer`

### Puerto ya en uso
```bash
docker compose down
lsof -i :9092  # o el puerto en conflicto
```

---

## Tecnologías

| Tecnología | Versión | Propósito |
|---|---|---|
| Apache Kafka | 3.7.0 | Broker de mensajería (modo KRaft) |
| Apache Spark | 3.5.3 | Procesamiento de datos en streaming |
| Python | 3.12 (Alpine) | Producer de datos |
| Docker | — | Contenedores para todos los servicios |
| WeatherAPI | — | Fuente de datos meteorológicos en tiempo real |

---

## Licencia

Proyecto educativo para uso en laboratorio.
