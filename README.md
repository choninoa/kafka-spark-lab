# Kafka + Spark Streaming Lab

Laboratorio práctico de procesamiento de datos en tiempo real con **Apache Kafka** y **Apache Spark Structured Streaming**, usando Docker.

Se obtienen datos meteorológicos reales desde [WeatherAPI](https://www.weatherapi.com/), se envían a un topic de Kafka y se procesan con Spark Streaming.

## Arquitectura

```
                    ┌──────────────────────────────────────────────────┐
                    │                   DOCKER                         │
                    │                                                  │
 WeatherAPI.com ──► │  producer.py ──► KAFKA ──► spark_consumer.py    │
   (Internet)       │  (tu PC)        topic:     (Spark Streaming)    │
                    │                weather-data                      │
                    │                     │                            │
                    │                     ▼                            │
                    │                  Kafka UI                        │
                    │               (localhost:8090)                   │
                    └──────────────────────────────────────────────────┘
```

**Flujo de datos:**
1. `producer.py` consulta el clima actual desde WeatherAPI cada N segundos
2. Los datos se envían como JSON al topic `weather-data` en Kafka
3. `spark_consumer.py` lee el stream de Kafka y muestra los datos en consola

---

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) y Docker Compose
- [Python 3.8+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/)
- Cuenta gratuita en [WeatherAPI](https://www.weatherapi.com/) (para obtener una API Key)

> **Nota:** Si deseas usar el consumer local (`spark_consumer_local.py`), necesitas además [Java 11+](https://adoptium.net/) instalado en tu máquina.

---

## Estructura del proyecto

```
kafka-spark-lab/
├── docker-compose.yml          # Define todos los servicios Docker
├── requirements.txt            # Dependencias Python
├── .env.example                # Plantilla de variables de entorno
├── GUIA-LAB.md                 # Guía paso a paso del laboratorio
├── README.md                   # Este archivo
└── scripts/
    ├── producer.py             # Productor: envía datos del clima a Kafka
    ├── spark_consumer.py       # Consumer: se ejecuta DENTRO del contenedor Spark
    └── spark_consumer_local.py # Consumer: se ejecuta en tu PC (requiere Java + PySpark)
```

---

## Servicios Docker

El archivo `docker-compose.yml` levanta los siguientes servicios:

| Servicio | Imagen | Puerto | Descripción |
|---|---|---|---|
| **kafka** | `apache/kafka:3.7.0` | `9092` (host), `9093` (interno) | Broker de mensajería Kafka en modo KRaft (sin Zookeeper) |
| **kafka-init** | `apache/kafka:3.7.0` | — | Contenedor de inicialización: crea el topic `weather-data` y termina |
| **spark-master** | `apache/spark:3.5.3-python3` | `8080` (Web UI), `7077` (Master), `4040` (App UI) | Nodo maestro de Spark |
| **spark-worker** | `apache/spark:3.5.3-python3` | — | Worker de Spark (2 cores, 2 GB RAM) |
| **kafka-ui** | `provectuslabs/kafka-ui` | `8090` | Interfaz web para inspeccionar topics y mensajes |

### Detalle de cada servicio

#### Kafka (broker)

Apache Kafka 3.7.0 en **modo KRaft** (sin Zookeeper). Actúa como broker y controller en un solo nodo.

- **Puerto 9092:** Para conexiones desde tu máquina (el producer se conecta aquí)
- **Puerto 9093:** Para conexiones internas entre contenedores Docker (el Spark consumer usa este)
- Los datos se persisten en el volumen `kafka-data`
- Retención de mensajes: 7 días (`168 horas`)

#### kafka-init (inicialización)

Contenedor efímero que se ejecuta una vez al arrancar. Espera a que Kafka esté healthy y luego crea el topic `weather-data` con 1 partición y factor de replicación 1. Después termina automáticamente.

#### Spark Master

Nodo maestro del clúster Spark. Coordina la ejecución de los jobs enviados. Puedes ver su estado en la Web UI (`http://localhost:8080`).

#### Spark Worker

Nodo de trabajo que ejecuta las tareas asignadas por el Master. Configurado con 2 cores y 2 GB de memoria.

#### Kafka UI (opcional)

Interfaz web para visualizar topics, mensajes, particiones y consumers de Kafka. Útil para verificar que los datos están llegando correctamente.

---

## Guía de instalación y uso

### 1. Clonar el repositorio

```bash
git clone <URL_DEL_REPOSITORIO>
cd kafka-spark-lab
```

### 2. Obtener API Key de WeatherAPI

1. Ve a [https://www.weatherapi.com/](https://www.weatherapi.com/) y crea una cuenta gratuita
2. En el dashboard, copia tu **API Key**

### 3. Configurar variables de entorno

Crea el archivo `.env` a partir de la plantilla:

```bash
cp .env.example .env
```

Edita `.env` y reemplaza `TU_API_KEY_AQUI` con tu API Key real:

```env
WEATHER_API_KEY=abc123tuapikeyreal
CITY=Trujillo
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=weather-data
INTERVAL_SECONDS=10
```

#### Variables disponibles

| Variable | Descripción | Valor por defecto |
|---|---|---|
| `WEATHER_API_KEY` | API Key de WeatherAPI.com **(obligatoria)** | `TU_API_KEY_AQUI` |
| `CITY` | Ciudad de la que se obtiene el clima | `Trujillo` |
| `KAFKA_BOOTSTRAP` | Dirección del broker Kafka | `localhost:9092` |
| `KAFKA_TOPIC` | Nombre del topic de Kafka | `weather-data` |
| `INTERVAL_SECONDS` | Intervalo en segundos entre cada consulta | `10` |

> Puedes cambiar `CITY` por cualquier ciudad válida en WeatherAPI (ej: `Lima`, `Madrid`, `New York`).

### 4. Levantar los servicios Docker

```bash
docker compose up -d
```

Espera **~30 segundos** a que Kafka arranque completamente. Verifica que el topic se creó correctamente:

```bash
docker compose logs kafka-init
```

Deberías ver: `>>> Topic weather-data creado exitosamente!`

Verifica que Kafka está funcionando:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Debe mostrar `weather-data` en la lista.

### 5. Crear entorno virtual de Python e instalar dependencias

```bash
# Crear entorno virtual
python3 -m venv venv

# Activar el entorno virtual
# En Linux/macOS:
source venv/bin/activate

# En Windows (PowerShell):
# .\venv\Scripts\Activate.ps1

# En Windows (CMD):
# venv\Scripts\activate.bat

# Instalar dependencias
pip install -r requirements.txt
```

### 6. Ejecutar el Producer

En una terminal (con el entorno virtual activado):

```bash
python scripts/producer.py
```

Verás algo como:

```
Productor conectado a Kafka en localhost:9092
Enviando datos del clima de 'Trujillo' al topic 'weather-data'
Intervalo: cada 10 segundos
------------------------------------------------------------
Enviado a Kafka: {'city': 'Trujillo', 'region': 'La Libertad', 'country': 'Peru', 'temperature_c': 22.0, 'humidity': 78, 'wind_kph': 15.1, 'condition': 'Partly cloudy', 'timestamp': '2026-03-15 10:30'}
```

**Deja esta terminal corriendo** mientras ejecutas el consumer.

### 7. Ejecutar el Spark Consumer

#### Opción A: Dentro del contenedor Spark (recomendado)

No requiere Java en tu máquina. Abre otra terminal y ejecuta:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark-scripts/spark_consumer.py
```

> La primera ejecución descarga las dependencias de Kafka para Spark (~30 segundos). Las siguientes veces será más rápido gracias al caché.

#### Opción B: Desde tu máquina (requiere Java 11+ y PySpark)

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    scripts/spark_consumer_local.py
```

En ambos casos verás los datos del clima llegando en formato tabla:

```
+----------+-----------+-------+--------------+--------+--------+---------------+----------------+
|city      |region     |country|temperature_c |humidity|wind_kph|condition      |timestamp       |
+----------+-----------+-------+--------------+--------+--------+---------------+----------------+
|Trujillo  |La Libertad|Peru   |22.0          |78      |15.1    |Partly cloudy  |2026-03-15 10:30|
+----------+-----------+-------+--------------+--------+--------+---------------+----------------+
```

### 8. Verificar mensajes (opcional)

**Kafka UI:** Abre [http://localhost:8090](http://localhost:8090) → Topics → `weather-data` → Messages

**Consumer de consola de Kafka:**

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --topic weather-data \
    --from-beginning \
    --bootstrap-server localhost:9092
```

### 9. Interfaces web

| Interfaz | URL | Descripción |
|---|---|---|
| Kafka UI | [http://localhost:8090](http://localhost:8090) | Ver topics, mensajes y particiones |
| Spark Master UI | [http://localhost:8080](http://localhost:8080) | Estado del clúster Spark |
| Spark Application UI | [http://localhost:4040](http://localhost:4040) | Detalles del job en ejecución |

---

## Scripts Python en detalle

### `scripts/producer.py` — Productor de datos

**Qué hace:** Consulta el clima actual de una ciudad usando la API de WeatherAPI.com y envía los datos como mensajes JSON al topic `weather-data` de Kafka, en un bucle continuo.

**Librerías que usa:**
- `kafka-python-ng` — Cliente de Kafka para Python
- `requests` — Para hacer peticiones HTTP a la API del clima
- `python-dotenv` — Para cargar las variables del archivo `.env`

**Flujo de ejecución:**
1. Carga las variables de entorno desde `.env`
2. Crea una conexión al broker de Kafka (`KafkaProducer`)
3. En un bucle infinito:
   - Llama a la API de WeatherAPI con la ciudad configurada
   - Construye un diccionario con los datos relevantes del clima
   - Serializa el diccionario a JSON y lo envía al topic de Kafka
   - Espera N segundos (configurable) antes de repetir

**Estructura del mensaje JSON que produce:**

```json
{
    "city": "Trujillo",
    "region": "La Libertad",
    "country": "Peru",
    "temperature_c": 22.0,
    "humidity": 78,
    "wind_kph": 15.1,
    "condition": "Partly cloudy",
    "timestamp": "2026-03-15 10:30"
}
```

**Dónde se ejecuta:** En tu máquina (fuera de Docker). Se conecta a Kafka por `localhost:9092`.

---

### `scripts/spark_consumer.py` — Consumer para ejecutar en Docker

**Qué hace:** Lee el stream de mensajes del topic `weather-data` de Kafka usando Spark Structured Streaming y muestra cada mensaje parseado en formato tabla en la consola.

**Librerías que usa:**
- `pyspark` — API de Python para Apache Spark

**Flujo de ejecución:**
1. Crea una `SparkSession` (se conecta al Spark Master del clúster Docker)
2. Define el schema del JSON esperado (los mismos campos que produce `producer.py`)
3. Abre un stream de lectura desde Kafka (`readStream`) suscribiéndose al topic `weather-data`
4. Parsea el valor de cada mensaje Kafka (que viene como bytes) a las columnas definidas en el schema
5. Escribe los resultados en la consola en modo `append` (solo muestra datos nuevos)

**Configuración de red:** Usa `kafka:9093` como bootstrap server (red interna de Docker).

**Cómo ejecutarlo:**

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark-scripts/spark_consumer.py
```

> El flag `--packages` descarga automáticamente el conector Kafka para Spark.

---

### `scripts/spark_consumer_local.py` — Consumer para ejecutar en tu PC

**Qué hace:** Exactamente lo mismo que `spark_consumer.py`, pero configurado para ejecutarse directamente en tu máquina en vez de dentro de Docker.

**Diferencias con `spark_consumer.py`:**

| Aspecto | `spark_consumer.py` (Docker) | `spark_consumer_local.py` (Local) |
|---|---|---|
| Kafka bootstrap server | `kafka:9093` (red Docker) | `localhost:9092` (red host) |
| Spark master | Clúster Docker (master/worker) | `local[*]` (todos los cores de tu PC) |
| Requiere Java local | No | Sí (Java 11+) |
| Requiere PySpark local | No | Sí |

**Cómo ejecutarlo:**

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    scripts/spark_consumer_local.py
```

> **Requisito:** Tener Java 11+ y PySpark instalados en tu máquina. Si no tienes Java, usa la Opción A (dentro de Docker).

---

## Comandos útiles

### Docker

```bash
# Ver logs de Kafka en tiempo real
docker compose logs -f kafka

# Ver logs de Spark Master
docker compose logs -f spark-master

# Ver estado de todos los contenedores
docker compose ps

# Parar todos los servicios
docker compose down

# Parar y borrar todos los datos (volúmenes)
docker compose down -v

# Recrear todo desde cero
docker compose down -v && docker compose up -d
```

### Kafka

```bash
# Listar topics existentes
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --list --bootstrap-server localhost:9092

# Ver mensajes del topic desde el inicio
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --topic weather-data \
    --from-beginning \
    --bootstrap-server localhost:9092

# Describir un topic (particiones, replicas, etc.)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --describe --topic weather-data \
    --bootstrap-server localhost:9092
```

### Python

```bash
# Crear entorno virtual
python3 -m venv venv

# Activar entorno virtual (Linux/macOS)
source venv/bin/activate

# Activar entorno virtual (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt

# Desactivar entorno virtual
deactivate
```

---

## Troubleshooting

### El producer dice "Error obteniendo clima"
- Verifica que tu `WEATHER_API_KEY` en `.env` es válida
- Comprueba que tienes conexión a internet
- Asegúrate de que el nombre de la ciudad en `CITY` es válido

### El producer no puede conectar a Kafka
- Verifica que los contenedores están corriendo: `docker compose ps`
- Espera 30 segundos después de `docker compose up -d` antes de ejecutar el producer
- Comprueba que el puerto 9092 no está siendo usado por otro proceso

### Spark consumer no muestra datos
- Asegúrate de que el producer está corriendo y enviando datos
- Verifica en Kafka UI ([http://localhost:8090](http://localhost:8090)) que los mensajes están llegando al topic
- La primera ejecución del consumer puede tardar ~30 segundos en descargar dependencias

### "Port already in use"
- Otro servicio está usando el puerto. Para detener servicios previos:
  ```bash
  docker compose down
  ```
- Revisa qué usa el puerto: `lsof -i :9092` (o el puerto en cuestión)

### Spark Application UI (localhost:4040) no carga
- Solo está disponible mientras hay un job de Spark ejecutándose
- Si usas la Opción A (Docker), asegúrate de que el contenedor spark-master tiene el puerto 4040 mapeado

---

## Tecnologías utilizadas

| Tecnología | Versión | Propósito |
|---|---|---|
| Apache Kafka | 3.7.0 | Broker de mensajería (modo KRaft) |
| Apache Spark | 3.5.3 | Procesamiento de datos en streaming |
| Python | 3.8+ | Scripts de productor y consumidor |
| Docker | — | Contenedores para los servicios |
| WeatherAPI | — | Fuente de datos meteorológicos en tiempo real |

---

## Licencia

Proyecto educativo para uso en laboratorio.
