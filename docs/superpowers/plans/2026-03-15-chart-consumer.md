# Chart Consumer Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Añadir un servicio `chart-consumer` que consume el topic `weather-data` de Kafka y muestra los datos en dos salidas simultáneas: gráficos de barras en terminal (plotext) y dashboard web interactivo (Flask + Plotly.js).

**Architecture:** Un solo script Python con tres hilos: (1) Kafka consumer que actualiza un dict compartido `latest_by_city`, (2) printer thread que imprime un bar chart con plotext a stdout cada 10 segundos, (3) Flask en el hilo principal sirviendo `/` (HTML+Plotly.js) y `/data` (JSON). El browser auto-refresca cada 5s haciendo fetch a `/data`.

**Tech Stack:** Python 3.12 Alpine, kafka-python-ng, flask, plotly, plotext, threading

---

## Chunk 1: Archivos de dependencias e imagen Docker

### Task 1: requirements.chart.txt

**Files:**
- Create: `requirements.chart.txt`

- [ ] **Step 1: Crear requirements.chart.txt**

```text
kafka-python-ng>=2.2.2
flask>=3.0.0
plotly>=5.24.0
plotext>=5.2.8
python-dotenv>=1.0.1
```

- [ ] **Step 2: Commit**

```bash
git add requirements.chart.txt
git commit -m "chore: agregar dependencias de chart-consumer"
```

---

### Task 2: Dockerfile.chart

**Files:**
- Create: `Dockerfile.chart`

- [ ] **Step 1: Crear Dockerfile.chart (multi-stage Alpine)**

```dockerfile
# Stage 1: instalar dependencias en venv aislado
FROM python:3.12-alpine AS deps
WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.chart.txt .
RUN pip install --no-cache-dir -r requirements.chart.txt

# Stage 2: imagen final minima
FROM python:3.12-alpine
ENV PATH="/opt/venv/bin:$PATH"
COPY --from=deps /opt/venv /opt/venv

# Scripts vienen del bind mount
WORKDIR /opt/spark-scripts

RUN adduser -D -H appuser
USER appuser

EXPOSE 8050
CMD ["python", "chart_consumer.py"]
```

- [ ] **Step 2: Verificar que buildea sin errores**

```bash
docker build -f Dockerfile.chart -t chart-consumer-test .
```
Expected: `Successfully built ...`

- [ ] **Step 3: Commit**

```bash
git add Dockerfile.chart
git commit -m "chore: Dockerfile.chart multi-stage Alpine para chart-consumer"
```

---

## Chunk 2: Script principal chart_consumer.py

### Task 3: chart_consumer.py

**Files:**
- Create: `scripts/chart_consumer.py`

- [ ] **Step 1: Crear scripts/chart_consumer.py**

```python
"""
Chart Consumer - Dashboard de clima en tiempo real
===================================================
Dos salidas simultaneas:
  - Terminal: bar chart con plotext cada 10 segundos (ver con docker logs -f chart-consumer)
  - Web:      dashboard Plotly.js en http://localhost:8050 (auto-refresca cada 5s)
"""

import json
import os
import threading
import time
from collections import OrderedDict

import plotext as plt
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string
from kafka import KafkaConsumer

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9093")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "weather-data")
REFRESH_WEB     = int(os.getenv("REFRESH_WEB", "5"))
REFRESH_TERM    = int(os.getenv("REFRESH_TERM", "10"))

# Estado compartido: {ciudad: ultimo_dato}
latest_by_city: dict = OrderedDict()
lock = threading.Lock()

# ============================================================
# TERMINAL — plotext
# ============================================================

def print_terminal_chart():
    with lock:
        if not latest_by_city:
            return
        cities = list(latest_by_city.keys())
        temps  = [latest_by_city[c]["temperature_c"] for c in cities]
        hums   = [latest_by_city[c]["humidity"]      for c in cities]

    print("\n" + "=" * 60)
    print("  TEMPERATURA (°C) — ultima lectura por ciudad")
    print("=" * 60)
    plt.clear_figure()
    plt.bar(cities, temps, marker="sd")
    plt.plotsize(60, 15)
    plt.theme("dark")
    plt.show()

    print("\n" + "=" * 60)
    print("  HUMEDAD (%) — ultima lectura por ciudad")
    print("=" * 60)
    plt.clear_figure()
    plt.bar(cities, hums, marker="sd", color="cyan")
    plt.plotsize(60, 15)
    plt.theme("dark")
    plt.show()
    print()


def terminal_printer_thread():
    """Imprime el chart a stdout cada REFRESH_TERM segundos."""
    while True:
        time.sleep(REFRESH_TERM)
        print_terminal_chart()


# ============================================================
# KAFKA CONSUMER THREAD
# ============================================================

def kafka_consumer_thread():
    print(f"[chart-consumer] Conectando a Kafka {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="chart-consumer",
    )
    print("[chart-consumer] Escuchando mensajes...")
    for msg in consumer:
        data = msg.value
        city = data.get("city", "Unknown")
        with lock:
            latest_by_city[city] = data
        print(f"[{data.get('timestamp')}] {city} | "
              f"{data.get('temperature_c')}°C | "
              f"{data.get('humidity')}% hum | "
              f"{data.get('condition')}")


# ============================================================
# FLASK WEB SERVER — Plotly.js
# ============================================================

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Weather Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #0d1117;
      color: #c9d1d9;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      padding: 24px;
    }
    h1 { color: #58a6ff; font-size: 1.6rem; margin-bottom: 4px; }
    .subtitle { color: #8b949e; font-size: 0.85rem; margin-bottom: 24px; }
    .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .chart-box {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 16px;
    }
    #last-update { color: #8b949e; font-size: 0.8rem; margin-top: 16px; }
    @media (max-width: 800px) { .charts { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <h1>Weather Dashboard — Live</h1>
  <p class="subtitle">Datos en tiempo real desde Kafka · Refresca cada {{ refresh }}s</p>
  <div class="charts">
    <div class="chart-box"><div id="chart-temp"></div></div>
    <div class="chart-box"><div id="chart-hum"></div></div>
  </div>
  <p id="last-update">Cargando...</p>

  <script>
    const BASE_LAYOUT = {
      paper_bgcolor: '#161b22',
      plot_bgcolor:  '#0d1117',
      font:  { color: '#c9d1d9', size: 12 },
      margin: { t: 40, b: 80, l: 50, r: 20 },
      xaxis: { gridcolor: '#21262d', tickangle: -35 },
      yaxis: { gridcolor: '#21262d' },
    };

    function draw(data) {
      if (!data.length) return;
      const cities     = data.map(d => d.city);
      const temps      = data.map(d => d.temperature_c);
      const hums       = data.map(d => d.humidity);
      const conditions = data.map(d => d.condition);

      Plotly.react('chart-temp', [{
        x: cities, y: temps, type: 'bar',
        marker: { color: temps, colorscale: 'RdYlBu_r', showscale: true,
                  colorbar: { title: '°C', thickness: 12 } },
        text: temps.map(t => t + '°C'), textposition: 'outside',
        hovertext: conditions, hoverinfo: 'x+y+text',
        name: 'Temperatura'
      }], { ...BASE_LAYOUT, title: { text: '🌡 Temperatura (°C)', font: { color: '#58a6ff' } } });

      Plotly.react('chart-hum', [{
        x: cities, y: hums, type: 'bar',
        marker: { color: hums, colorscale: 'Blues', showscale: true,
                  colorbar: { title: '%', thickness: 12 } },
        text: hums.map(h => h + '%'), textposition: 'outside',
        hoverinfo: 'x+y+text',
        name: 'Humedad'
      }], { ...BASE_LAYOUT, title: { text: '💧 Humedad (%)', font: { color: '#58a6ff' } } });

      document.getElementById('last-update').textContent =
        'Última actualización: ' + new Date().toLocaleTimeString() +
        ' · ' + data.length + ' ciudades';
    }

    function fetchAndDraw() {
      fetch('/data').then(r => r.json()).then(draw).catch(console.error);
    }

    fetchAndDraw();
    setInterval(fetchAndDraw, {{ refresh }} * 1000);
  </script>
</body>
</html>"""

app = Flask(__name__)


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE, refresh=REFRESH_WEB)


@app.route("/data")
def data():
    with lock:
        return jsonify(list(latest_by_city.values()))


# ============================================================
# MAIN
# ============================================================

def main():
    # Hilo 1: Kafka consumer
    t_kafka = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t_kafka.start()

    # Hilo 2: Terminal printer
    t_term = threading.Thread(target=terminal_printer_thread, daemon=True)
    t_term.start()

    # Hilo principal: Flask web server
    print(f"[chart-consumer] Dashboard web en http://0.0.0.0:8050")
    app.run(host="0.0.0.0", port=8050, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verificar sintaxis**

```bash
python -c "import ast; ast.parse(open('scripts/chart_consumer.py').read()); print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add scripts/chart_consumer.py
git commit -m "feat: chart_consumer con plotext (terminal) y Flask+Plotly (web)"
```

---

## Chunk 3: Servicio en docker-compose y prueba final

### Task 4: Añadir chart-consumer a docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Añadir servicio chart-consumer en docker-compose.yml**

Añadir después del servicio `spark-consumer`:

```yaml
  # ============================================
  # CHART CONSUMER - Dashboard web + terminal
  # Web: http://localhost:8050
  # Terminal: docker logs -f chart-consumer
  # ============================================
  chart-consumer:
    build:
      context: .
      dockerfile: Dockerfile.chart
    container_name: chart-consumer
    restart: unless-stopped
    env_file: .env
    environment:
      KAFKA_BOOTSTRAP: kafka:9093
      PYTHONUNBUFFERED: "1"
      REFRESH_WEB: "5"
      REFRESH_TERM: "10"
    ports:
      - "8050:8050"
    volumes:
      - ./scripts:/opt/spark-scripts
    depends_on:
      kafka:
        condition: service_healthy
    deploy:
      resources:
        limits:
          memory: 256M
          cpus: '0.5'
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    networks:
      - lab-network
```

- [ ] **Step 2: Build y arrancar**

```bash
docker compose build chart-consumer
docker compose up -d chart-consumer
```
Expected: `Container chart-consumer Started`

- [ ] **Step 3: Verificar terminal chart**

```bash
sleep 15 && docker logs chart-consumer 2>&1 | tail -30
```
Expected: logs de mensajes Kafka + gráfico plotext con barras Unicode

- [ ] **Step 4: Verificar web dashboard**

```bash
curl -s http://localhost:8050/data | python3 -m json.tool | head -20
```
Expected: JSON array con datos de ciudades

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: añadir servicio chart-consumer al docker-compose"
```

---

### Task 5: Actualizar documentación

**Files:**
- Modify: `README.md`
- Modify: `GUIA-LAB.md`

- [ ] **Step 1: Añadir chart-consumer a la tabla de servicios en README.md**

En la tabla de servicios añadir:
```markdown
| **chart-consumer** | `Dockerfile.chart` (Alpine) | `8050` | Dashboard web (Plotly.js) + gráficos en terminal |
```

- [ ] **Step 2: Añadir sección de interfaces web en README.md**

En la tabla de interfaces web añadir:
```markdown
| Chart Dashboard | [http://localhost:8050](http://localhost:8050) | Temperatura y humedad por ciudad en tiempo real |
```

- [ ] **Step 3: Añadir en GUIA-LAB.md**

En la tabla de servicios añadir:
```markdown
| chart-consumer | Dockerfile.chart (Alpine) | 8050 | Dashboard web Plotly + charts en terminal |
```

Y añadir al Paso 5 (Ver datos):
```markdown
# Chart dashboard (web + terminal simultaneo)
open http://localhost:8050          # Dashboard Plotly.js en el browser
docker logs -f chart-consumer       # Charts de barras en terminal (plotext)
```

- [ ] **Step 4: Commit y push**

```bash
git add README.md GUIA-LAB.md
git commit -m "docs: añadir chart-consumer a documentacion"
git push
```
