"""
Chart Consumer - Dashboard de clima en tiempo real
===================================================
Dos salidas simultaneas:
  - Terminal: bar charts con plotext cada 10 segundos
              (ver con: docker logs -f chart-consumer)
  - Web:      dashboard Plotly.js en http://localhost:8050
              (auto-refresca cada 5s)
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

    print("\n" + "=" * 65)
    print("  TEMPERATURA (°C) — ultima lectura por ciudad")
    print("=" * 65)
    plt.clear_figure()
    plt.bar(cities, temps, marker="sd")
    plt.plotsize(65, 15)
    plt.theme("dark")
    plt.show()

    print("\n" + "=" * 65)
    print("  HUMEDAD (%) — ultima lectura por ciudad")
    print("=" * 65)
    plt.clear_figure()
    plt.bar(cities, hums, marker="sd", color="cyan+")
    plt.plotsize(65, 15)
    plt.theme("dark")
    plt.show()
    print()


def terminal_printer_thread():
    """Imprime charts a stdout cada REFRESH_TERM segundos."""
    while True:
        time.sleep(REFRESH_TERM)
        print_terminal_chart()


# ============================================================
# KAFKA CONSUMER THREAD
# ============================================================

def kafka_consumer_thread():
    while True:
        try:
            print(f"[chart-consumer] Conectando a Kafka {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}")
            # group_id unico por arranque: sin offsets comprometidos previos
            # auto_offset_reset='earliest': carga historico al arrancar
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id=f"chart-consumer-{int(time.time())}",
                enable_auto_commit=False,
            )
            print("[chart-consumer] Escuchando mensajes...")
            for msg in consumer:
                data = msg.value
                city = data.get("city", "Unknown")
                with lock:
                    latest_by_city[city] = data
                print(f"[{data.get('timestamp')}] {city}, {data.get('country')} | "
                      f"{data.get('temperature_c')}°C | "
                      f"{data.get('humidity')}% hum | "
                      f"{data.get('condition')}")
        except Exception as e:
            print(f"[chart-consumer] Error en Kafka consumer: {e}. Reintentando en 5s...")
            time.sleep(5)


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
    .charts {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-bottom: 16px;
    }
    .chart-box {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 16px;
    }
    #last-update {
      color: #8b949e;
      font-size: 0.8rem;
      text-align: right;
    }
    @media (max-width: 800px) {
      .charts { grid-template-columns: 1fr; }
    }
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
      margin: { t: 50, b: 90, l: 50, r: 20 },
      xaxis: { gridcolor: '#21262d', tickangle: -35 },
      yaxis: { gridcolor: '#21262d' },
    };

    function draw(data) {
      if (!data.length) return;

      const cities     = data.map(d => d.city);
      const temps      = data.map(d => d.temperature_c);
      const hums       = data.map(d => d.humidity);
      const conditions = data.map(d => `${d.condition} | ${d.wind_kph} kph`);

      Plotly.react('chart-temp', [{
        x: cities,
        y: temps,
        type: 'bar',
        marker: {
          color: temps,
          colorscale: 'RdYlBu_r',
          showscale: true,
          colorbar: { title: '°C', thickness: 14, len: 0.8 }
        },
        text: temps.map(t => t + '°C'),
        textposition: 'outside',
        hovertext: conditions,
        hoverinfo: 'x+y+text',
        name: 'Temperatura'
      }], {
        ...BASE_LAYOUT,
        title: { text: '🌡 Temperatura (°C)', font: { color: '#58a6ff', size: 15 } },
        yaxis: { ...BASE_LAYOUT.yaxis, title: '°C' }
      });

      Plotly.react('chart-hum', [{
        x: cities,
        y: hums,
        type: 'bar',
        marker: {
          color: hums,
          colorscale: 'Blues',
          showscale: true,
          colorbar: { title: '%', thickness: 14, len: 0.8 }
        },
        text: hums.map(h => h + '%'),
        textposition: 'outside',
        hoverinfo: 'x+y+text',
        name: 'Humedad'
      }], {
        ...BASE_LAYOUT,
        title: { text: '💧 Humedad (%)', font: { color: '#58a6ff', size: 15 } },
        yaxis: { ...BASE_LAYOUT.yaxis, title: '%', range: [0, 110] }
      });

      document.getElementById('last-update').textContent =
        'Última actualización: ' + new Date().toLocaleTimeString() +
        ' · ' + data.length + ' ciudades activas';
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

    # Hilo principal: Flask
    print(f"[chart-consumer] Dashboard web en http://0.0.0.0:8050")
    app.run(host="0.0.0.0", port=8050, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
