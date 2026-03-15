"""
Chart Consumer - Dashboard de clima en tiempo real
===================================================
Dos salidas simultaneas:
  - Terminal: bar charts con plotext cada 3 segundos
              (ver con: docker logs -f chart-consumer)
  - Web:      dashboard Plotly.js en http://localhost:8050
              (auto-refresca cada 1s, resalta ultimo dato)
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
REFRESH_WEB     = int(os.getenv("REFRESH_WEB", "1"))
REFRESH_TERM    = int(os.getenv("REFRESH_TERM", "3"))

# Estado compartido
latest_by_city: dict = OrderedDict()
last_received:  dict = {}          # ultimo mensaje recibido completo
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
        last   = last_received.copy()

    # Banner del ultimo dato recibido
    if last:
        print("\n" + "▶" * 65)
        print(f"  ÚLTIMO DATO:  {last.get('city')}, {last.get('country')}  |  "
              f"{last.get('temperature_c')}°C  |  {last.get('humidity')}% hum  |  "
              f"{last.get('condition')}  |  {last.get('timestamp')}")
        print("▶" * 65)

    # Chart temperatura — resaltar ciudad del ultimo dato
    last_city = last.get("city") if last else None
    print("\n" + "=" * 65)
    print(f"  TEMPERATURA (°C)  {'← ' + last_city if last_city else ''}")
    print("=" * 65)
    plt.clear_figure()
    colors = ["red+" if c == last_city else "blue+" for c in cities]
    plt.bar(cities, temps, marker="sd", color=colors)
    plt.plotsize(65, 15)
    plt.theme("dark")
    plt.show()

    print("\n" + "=" * 65)
    print(f"  HUMEDAD (%)  {'← ' + last_city if last_city else ''}")
    print("=" * 65)
    plt.clear_figure()
    colors = ["yellow+" if c == last_city else "cyan+" for c in cities]
    plt.bar(cities, hums, marker="sd", color=colors)
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
                    last_received.clear()
                    last_received.update(data)
                print(f"[{data.get('timestamp')}] ▶ {city}, {data.get('country')} | "
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
    .subtitle { color: #8b949e; font-size: 0.85rem; margin-bottom: 12px; }
    #last-banner {
      background: #1c2128;
      border: 1px solid #ffd700;
      border-radius: 6px;
      padding: 10px 16px;
      margin-bottom: 16px;
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 0.9rem;
    }
    #last-banner .dot {
      width: 10px; height: 10px;
      background: #ffd700;
      border-radius: 50%;
      animation: pulse 1s infinite;
      flex-shrink: 0;
    }
    @keyframes pulse {
      0%,100% { opacity: 1; transform: scale(1); }
      50%      { opacity: 0.4; transform: scale(1.4); }
    }
    #last-label { color: #ffd700; font-weight: 600; }
    #last-detail { color: #c9d1d9; }
    .charts {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-bottom: 12px;
    }
    .chart-box {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 16px;
    }
    #footer { color: #8b949e; font-size: 0.75rem; text-align: right; }
    @media (max-width: 800px) { .charts { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <h1>Weather Dashboard — Live</h1>
  <p class="subtitle">Datos en tiempo real desde Kafka · Refresca cada {{ refresh }}s</p>

  <div id="last-banner">
    <div class="dot"></div>
    <span id="last-label">Esperando datos...</span>
    <span id="last-detail"></span>
  </div>

  <div class="charts">
    <div class="chart-box"><div id="chart-temp"></div></div>
    <div class="chart-box"><div id="chart-hum"></div></div>
  </div>
  <p id="footer">—</p>

  <script>
    const BASE_LAYOUT = {
      paper_bgcolor: '#161b22',
      plot_bgcolor:  '#0d1117',
      font:  { color: '#c9d1d9', size: 12 },
      margin: { t: 50, b: 90, l: 50, r: 20 },
      xaxis: { gridcolor: '#21262d', tickangle: -35 },
      yaxis: { gridcolor: '#21262d' },
    };

    function draw(resp) {
      const data     = resp.cities;
      const lastCity = resp.last_city;
      if (!data.length) return;

      const cities     = data.map(d => d.city);
      const temps      = data.map(d => d.temperature_c);
      const hums       = data.map(d => d.humidity);
      const conditions = data.map(d => `${d.condition} | ${d.wind_kph} kph`);
      const lastIdx    = cities.indexOf(lastCity);
      const lastData   = lastIdx >= 0 ? data[lastIdx] : null;

      // Colores: dorado para el ultimo, escala normal para el resto
      const tempColors = cities.map((c, i) =>
        c === lastCity ? '#ffd700' : temps[i]);
      const humColors  = cities.map(c =>
        c === lastCity ? '#ffd700' : null);

      // Bordes resaltados en el ultimo
      const markerLine = {
        color:  cities.map(c => c === lastCity ? '#ffffff' : 'rgba(0,0,0,0)'),
        width:  cities.map(c => c === lastCity ? 2.5 : 0),
      };

      // Anotacion "← último" encima de la barra
      const anno = lastIdx >= 0 ? [{
        x: lastCity,
        y: Math.max(...(lastIdx >= 0 ? [temps[lastIdx]] : temps)) + 1.5,
        text: '▶ último',
        showarrow: true,
        arrowcolor: '#ffd700',
        arrowsize: 1,
        arrowwidth: 1.5,
        ax: 0, ay: -28,
        font: { color: '#ffd700', size: 11 },
        bgcolor: 'rgba(0,0,0,0)',
      }] : [];

      Plotly.react('chart-temp', [{
        x: cities, y: temps, type: 'bar',
        marker: {
          color: tempColors,
          colorscale: 'RdYlBu_r',
          showscale: true,
          colorbar: { title: '°C', thickness: 14, len: 0.8 },
          line: markerLine,
        },
        text: temps.map(t => t + '°C'), textposition: 'outside',
        hovertext: conditions, hoverinfo: 'x+y+text',
        name: 'Temperatura',
      }], {
        ...BASE_LAYOUT,
        title: { text: '🌡 Temperatura (°C)', font: { color: '#58a6ff', size: 15 } },
        yaxis: { ...BASE_LAYOUT.yaxis, title: '°C' },
        annotations: anno,
      });

      const annoHum = lastIdx >= 0 ? [{
        x: lastCity,
        y: hums[lastIdx] + 3,
        text: '▶ último',
        showarrow: true,
        arrowcolor: '#ffd700',
        arrowsize: 1,
        arrowwidth: 1.5,
        ax: 0, ay: -28,
        font: { color: '#ffd700', size: 11 },
        bgcolor: 'rgba(0,0,0,0)',
      }] : [];

      Plotly.react('chart-hum', [{
        x: cities, y: hums, type: 'bar',
        marker: {
          color: humColors,
          colorscale: 'Blues',
          showscale: true,
          colorbar: { title: '%', thickness: 14, len: 0.8 },
          line: markerLine,
        },
        text: hums.map(h => h + '%'), textposition: 'outside',
        hoverinfo: 'x+y+text',
        name: 'Humedad',
      }], {
        ...BASE_LAYOUT,
        title: { text: '💧 Humedad (%)', font: { color: '#58a6ff', size: 15 } },
        yaxis: { ...BASE_LAYOUT.yaxis, title: '%', range: [0, 115] },
        annotations: annoHum,
      });

      // Banner ultimo dato
      if (lastData) {
        document.getElementById('last-label').textContent =
          `▶  ${lastData.city}, ${lastData.country}`;
        document.getElementById('last-detail').textContent =
          `${lastData.temperature_c}°C  ·  ${lastData.humidity}% hum  ·  `+
          `${lastData.wind_kph} kph  ·  ${lastData.condition}  ·  ${lastData.timestamp}`;
      }

      document.getElementById('footer').textContent =
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
        return jsonify({
            "cities":    list(latest_by_city.values()),
            "last_city": last_received.get("city"),
        })


# ============================================================
# MAIN
# ============================================================

def main():
    t_kafka = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t_kafka.start()

    t_term = threading.Thread(target=terminal_printer_thread, daemon=True)
    t_term.start()

    print(f"[chart-consumer] Dashboard web en http://0.0.0.0:8050")
    app.run(host="0.0.0.0", port=8050, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
