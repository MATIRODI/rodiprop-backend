from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
import threading
import time

app = Flask(__name__)
CORS(app)

DATA_FILE = "/tmp/propiedades.json"

def cargar_propiedades():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def guardar_propiedades(props):
    vistas = set()
    unicas = []
    for p in props:
        key = p.get("url") or p.get("titulo", "") + p.get("precio", "")
        if key and key not in vistas:
            vistas.add(key)
            unicas.append(p)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False, indent=2)
    print(f"💾 {len(unicas)} propiedades guardadas")
    return unicas

def ejecutar_scraping():
    from scraper import scrape_mercadolibre, scrape_zonaprop, scrape_argenprop
    todas = []
    try:
        props_ml = scrape_mercadolibre(paginas=3)
        todas.extend(props_ml)
        print(f"ML: {len(props_ml)}")
    except Exception as e:
        print(f"ML error: {e}")
    try:
        props_zp = scrape_zonaprop(paginas=3)
        todas.extend(props_zp)
        print(f"ZP: {len(props_zp)}")
    except Exception as e:
        print(f"ZP error: {e}")
    try:
        props_ap = scrape_argenprop(paginas=3)
        todas.extend(props_ap)
        print(f"AP: {len(props_ap)}")
    except Exception as e:
        print(f"AP error: {e}")
    return guardar_propiedades(todas)

def scraper_automatico():
    time.sleep(10)  # Esperar que el servidor arranque
    while True:
        try:
            print("🔄 Scraper automático iniciando...")
            unicas = ejecutar_scraping()
            print(f"✅ Total: {len(unicas)} propiedades")
        except Exception as e:
            print(f"❌ Error: {e}")
        time.sleep(7200)

@app.route("/")
def home():
    props = cargar_propiedades()
    return jsonify({"status": "RodiProp API funcionando", "version": "2.0", "total": len(props)})

@app.route("/api/propiedades")
def propiedades():
    props = cargar_propiedades()
    zona = request.args.get("zona", "").lower()
    tipo = request.args.get("tipo", "").lower()
    operacion = request.args.get("operacion", "").lower()
    fuente = request.args.get("fuente", "").lower()
    limit = request.args.get("limit", 50, type=int)

    filtradas = []
    for p in props:
        titulo = p.get("titulo", "").lower()
        ubicacion = p.get("ubicacion", "").lower()
        if zona and zona not in ubicacion and zona not in titulo:
            continue
        if tipo and tipo not in titulo:
            continue
        if operacion and operacion != p.get("operacion", "").lower():
            continue
        if fuente and fuente.lower() not in p.get("fuente", "").lower():
            continue
        filtradas.append(p)

    return jsonify({
        "total": len(filtradas),
        "propiedades": filtradas[:limit],
        "fuentes": list(set(p.get("fuente", "") for p in props))
    })

@app.route("/api/stats")
def stats():
    props = cargar_propiedades()
    fuentes = {}
    for p in props:
        f = p.get("fuente", "Otro")
        fuentes[f] = fuentes.get(f, 0) + 1
    return jsonify({"total": len(props), "por_fuente": fuentes})

@app.route("/api/scraper/ejecutar", methods=["GET", "POST"])
def trigger_scraper():
    def run():
        try:
            unicas = ejecutar_scraping()
            print(f"✅ Scraper manual: {len(unicas)} props")
        except Exception as e:
            print(f"❌ Error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "Scraper iniciado en background"})

# Arrancar scraper automático
threading.Thread(target=scraper_automatico, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
