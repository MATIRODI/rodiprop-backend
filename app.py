from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
import threading
import time
from scraper import scrape_mercadolibre, scrape_zonaprop, guardar_json

app = Flask(__name__)
CORS(app)  # Permite que la landing en GitHub Pages consuma la API

DATA_FILE = "propiedades.json"

def cargar_propiedades():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def scraper_automatico():
    """Corre el scraper cada 2 horas automáticamente"""
    while True:
        print("🔄 Actualizando propiedades...")
        props_ml = scrape_mercadolibre(zona="cordoba", paginas=5)
        props_zp = scrape_zonaprop(zona="cordoba", paginas=5)
        todas = props_ml + props_zp
        guardar_json(todas)
        print(f"✅ {len(todas)} propiedades actualizadas")
        time.sleep(7200)  # 2 horas

@app.route("/")
def home():
    return jsonify({"status": "RodiProp API funcionando", "version": "1.0"})

@app.route("/api/propiedades")
def propiedades():
    props = cargar_propiedades()
    
    # Filtros opcionales
    operacion = request.args.get("operacion", "")
    tipo = request.args.get("tipo", "")
    zona = request.args.get("zona", "")
    precio_min = request.args.get("precio_min", 0, type=int)
    precio_max = request.args.get("precio_max", 99999999, type=int)
    fuente = request.args.get("fuente", "")
    limit = request.args.get("limit", 50, type=int)
    
    filtradas = []
    for p in props:
        titulo = p.get("titulo", "").lower()
        ubicacion = p.get("ubicacion", "").lower()
        
        if zona and zona.lower() not in ubicacion:
            continue
        if tipo and tipo.lower() not in titulo:
            continue
        if fuente and fuente.lower() != p.get("fuente", "").lower():
            continue
        try:
            precio = int(str(p.get("precio", "0")).replace(".", "").replace(",", ""))
            if precio < precio_min or precio > precio_max:
                continue
        except:
            pass
        
        filtradas.append(p)
    
    return jsonify({
        "total": len(filtradas),
        "propiedades": filtradas[:limit],
        "fuentes": list(set(p.get("fuente","") for p in props))
    })

@app.route("/api/stats")
def stats():
    props = cargar_propiedades()
    fuentes = {}
    for p in props:
        f = p.get("fuente", "Otro")
        fuentes[f] = fuentes.get(f, 0) + 1
    return jsonify({
        "total": len(props),
        "por_fuente": fuentes
    })

@app.route("/api/scraper/ejecutar", methods=["GET", "POST"])
def ejecutar_scraper():
    """Ejecuta el scraper manualmente"""
    def run():
        props_ml = scrape_mercadolibre(zona="cordoba", paginas=3)
        props_zp = scrape_zonaprop(zona="cordoba", paginas=3)
        guardar_json(props_ml + props_zp)
    threading.Thread(target=run).start()
    return jsonify({"status": "Scraper iniciado en background"})

if __name__ == "__main__":
    # Arrancar scraper automático en background
    t = threading.Thread(target=scraper_automatico, daemon=True)
    t.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
