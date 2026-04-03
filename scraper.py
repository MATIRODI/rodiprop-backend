from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
import threading
import time
from scraper import scrape_mercadolibre, scrape_zonaprop, scrape_argenprop

app = Flask(__name__)
CORS(app)

DATA_FILE = "/tmp/propiedades.json"  # Usar /tmp que persiste más

def cargar_propiedades():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def guardar_propiedades(props):
    # Deduplicar
    vistas = set()
    unicas = []
    for p in props:
        key = p.get("url") or p.get("titulo", "") + p.get("precio", "")
        if key and key not in vistas:
            vistas.add(key)
            unicas.append(p)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False, indent=2)
    print(f"💾 {len(unicas)} propiedades guardadas en {DATA_FILE}")
    return unicas

def scraper_automatico():
    """Corre el scraper cada 2 horas"""
    while True:
        try:
            print("🔄 Actualizando propiedades...")
            todas = []
            
            props_ml = scrape_mercadolibre(paginas=5)
            print(f"ML: {len(props_ml)}")
            todas.extend(props_ml)
            
            props_zp = scrape_zonaprop(paginas=3)
            print(f"ZP: {len(props_zp)}")
            todas.extend(props_zp)
            
            props_ap = scrape_argenprop(paginas=3)
            print(f"AP: {len(props_ap)}")
            todas.extend(props_ap)
            
            unicas = guardar_propiedades(todas)
            print(f"✅ Total: {len(unicas)} propiedades únicas")
        except Exception as e:
            print(f"❌ Error scraper: {e}")
        
        time.sleep(7200)  # 2 horas

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
    precio_min = request.args.get("precio_min", 0, type=int)
    precio_max = request.args.get("precio_max", 99999999, type=int)
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
        try:
            precio = int(str(p.get("precio", "0")).replace(".", "").replace(",", "").strip() or "0")
            if precio_min and precio < precio_min:
                continue
            if precio_max < 99999999 and precio > precio_max:
                continue
        except:
            pass
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
    operaciones = {}
    for p in props:
        f = p.get("fuente", "Otro")
        fuentes[f] = fuentes.get(f, 0) + 1
        o = p.get("operacion", "otro")
        operaciones[o] = operaciones.get(o, 0) + 1
    return jsonify({
        "total": len(props),
        "por_fuente": fuentes,
        "por_operacion": operaciones
    })

@app.route("/api/scraper/ejecutar", methods=["GET", "POST"])
def ejecutar_scraper():
    def run():
        try:
            todas = []
            props_ml = scrape_mercadolibre(paginas=3)
            todas.extend(props_ml)
            props_zp = scrape_zonaprop(paginas=3)
            todas.extend(props_zp)
            props_ap = scrape_argenprop(paginas=3)
            todas.extend(props_ap)
            guardar_propiedades(todas)
            print(f"✅ Scraper completado: {len(todas)} props")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "Scraper iniciado en background"})

# Arrancar scraper al iniciar
scraper_thread = threading.Thread(target=scraper_automatico, daemon=True)
scraper_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
