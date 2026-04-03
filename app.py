from flask import Flask, jsonify, request
from flask_cors import CORS
import json, os, threading, time, requests, random
from bs4 import BeautifulSoup
from datetime import datetime

app = Flask(__name__)
CORS(app)

DATA_FILE = "/tmp/props.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "es-AR,es;q=0.9"}

def cargar():
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE) as f:
                return json.load(f)
    except:
        pass
    return []

def guardar(props):
    vistas, unicas = set(), []
    for p in props:
        k = p.get("url") or (p.get("titulo","") + p.get("precio",""))
        if k and k not in vistas:
            vistas.add(k)
            unicas.append(p)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False)
    print(f"💾 {len(unicas)} props guardadas")
    return unicas

def scrape_ml(paginas=3):
    props = []
    for op in ["venta", "alquiler"]:
        for i in range(paginas):
            try:
                url = f"https://inmuebles.mercadolibre.com.ar/{op}/cordoba/_Desde_{i*48+1}_DisplayType_G"
                r = requests.get(url, headers=get_headers(), timeout=15)
                soup = BeautifulSoup(r.text, "html.parser")
                for card in soup.select(".ui-search-layout__item"):
                    try:
                        t = card.select_one(".poly-component__title")
                        p = card.select_one(".andes-money-amount__fraction")
                        m = card.select_one(".andes-money-amount__currency-symbol")
                        u = card.select_one(".poly-component__location")
                        l = card.select_one("a.poly-component__title")
                        img = card.select_one("img.poly-component__picture")
                        if t and p:
                            props.append({
                                "titulo": t.text.strip(),
                                "precio": p.text.strip().replace(".",""),
                                "moneda": m.text.strip() if m else "USD",
                                "ubicacion": u.text.strip() if u else "",
                                "url": l["href"] if l else "",
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "MercadoLibre",
                                "operacion": op,
                                "fecha": datetime.now().isoformat()
                            })
                    except: pass
                print(f"ML {op} p{i+1}: {len(props)}")
                time.sleep(random.uniform(1,2))
            except Exception as e:
                print(f"ML error: {e}")
    return props

def scrape_ap(paginas=3):
    props = []
    s = requests.Session()
    try: s.get("https://www.argenprop.com", headers=get_headers(), timeout=10)
    except: pass
    for op in ["venta", "alquiler"]:
        for i in range(1, paginas+1):
            try:
                url = f"https://www.argenprop.com/departamento-y-casa-en-{op}-en-cordoba--pagina-{i}"
                r = s.get(url, headers=get_headers(), timeout=15)
                if r.status_code == 403: break
                soup = BeautifulSoup(r.text, "html.parser")
                cards = soup.select(".listing__item") or soup.select("article")
                for card in cards:
                    try:
                        t = card.select_one(".card__title") or card.select_one("h2")
                        p = card.select_one(".card__price") or card.select_one("[class*='price']")
                        u = card.select_one(".card__address") or card.select_one("[class*='address']")
                        l = card.select_one("a[href]")
                        img = card.select_one("img")
                        titulo = t.text.strip() if t else ""
                        precio = p.text.strip().replace(".","").replace("$","").strip() if p else ""
                        if titulo or precio:
                            href = l["href"] if l else ""
                            props.append({
                                "titulo": titulo,
                                "precio": precio,
                                "moneda": "USD",
                                "ubicacion": u.text.strip() if u else "",
                                "url": href if href.startswith("http") else "https://www.argenprop.com" + href,
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "ArgenProp",
                                "operacion": op,
                                "fecha": datetime.now().isoformat()
                            })
                    except: pass
                print(f"AP {op} p{i}: {len(cards)} cards")
                time.sleep(random.uniform(1,2))
            except Exception as e:
                print(f"AP error: {e}")
    return props

def run_scraper():
    print("🔍 Scraper iniciando...")
    todas = scrape_ml(3) + scrape_ap(3)
    unicas = guardar(todas)
    print(f"✅ Total: {len(unicas)}")

def auto_scraper():
    time.sleep(5)
    while True:
        try: run_scraper()
        except Exception as e: print(f"Auto error: {e}")
        time.sleep(7200)

@app.route("/")
def home():
    return jsonify({"status": "RodiProp API OK", "version": "3.0", "total": len(cargar())})

@app.route("/api/propiedades")
def propiedades():
    props = cargar()
    zona = request.args.get("zona","").lower()
    tipo = request.args.get("tipo","").lower()
    op = request.args.get("operacion","").lower()
    fuente = request.args.get("fuente","").lower()
    limit = request.args.get("limit", 50, type=int)
    result = []
    for p in props:
        if zona and zona not in p.get("ubicacion","").lower() and zona not in p.get("titulo","").lower(): continue
        if tipo and tipo not in p.get("titulo","").lower(): continue
        if op and op != p.get("operacion","").lower(): continue
        if fuente and fuente not in p.get("fuente","").lower(): continue
        result.append(p)
    return jsonify({"total": len(result), "propiedades": result[:limit], "fuentes": list(set(p.get("fuente","") for p in props))})

@app.route("/api/stats")
def stats():
    props = cargar()
    fuentes = {}
    for p in props:
        f = p.get("fuente","Otro")
        fuentes[f] = fuentes.get(f,0) + 1
    return jsonify({"total": len(props), "por_fuente": fuentes})

@app.route("/api/scraper/ejecutar", methods=["GET","POST"])
def trigger():
    threading.Thread(target=run_scraper, daemon=True).start()
    return jsonify({"status": "Scraper iniciado en background"})

threading.Thread(target=auto_scraper, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
