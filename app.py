from flask import Flask, jsonify, request
from flask_cors import CORS
import os, threading, time, requests, random, json
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:eVWzxoJJMSiSkdoZxEuSNQmmaWVGlvPk@postgres.railway.internal:5432/railway")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "es-AR,es;q=0.9"}

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS propiedades (
            id SERIAL PRIMARY KEY,
            titulo TEXT,
            precio TEXT,
            moneda TEXT DEFAULT 'USD',
            ubicacion TEXT,
            url TEXT UNIQUE,
            imagen TEXT,
            fuente TEXT,
            operacion TEXT,
            atributos TEXT,
            fecha TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ DB inicializada")

def guardar_props(props):
    conn = get_conn()
    cur = conn.cursor()
    guardadas = 0
    for p in props:
        try:
            cur.execute("""
                INSERT INTO propiedades (titulo, precio, moneda, ubicacion, url, imagen, fuente, operacion, atributos)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                    precio = EXCLUDED.precio,
                    imagen = EXCLUDED.imagen,
                    fecha = NOW()
            """, (
                p.get("titulo",""),
                p.get("precio",""),
                p.get("moneda","USD"),
                p.get("ubicacion",""),
                p.get("url","") or None,
                p.get("imagen",""),
                p.get("fuente",""),
                p.get("operacion",""),
                json.dumps(p.get("atributos",[]))
            ))
            guardadas += 1
        except Exception as e:
            conn.rollback()
    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 {guardadas} props guardadas en DB")

def contar_props():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM propiedades")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
        return total
    except:
        return 0

def cargar_props(zona="", tipo="", operacion="", fuente="", limit=50):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = "SELECT * FROM propiedades WHERE 1=1"
        params = []
        if zona:
            query += " AND (LOWER(ubicacion) LIKE %s OR LOWER(titulo) LIKE %s)"
            params += [f"%{zona.lower()}%", f"%{zona.lower()}%"]
        if tipo:
            query += " AND LOWER(titulo) LIKE %s"
            params.append(f"%{tipo.lower()}%")
        if operacion:
            query += " AND LOWER(operacion) = %s"
            params.append(operacion.lower())
        if fuente:
            query += " AND LOWER(fuente) LIKE %s"
            params.append(f"%{fuente.lower()}%")
        query += " ORDER BY fecha DESC LIMIT %s"
        params.append(limit)
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error cargando: {e}")
        return []

def stats_db():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT fuente, COUNT(*) FROM propiedades GROUP BY fuente")
        fuentes = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT COUNT(*) FROM propiedades")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"total": total, "por_fuente": fuentes}
    except Exception as e:
        return {"total": 0, "por_fuente": {}, "error": str(e)}

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
                        attrs = card.select(".poly-attributes-list__item")
                        if t and p:
                            props.append({
                                "titulo": t.text.strip(),
                                "precio": p.text.strip().replace(".",""),
                                "moneda": m.text.strip() if m else "USD",
                                "ubicacion": u.text.strip() if u else "",
                                "url": l["href"] if l else f"ml_{i}_{len(props)}",
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "MercadoLibre",
                                "operacion": op,
                                "atributos": [a.text.strip() for a in attrs]
                            })
                    except: pass
                print(f"✅ ML {op} p{i+1}: {len(props)} total")
                time.sleep(random.uniform(1,2))
            except Exception as e:
                print(f"❌ ML error: {e}")
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
                        href = l["href"] if l else ""
                        url_prop = href if href.startswith("http") else "https://www.argenprop.com" + href
                        if titulo or precio:
                            props.append({
                                "titulo": titulo,
                                "precio": precio,
                                "moneda": "USD",
                                "ubicacion": u.text.strip() if u else "",
                                "url": url_prop or f"ap_{i}_{len(props)}",
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "ArgenProp",
                                "operacion": op,
                                "atributos": []
                            })
                    except: pass
                print(f"✅ AP {op} p{i}: {len(cards)} cards")
                time.sleep(random.uniform(1,2))
            except Exception as e:
                print(f"❌ AP error: {e}")
    return props

def run_scraper():
    print("🔍 Scraper iniciando...")
    todas = scrape_ml(3) + scrape_ap(3)
    guardar_props(todas)
    total = contar_props()
    print(f"✅ Total en DB: {total}")

def auto_scraper():
    time.sleep(10)
    while True:
        try: run_scraper()
        except Exception as e: print(f"Auto error: {e}")
        time.sleep(7200)

# Inicializar DB y arrancar scraper
try:
    init_db()
except Exception as e:
    print(f"DB init error: {e}")

threading.Thread(target=auto_scraper, daemon=True).start()

@app.route("/")
def home():
    return jsonify({"status": "RodiProp API OK", "version": "4.0", "total": contar_props()})

@app.route("/api/propiedades")
def propiedades():
    props = cargar_props(
        zona=request.args.get("zona",""),
        tipo=request.args.get("tipo",""),
        operacion=request.args.get("operacion",""),
        fuente=request.args.get("fuente",""),
        limit=request.args.get("limit", 50, type=int)
    )
    # Serializar fechas
    for p in props:
        if "fecha" in p and p["fecha"]:
            p["fecha"] = str(p["fecha"])
        if "atributos" in p and isinstance(p["atributos"], str):
            try: p["atributos"] = json.loads(p["atributos"])
            except: p["atributos"] = []
    return jsonify({"total": len(props), "propiedades": props})

@app.route("/api/stats")
def stats():
    return jsonify(stats_db())

@app.route("/api/scraper/ejecutar", methods=["GET","POST"])
def trigger():
    threading.Thread(target=run_scraper, daemon=True).start()
    return jsonify({"status": "Scraper iniciado en background"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
