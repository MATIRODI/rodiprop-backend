from flask import Flask, jsonify, request
from flask_cors import CORS
import os, threading, time, requests, random, json, re
from bs4 import BeautifulSoup
from datetime import datetime

app = Flask(__name__)
CORS(app)

# DB - usar variables individuales de Railway
PG_HOST = os.environ.get("PGHOST", "postgres.railway.internal")
PG_USER = os.environ.get("PGUSER", "postgres")
PG_PASS = os.environ.get("PGPASSWORD", "eVWzxoJJMSiSkdoZxEuSNQmmaWVGlvPk")
PG_DB = os.environ.get("PGDATABASE", "railway")
PG_PORT = int(os.environ.get("PGPORT", "5432"))

# Twilio
TWILIO_SID = os.environ.get("TWILIO_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_TOKEN", "")
TWILIO_WA = os.environ.get("TWILIO_WA", "whatsapp:+14155238886")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "es-AR,es;q=0.9"}

def limpiar_precio(raw):
    if not raw: return ""
    raw = str(raw).split(" - ")[0].split("–")[0].split("-")[0] if len(str(raw)) > 15 else str(raw)
    nums = re.sub(r'[^0-9]', '', raw)
    return nums if nums else ""

def get_conn():
    import pg8000.dbapi as pg
    print(f"Connecting to {PG_HOST}:{PG_PORT} db={PG_DB} user={PG_USER}")
    return pg.connect(user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT, database=PG_DB)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS propiedades (
        id SERIAL PRIMARY KEY, titulo TEXT, precio TEXT, moneda TEXT DEFAULT 'USD',
        ubicacion TEXT, url TEXT UNIQUE, imagen TEXT, fuente TEXT, operacion TEXT,
        atributos TEXT, fecha TIMESTAMP DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS usuarios (
        id SERIAL PRIMARY KEY, nombre TEXT, email TEXT UNIQUE, whatsapp TEXT,
        zona TEXT, tipo TEXT, operacion TEXT DEFAULT 'venta',
        precio_min INTEGER DEFAULT 0, precio_max INTEGER DEFAULT 999999999,
        activo BOOLEAN DEFAULT TRUE, plan TEXT DEFAULT 'gratis', fecha TIMESTAMP DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS alertas_enviadas (
        id SERIAL PRIMARY KEY, usuario_id INTEGER, propiedad_url TEXT, fecha TIMESTAMP DEFAULT NOW())""")
    conn.commit()
    cur.close()
    conn.close()
    print("✅ DB inicializada")

def guardar_props(props):
    conn = get_conn()
    cur = conn.cursor()
    guardadas = 0
    for i, p in enumerate(props):
        try:
            url = p.get("url","").strip() or f"{p.get('fuente','')}_{i}"
            cur.execute("""INSERT INTO propiedades (titulo,precio,moneda,ubicacion,url,imagen,fuente,operacion,atributos)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (url) DO UPDATE SET precio=EXCLUDED.precio, imagen=EXCLUDED.imagen, fecha=NOW()""",
                (p.get("titulo","")[:500], limpiar_precio(p.get("precio","")), p.get("moneda","USD"),
                 p.get("ubicacion","")[:500], url[:1000], p.get("imagen","")[:1000],
                 p.get("fuente",""), p.get("operacion",""), json.dumps(p.get("atributos",[]))))
            guardadas += 1
        except Exception as e:
            try: conn.rollback()
            except: pass
    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 {guardadas}/{len(props)} props guardadas")

def contar_props():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM propiedades")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
        return total
    except Exception as e:
        print(f"Count error: {e}")
        return 0

def cargar_props(zona="", tipo="", operacion="", fuente="", limit=50):
    try:
        conn = get_conn()
        cur = conn.cursor()
        query = "SELECT titulo,precio,moneda,ubicacion,url,imagen,fuente,operacion,atributos FROM propiedades WHERE 1=1"
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
        cols = ["titulo","precio","moneda","ubicacion","url","imagen","fuente","operacion","atributos"]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if isinstance(r.get("atributos"), str):
                try: r["atributos"] = json.loads(r["atributos"])
                except: r["atributos"] = []
        return rows
    except Exception as e:
        print(f"Load error: {e}")
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

def scrape_ml(paginas=5):
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
                            props.append({"titulo": t.text.strip(), "precio": p.text.strip().replace(".","").replace(",",""),
                                "moneda": m.text.strip() if m else "USD", "ubicacion": u.text.strip() if u else "",
                                "url": l["href"] if l else "", "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "MercadoLibre", "operacion": op, "atributos": [a.text.strip() for a in attrs]})
                    except: pass
                print(f"✅ ML {op} p{i+1}: {len(props)}")
                time.sleep(random.uniform(1.5, 3))
            except Exception as e: print(f"❌ ML: {e}")
    return props

def scrape_ap(paginas=5):
    props = []
    s = requests.Session()
    try: s.get("https://www.argenprop.com", headers=get_headers(), timeout=10)
    except: pass
    for op in ["venta", "alquiler"]:
        for i in range(1, paginas+1):
            try:
                url = f"https://www.argenprop.com/propiedades-en-{op}-en-provincia-cordoba--pagina-{i}"
                r = s.get(url, headers=get_headers(), timeout=15)
                if r.status_code in [403, 429]: break
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
                        precio_raw = p.text.strip() if p else ""
                        precio = precio_raw.split(" - ")[0].split("–")[0].strip() if precio_raw else ""
                        href = l["href"] if l else ""
                        url_prop = href if href.startswith("http") else "https://www.argenprop.com" + href
                        if titulo or precio:
                            props.append({"titulo": titulo, "precio": precio, "moneda": "USD",
                                "ubicacion": u.text.strip() if u else "Córdoba", "url": url_prop,
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "ArgenProp", "operacion": op, "atributos": []})
                    except: pass
                print(f"✅ AP {op} p{i}: {len(cards)}")
                time.sleep(random.uniform(1.5, 3))
            except Exception as e: print(f"❌ AP: {e}")
    return props

def scrape_lavoz(paginas=5):
    props = []
    s = requests.Session()
    try: s.get("https://clasificados.lavoz.com.ar", headers=get_headers(), timeout=10)
    except: pass
    for op, slug in [("venta","venta"), ("alquiler","alquiler")]:
        for i in range(1, paginas+1):
            try:
                url = f"https://clasificados.lavoz.com.ar/inmuebles/{slug}?page={i}"
                r = s.get(url, headers=get_headers(), timeout=20)
                if r.status_code in [403, 429]: break
                soup = BeautifulSoup(r.text, "html.parser")
                cards = soup.select(".aviso") or soup.select("article") or soup.select(".card")
                for card in cards:
                    try:
                        t = card.select_one("h2") or card.select_one("h3") or card.select_one("[class*='title']")
                        p = card.select_one("[class*='price']") or card.select_one("[class*='precio']")
                        u = card.select_one("[class*='location']") or card.select_one("[class*='address']")
                        l = card.select_one("a[href]")
                        img = card.select_one("img")
                        titulo = t.text.strip() if t else ""
                        precio = p.text.strip() if p else ""
                        href = l["href"] if l and l.get("href") else ""
                        url_prop = href if href.startswith("http") else "https://clasificados.lavoz.com.ar" + href
                        if titulo or precio:
                            props.append({"titulo": titulo, "precio": precio, "moneda": "ARS",
                                "ubicacion": u.text.strip() if u else "Córdoba", "url": url_prop,
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "LaVoz", "operacion": op, "atributos": []})
                    except: pass
                print(f"✅ LaVoz {op} p{i}: {len(cards)}")
                time.sleep(random.uniform(1.5, 3))
            except Exception as e: print(f"❌ LaVoz: {e}")
    return props

def enviar_whatsapp(numero, mensaje):
    try:
        import urllib.request, urllib.parse, base64
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
        to = f"whatsapp:+549{numero}" if not numero.startswith("+") else f"whatsapp:{numero}"
        data = urllib.parse.urlencode({"From": TWILIO_WA, "To": to, "Body": mensaje}).encode()
        credentials = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Basic {credentials}")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            print(f"✅ WA enviado a {numero}: {result.get('sid','')}")
            return True
    except Exception as e:
        print(f"❌ WA error: {e}")
        return False

def chequear_alertas():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id,nombre,email,whatsapp,zona,tipo,operacion FROM usuarios WHERE activo=TRUE AND whatsapp != ''")
        cols = ["id","nombre","email","whatsapp","zona","tipo","operacion"]
        usuarios = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for u in usuarios:
            try:
                conn2 = get_conn()
                cur2 = conn2.cursor()
                query = "SELECT titulo,precio,moneda,ubicacion,url,fuente FROM propiedades WHERE fecha > NOW() - INTERVAL '3 hours'"
                params = []
                if u.get("zona"):
                    query += " AND (LOWER(ubicacion) LIKE %s OR LOWER(titulo) LIKE %s)"
                    params += [f"%{u['zona'].lower()}%", f"%{u['zona'].lower()}%"]
                if u.get("tipo"):
                    query += " AND LOWER(titulo) LIKE %s"
                    params.append(f"%{u['tipo'].lower()}%")
                if u.get("operacion"):
                    query += " AND LOWER(operacion) = %s"
                    params.append(u["operacion"].lower())
                query += " AND url NOT IN (SELECT propiedad_url FROM alertas_enviadas WHERE usuario_id=%s) LIMIT 3"
                params.append(u["id"])
                cur2.execute(query, params)
                for prop in cur2.fetchall():
                    titulo, precio, moneda, ubicacion, url, fuente = prop
                    precio_str = f"{moneda} {int(precio):,}" if precio and precio.isdigit() else precio or "Consultar"
                    mensaje = f"🏠 *RodiProp — Nueva propiedad!*\n\n{titulo}\n📍 {ubicacion}\n💰 {precio_str}\n🔗 {url}\n\n_Fuente: {fuente}_\nPara pausar respondé STOP"
                    if enviar_whatsapp(u["whatsapp"], mensaje):
                        cur2.execute("INSERT INTO alertas_enviadas (usuario_id, propiedad_url) VALUES (%s,%s) ON CONFLICT DO NOTHING", (u["id"], url))
                        conn2.commit()
                cur2.close()
                conn2.close()
            except Exception as e: print(f"Error usuario {u['id']}: {e}")
        print(f"✅ Alertas chequeadas: {len(usuarios)} usuarios")
    except Exception as e: print(f"Error alertas: {e}")

def run_scraper():
    print("🔍 Scraper iniciando...")
    todas = []
    for fn, name in [(scrape_ml, "ML"), (scrape_ap, "AP"), (scrape_lavoz, "LaVoz")]:
        try:
            r = fn(5)
            todas.extend(r)
            print(f"{name}: {len(r)}")
        except Exception as e: print(f"{name} fail: {e}")
    guardar_props(todas)
    print(f"✅ Total DB: {contar_props()}")

def auto_scraper():
    time.sleep(10)
    while True:
        try:
            run_scraper()
            chequear_alertas()
        except Exception as e: print(f"Auto error: {e}")
        time.sleep(7200)

try: init_db()
except Exception as e: print(f"DB init error: {e}")

threading.Thread(target=auto_scraper, daemon=True).start()

@app.route("/")
def home():
    return jsonify({"status": "RodiProp API OK", "version": "7.0", "pg_host": PG_HOST, "total": contar_props()})

@app.route("/api/propiedades")
def propiedades():
    props = cargar_props(zona=request.args.get("zona",""), tipo=request.args.get("tipo",""),
        operacion=request.args.get("operacion",""), fuente=request.args.get("fuente",""),
        limit=request.args.get("limit", 50, type=int))
    return jsonify({"total": len(props), "propiedades": props})

@app.route("/api/stats")
def stats():
    return jsonify(stats_db())

@app.route("/api/scraper/ejecutar", methods=["GET","POST"])
def trigger():
    threading.Thread(target=run_scraper, daemon=True).start()
    return jsonify({"status": "Scraper iniciado"})

@app.route("/api/usuarios/registro", methods=["POST","OPTIONS"])
def registro():
    if request.method == "OPTIONS": return jsonify({}), 200
    data = request.get_json()
    if not data: return jsonify({"error": "Datos requeridos"}), 400
    email = data.get("email","").strip().lower()
    if not email: return jsonify({"error": "Email requerido"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""INSERT INTO usuarios (nombre,email,whatsapp,zona,tipo,operacion,precio_min,precio_max)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (email) DO UPDATE SET nombre=EXCLUDED.nombre, whatsapp=EXCLUDED.whatsapp,
            zona=EXCLUDED.zona, tipo=EXCLUDED.tipo, operacion=EXCLUDED.operacion, activo=TRUE""",
            (data.get("nombre",""), email, data.get("whatsapp",""), data.get("zona",""),
             data.get("tipo",""), data.get("operacion","venta"),
             int(data.get("precio_min",0) or 0), int(data.get("precio_max",999999999) or 999999999)))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "mensaje": "¡Alerta creada!"})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/usuarios/lista")
def lista_usuarios():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id,nombre,email,whatsapp,zona,tipo,operacion,plan,fecha FROM usuarios ORDER BY fecha DESC")
        cols = ["id","nombre","email","whatsapp","zona","tipo","operacion","plan","fecha"]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if r.get("fecha"): r["fecha"] = str(r["fecha"])
        return jsonify({"total": len(rows), "usuarios": rows})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/alertas/test", methods=["GET","POST"])
def test_alerta():
    threading.Thread(target=chequear_alertas, daemon=True).start()
    return jsonify({"status": "Chequeando alertas en background"})

@app.route("/api/usuarios/stats")
def usuarios_stats():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM usuarios WHERE activo=TRUE")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({"total_usuarios": total})
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
