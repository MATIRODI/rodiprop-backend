from flask import Flask, jsonify, request
from flask_cors import CORS
import os, threading, time, requests, random, json, re
from bs4 import BeautifulSoup
from datetime import datetime
import pg8000.dbapi as pg

# Twilio config
TWILIO_SID = os.environ.get("TWILIO_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_TOKEN", "")
TWILIO_WA = os.environ.get("TWILIO_WA", "whatsapp:+14155238886")

app = Flask(__name__)
CORS(app)

DB_URL = os.environ.get("DATABASE_URL", "")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "es-AR,es;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

def limpiar_precio(raw):
    """Extrae el primer número válido de un string de precio"""
    if not raw:
        return ""
    # Tomar solo la primera parte si hay rango
    raw = str(raw).split(" - ")[0].split("–")[0].split("-")[0] if len(str(raw)) > 15 else str(raw)
    # Eliminar todo excepto números
    nums = re.sub(r'[^0-9]', '', raw)
    return nums if nums else ""

def get_conn():
    """Conectar usando DATABASE_URL directamente"""
    url = DB_URL.replace("postgresql://","").replace("postgres://","")
    # formato: user:password@host:port/db
    at_idx = url.rfind("@")
    user_pass = url[:at_idx]
    host_rest = url[at_idx+1:]
    
    # user:password
    colon_idx = user_pass.find(":")
    user = user_pass[:colon_idx]
    password = user_pass[colon_idx+1:]
    
    # host:port/db
    slash_idx = host_rest.rfind("/")
    db = host_rest[slash_idx+1:]
    host_port = host_rest[:slash_idx]
    
    if ":" in host_port:
        host, port = host_port.rsplit(":", 1)
        port = int(port)
    else:
        host = host_port
        port = 5432
    
    return pg.connect(user=user, password=password, host=host, port=port, database=db)

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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            nombre TEXT,
            email TEXT UNIQUE,
            whatsapp TEXT,
            zona TEXT,
            tipo TEXT,
            operacion TEXT DEFAULT 'venta',
            precio_min INTEGER DEFAULT 0,
            precio_max INTEGER DEFAULT 999999999,
            activo BOOLEAN DEFAULT TRUE,
            plan TEXT DEFAULT 'gratis',
            fecha TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alertas_enviadas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER,
            propiedad_url TEXT,
            fecha TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ DB inicializada con usuarios y alertas")

def guardar_props(props):
    conn = get_conn()
    cur = conn.cursor()
    guardadas = 0
    for p in props:
        try:
            url = p.get("url","").strip() or f"{p.get('fuente','')}_{p.get('titulo',''[:50])}_{guardadas}"
            cur.execute("""
                INSERT INTO propiedades (titulo, precio, moneda, ubicacion, url, imagen, fuente, operacion, atributos)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (url) DO UPDATE SET
                    precio=EXCLUDED.precio, imagen=EXCLUDED.imagen, fecha=NOW()
            """, (
                p.get("titulo","")[:500],
                limpiar_precio(p.get("precio","")),
                p.get("moneda","USD"),
                p.get("ubicacion","")[:500],
                url[:1000],
                p.get("imagen","")[:1000],
                p.get("fuente",""),
                p.get("operacion",""),
                json.dumps(p.get("atributos",[]))
            ))
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
    except:
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


# ─────────────────────────────────────────
# SCRAPERS
# ─────────────────────────────────────────

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
                            props.append({
                                "titulo": t.text.strip(),
                                "precio": p.text.strip().replace(".","").replace(",",""),
                                "moneda": m.text.strip() if m else "USD",
                                "ubicacion": u.text.strip() if u else "",
                                "url": l["href"] if l else "",
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "MercadoLibre",
                                "operacion": op,
                                "atributos": [a.text.strip() for a in attrs]
                            })
                    except: pass
                print(f"✅ ML {op} p{i+1}: {len(props)} total")
                time.sleep(random.uniform(1.5, 3))
            except Exception as e:
                print(f"❌ ML error: {e}")
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
                        # Limpiar rango: tomar solo primer valor
                        precio = precio_raw.split(" - ")[0].split("–")[0].strip() if precio_raw else ""
                        href = l["href"] if l else ""
                        url_prop = href if href.startswith("http") else "https://www.argenprop.com" + href
                        if titulo or precio:
                            props.append({
                                "titulo": titulo,
                                "precio": precio,
                                "moneda": "USD",
                                "ubicacion": u.text.strip() if u else "Córdoba",
                                "url": url_prop,
                                "imagen": (img.get("data-src") or img.get("src","")) if img else "",
                                "fuente": "ArgenProp",
                                "operacion": op,
                                "atributos": []
                            })
                    except: pass
                print(f"✅ AP {op} p{i}: {len(cards)} cards")
                time.sleep(random.uniform(1.5, 3))
            except Exception as e:
                print(f"❌ AP error: {e}")
    return props


def scrape_zonaprop(paginas=5):
    props = []
    s = requests.Session()
    try: s.get("https://www.zonaprop.com.ar", headers=get_headers(), timeout=10)
    except: pass
    for op, slug in [("venta","venta"), ("alquiler","alquiler")]:
        for i in range(1, paginas+1):
            try:
                # ZonaProp con headers más completos para evitar bloqueo
                url = f"https://www.zonaprop.com.ar/inmuebles-{slug}-cordoba-pagina-{i}.html"
                hdrs = get_headers()
                hdrs["Referer"] = "https://www.zonaprop.com.ar/"
                hdrs["sec-ch-ua"] = '"Not_A Brand";v="8", "Chromium";v="120"'
                hdrs["sec-fetch-site"] = "same-origin"
                hdrs["sec-fetch-mode"] = "navigate"
                r = s.get(url, headers=hdrs, timeout=20)
                if r.status_code in [403, 429]:
                    print(f"ZP bloqueado p{i}")
                    break
                soup = BeautifulSoup(r.text, "html.parser")
                cards = soup.select("div[data-id]") or soup.select("[class*='postingCard']")
                for card in cards:
                    try:
                        precio_el = card.select_one("[data-price]") or card.select_one(".firstPrice")
                        titulo_el = card.select_one(".postingCardTitle") or card.select_one("h2")
                        ubicacion_el = card.select_one(".postingCardLocation") or card.select_one("[class*='location']")
                        link_el = card.select_one("a[href*='/propiedades/']") or card.select_one("a")
                        img_el = card.select_one("img")
                        attrs = card.select(".postingCardAttribute") or []

                        precio = precio_el.get("data-price","") if precio_el else ""
                        if not precio and precio_el:
                            precio = precio_el.text.strip()
                        titulo = titulo_el.text.strip() if titulo_el else ""
                        ubicacion = ubicacion_el.text.strip() if ubicacion_el else "Córdoba"
                        href = link_el["href"] if link_el and link_el.get("href") else ""
                        url_prop = href if href.startswith("http") else "https://www.zonaprop.com.ar" + href
                        imagen = (img_el.get("data-src") or img_el.get("src","")) if img_el else ""

                        if titulo or precio:
                            props.append({
                                "titulo": titulo,
                                "precio": str(precio),
                                "moneda": "USD",
                                "ubicacion": ubicacion,
                                "url": url_prop,
                                "imagen": imagen,
                                "fuente": "ZonaProp",
                                "operacion": op,
                                "atributos": [a.text.strip() for a in attrs]
                            })
                    except: pass
                print(f"✅ ZP {op} p{i}: {len(cards)} cards")
                time.sleep(random.uniform(2, 4))
            except Exception as e:
                print(f"❌ ZP error: {e}")
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
                cards = soup.select(".aviso") or soup.select("[class*='listing']") or soup.select("article") or soup.select(".card")
                for card in cards:
                    try:
                        t = card.select_one("h2") or card.select_one("h3") or card.select_one("[class*='title']")
                        p = card.select_one("[class*='price']") or card.select_one("[class*='precio']")
                        u = card.select_one("[class*='location']") or card.select_one("[class*='address']") or card.select_one("[class*='lugar']")
                        l = card.select_one("a[href]")
                        img = card.select_one("img")
                        titulo = t.text.strip() if t else ""
                        precio = p.text.strip() if p else ""
                        ubicacion = u.text.strip() if u else "Córdoba"
                        href = l["href"] if l and l.get("href") else ""
                        url_prop = href if href.startswith("http") else "https://clasificados.lavoz.com.ar" + href
                        imagen = (img.get("data-src") or img.get("src","")) if img else ""
                        if titulo or precio:
                            props.append({
                                "titulo": titulo,
                                "precio": precio,
                                "moneda": "ARS",
                                "ubicacion": ubicacion,
                                "url": url_prop,
                                "imagen": imagen,
                                "fuente": "LaVoz",
                                "operacion": op,
                                "atributos": []
                            })
                    except: pass
                print(f"✅ LaVoz {op} p{i}: {len(cards)} cards")
                time.sleep(random.uniform(1.5, 3))
            except Exception as e:
                print(f"❌ LaVoz error: {e}")
    return props


def enviar_whatsapp(numero, mensaje):
    """Envía WhatsApp via Twilio"""
    try:
        import urllib.request, urllib.parse, base64
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
        data = urllib.parse.urlencode({
            "From": TWILIO_WA,
            "To": f"whatsapp:+549{numero}" if not numero.startswith("+") else f"whatsapp:{numero}",
            "Body": mensaje
        }).encode()
        credentials = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Basic {credentials}")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            print(f"✅ WA enviado a {numero}: {result.get('sid','')}")
            return True
    except Exception as e:
        print(f"❌ WA error a {numero}: {e}")
        return False

def chequear_alertas():
    """Chequea usuarios y envía alertas de nuevas propiedades"""
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
                # Buscar propiedades nuevas (últimas 2 horas) que matcheen
                conn2 = get_conn()
                cur2 = conn2.cursor()
                query = """
                    SELECT titulo, precio, moneda, ubicacion, url, fuente 
                    FROM propiedades 
                    WHERE fecha > NOW() - INTERVAL '2 hours'
                """
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
                query += " AND url NOT IN (SELECT propiedad_url FROM alertas_enviadas WHERE usuario_id=%s)"
                params.append(u["id"])
                query += " LIMIT 3"
                cur2.execute(query, params)
                props = cur2.fetchall()

                for prop in props:
                    titulo, precio, moneda, ubicacion, url, fuente = prop
                    precio_str = f"{moneda} {int(precio):,}" if precio and precio.isdigit() else precio or "Consultar"
                    mensaje = f"""🏠 *RodiProp — Nueva propiedad!*

{titulo}
📍 {ubicacion}
💰 {precio_str}
🔗 {url}

_Fuente: {fuente}_
_Buscabas: {u.get('zona','')} · {u.get('tipo','')} · {u.get('operacion','')}_

Para pausar alertas respondé STOP"""

                    if enviar_whatsapp(u["whatsapp"], mensaje):
                        cur2.execute(
                            "INSERT INTO alertas_enviadas (usuario_id, propiedad_url) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (u["id"], url)
                        )
                        conn2.commit()

                cur2.close()
                conn2.close()
            except Exception as e:
                print(f"Error alertas usuario {u['id']}: {e}")

        print(f"✅ Alertas chequeadas para {len(usuarios)} usuarios")
    except Exception as e:
        print(f"Error chequear alertas: {e}")

def run_scraper():
    print("🔍 Scraper v2 iniciando — 4 fuentes...")
    todas = []
    try:
        ml = scrape_ml(5)
        todas.extend(ml)
        print(f"ML: {len(ml)}")
    except Exception as e: print(f"ML fail: {e}")
    try:
        ap = scrape_ap(5)
        todas.extend(ap)
        print(f"AP: {len(ap)}")
    except Exception as e: print(f"AP fail: {e}")
    try:
        zp = scrape_zonaprop(5)
        todas.extend(zp)
        print(f"ZP: {len(zp)}")
    except Exception as e: print(f"ZP fail: {e}")
    try:
        lv = scrape_lavoz(5)
        todas.extend(lv)
        print(f"LaVoz: {len(lv)}")
    except Exception as e: print(f"LaVoz fail: {e}")
    guardar_props(todas)
    print(f"✅ Total en DB: {contar_props()}")

def auto_scraper():
    time.sleep(10)
    while True:
        try:
            run_scraper()
            chequear_alertas()
        except Exception as e:
            print(f"Auto error: {e}")
        time.sleep(7200)

try:
    init_db()
except Exception as e:
    print(f"DB init error: {e}")

threading.Thread(target=auto_scraper, daemon=True).start()

@app.route("/")
def home():
    return jsonify({"status": "RodiProp API OK", "version": "6.0", "total": contar_props()})

@app.route("/api/propiedades")
def propiedades():
    props = cargar_props(
        zona=request.args.get("zona",""),
        tipo=request.args.get("tipo",""),
        operacion=request.args.get("operacion",""),
        fuente=request.args.get("fuente",""),
        limit=request.args.get("limit", 50, type=int)
    )
    return jsonify({"total": len(props), "propiedades": props})

@app.route("/api/stats")
def stats():
    return jsonify(stats_db())

@app.route("/api/scraper/ejecutar", methods=["GET","POST"])
def trigger():
    threading.Thread(target=run_scraper, daemon=True).start()
    return jsonify({"status": "Scraper iniciado — 4 fuentes: ML + AP + ZP + LaVoz"})

@app.route("/api/usuarios/registro", methods=["POST", "OPTIONS"])
def registro():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json()
    if not data:
        return jsonify({"error": "Datos requeridos"}), 400
    email = data.get("email","").strip().lower()
    if not email:
        return jsonify({"error": "Email requerido"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO usuarios (nombre, email, whatsapp, zona, tipo, operacion, precio_min, precio_max)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (email) DO UPDATE SET
                nombre=EXCLUDED.nombre,
                whatsapp=EXCLUDED.whatsapp,
                zona=EXCLUDED.zona,
                tipo=EXCLUDED.tipo,
                operacion=EXCLUDED.operacion,
                precio_min=EXCLUDED.precio_min,
                precio_max=EXCLUDED.precio_max,
                activo=TRUE
        """, (
            data.get("nombre",""),
            email,
            data.get("whatsapp",""),
            data.get("zona",""),
            data.get("tipo",""),
            data.get("operacion","venta"),
            int(data.get("precio_min", 0) or 0),
            int(data.get("precio_max", 999999999) or 999999999),
        ))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "mensaje": "¡Alerta creada! Te avisamos cuando aparezca algo."})
    except Exception as e:
        print(f"Registro error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/usuarios/lista")
def lista_usuarios():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id,nombre,email,whatsapp,zona,tipo,operacion,precio_min,precio_max,plan,fecha FROM usuarios ORDER BY fecha DESC")
        cols = ["id","nombre","email","whatsapp","zona","tipo","operacion","precio_min","precio_max","plan","fecha"]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if r.get("fecha"): r["fecha"] = str(r["fecha"])
        return jsonify({"total": len(rows), "usuarios": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/alertas/test", methods=["GET","POST"])
def test_alerta():
    """Envía alerta de prueba al primer usuario"""
    threading.Thread(target=chequear_alertas, daemon=True).start()
    return jsonify({"status": "Chequeando alertas en background"})

@app.route("/api/usuarios/stats")
def usuarios_stats():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM usuarios WHERE activo=TRUE")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM usuarios WHERE plan='premium'")
        premium = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({"total_usuarios": total, "premium": premium, "gratis": total - premium})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
