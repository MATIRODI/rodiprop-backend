from flask import Flask, jsonify, request, redirect
from flask_cors import CORS
import os, threading, time, requests, random, json, re, urllib.request, urllib.parse, base64
from bs4 import BeautifulSoup
from datetime import datetime

app = Flask(__name__)
CORS(app)

# ─── CONFIG ─────────────────────────────────────────────────────────────────

PG_HOST = os.environ.get("PGHOST", "postgres.railway.internal")
PG_USER = os.environ.get("PGUSER", "postgres")
PG_PASS = os.environ.get("PGPASSWORD", "")
PG_DB   = os.environ.get("PGDATABASE", "railway")
PG_PORT = int(os.environ.get("PGPORT", "5432"))

TWILIO_SID   = os.environ.get("TWILIO_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_TOKEN", "")
TWILIO_WA    = os.environ.get("TWILIO_WA", "whatsapp:+14155238886")

# Mercado Pago — agregar en Railway: MP_ACCESS_TOKEN, MP_CLIENT_SECRET
MP_ACCESS_TOKEN  = os.environ.get("MP_ACCESS_TOKEN", "")
MP_CLIENT_SECRET = os.environ.get("MP_CLIENT_SECRET", "")
BACKEND_URL  = os.environ.get("BACKEND_URL", "https://web-production-88fd4.up.railway.app")
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://rodiprop.gruporodi.com.ar")
MI_WHATSAPP  = os.environ.get("MI_WHATSAPP", "")  # tu número personal para notificaciones

PLANES = {
    "premium": {
        "nombre": "RodiProp Premium",
        "precio": 4999,
        "moneda": "ARS",
        "descripcion": "Alertas ilimitadas por WhatsApp + filtros avanzados",
    },
    "inversor": {
        "nombre": "RodiProp Inversor",
        "precio": 15000,
        "moneda": "ARS",
        "descripcion": "Todo Premium + Tasador de Propiedades + Analytics",
    },
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

# ─── HELPERS ────────────────────────────────────────────────────────────────

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "es-AR,es;q=0.9"}

def limpiar_precio(raw):
    if not raw:
        return ""
    raw = str(raw).split(" - ")[0].split("–")[0].split("-")[0] if len(str(raw)) > 15 else str(raw)
    nums = re.sub(r'[^0-9]', '', raw)
    return nums if nums else ""

def get_conn():
    import pg8000.dbapi as pg
    print("Connecting to " + PG_HOST + ":" + str(PG_PORT) + " db=" + PG_DB)
    return pg.connect(user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT, database=PG_DB)

# ─── DB INIT ────────────────────────────────────────────────────────────────

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
        ambientes TEXT DEFAULT '', cocheras TEXT DEFAULT '',
        activo BOOLEAN DEFAULT TRUE, plan TEXT DEFAULT 'gratis',
        alertas_enviadas_count INTEGER DEFAULT 0,
        fecha TIMESTAMP DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS alertas_enviadas (
        id SERIAL PRIMARY KEY, usuario_id INTEGER, propiedad_url TEXT,
        fecha TIMESTAMP DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS pagos (
        id SERIAL PRIMARY KEY,
        usuario_id INTEGER,
        plan TEXT,
        mp_payment_id TEXT UNIQUE,
        monto NUMERIC DEFAULT 0,
        estado TEXT DEFAULT 'pending',
        tipo TEXT DEFAULT 'checkout',
        fecha TIMESTAMP DEFAULT NOW())""")
    for col_sql in [
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS ambientes TEXT DEFAULT ''",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS cocheras TEXT DEFAULT ''",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS alertas_enviadas_count INTEGER DEFAULT 0",
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS plan_vence TIMESTAMP",
    ]:
        try:
            cur.execute(col_sql)
        except Exception:
            pass
    conn.commit()
    cur.close()
    conn.close()
    print("DB inicializada")

# ─── DB PROPS ───────────────────────────────────────────────────────────────

def guardar_props(props):
    conn = get_conn()
    cur = conn.cursor()
    guardadas = 0
    for i, p in enumerate(props):
        try:
            url = p.get("url", "").strip() or (p.get("fuente", "") + "_" + str(i))
            cur.execute(
                "INSERT INTO propiedades (titulo,precio,moneda,ubicacion,url,imagen,fuente,operacion,atributos)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                " ON CONFLICT (url) DO UPDATE SET precio=EXCLUDED.precio, imagen=EXCLUDED.imagen, fecha=NOW()",
                (p.get("titulo", "")[:500], limpiar_precio(p.get("precio", "")), p.get("moneda", "USD"),
                 p.get("ubicacion", "")[:500], url[:1000], p.get("imagen", "")[:1000],
                 p.get("fuente", ""), p.get("operacion", ""), json.dumps(p.get("atributos", [])))
            )
            guardadas += 1
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    conn.commit()
    cur.close()
    conn.close()
    print(str(guardadas) + "/" + str(len(props)) + " props guardadas")

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
        print("Count error: " + str(e))
        return 0

def cargar_props(zona="", tipo="", operacion="", fuente="", limit=50):
    try:
        conn = get_conn()
        cur = conn.cursor()
        query = "SELECT titulo,precio,moneda,ubicacion,url,imagen,fuente,operacion,atributos FROM propiedades WHERE 1=1"
        params = []
        if zona:
            query += " AND (LOWER(ubicacion) LIKE %s OR LOWER(titulo) LIKE %s)"
            params += ["%" + zona.lower() + "%", "%" + zona.lower() + "%"]
        if tipo:
            query += " AND LOWER(titulo) LIKE %s"
            params.append("%" + tipo.lower() + "%")
        if operacion:
            query += " AND LOWER(operacion) = %s"
            params.append(operacion.lower())
        if fuente:
            query += " AND LOWER(fuente) LIKE %s"
            params.append("%" + fuente.lower() + "%")
        query += " ORDER BY fecha DESC LIMIT %s"
        params.append(limit)
        cur.execute(query, params)
        cols = ["titulo", "precio", "moneda", "ubicacion", "url", "imagen", "fuente", "operacion", "atributos"]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if isinstance(r.get("atributos"), str):
                try:
                    r["atributos"] = json.loads(r["atributos"])
                except Exception:
                    r["atributos"] = []
        return rows
    except Exception as e:
        print("Load error: " + str(e))
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

# ─── WHATSAPP ────────────────────────────────────────────────────────────────

def enviar_whatsapp(numero, mensaje):
    try:
        url = "https://api.twilio.com/2010-04-01/Accounts/" + TWILIO_SID + "/Messages.json"
        to = "whatsapp:+549" + numero if not numero.startswith("+") else "whatsapp:" + numero
        data = urllib.parse.urlencode({"From": TWILIO_WA, "To": to, "Body": mensaje}).encode()
        credentials = base64.b64encode((TWILIO_SID + ":" + TWILIO_TOKEN).encode()).decode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", "Basic " + credentials)
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
        print("WA enviado a " + str(numero) + ": " + str(result.get("sid", "")))
        return True
    except Exception as e:
        print("WA error: " + str(e))
        return False

# ─── MERCADO PAGO ────────────────────────────────────────────────────────────

def mp_request(method, endpoint, data=None):
    url = "https://api.mercadopago.com" + endpoint
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", "Bearer " + MP_ACCESS_TOKEN)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())

def registrar_pago_db(usuario_id, plan, mp_payment_id, monto, estado, tipo="checkout"):
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pagos (usuario_id, plan, mp_payment_id, monto, estado, tipo)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (mp_payment_id) DO UPDATE SET estado=EXCLUDED.estado",
            (usuario_id, plan, str(mp_payment_id), monto, estado, tipo)
        )
        if estado == "approved":
            cur.execute(
                "UPDATE usuarios SET plan=%s, activo=TRUE WHERE id=%s",
                (plan, usuario_id)
            )
        conn.commit()
        cur.close()
    finally:
        if conn:
            conn.close()

def notificar_pago_whatsapp(nombre, email, plan, monto):
    if not MI_WHATSAPP:
        return
    msg = (
        "💰 *Nuevo pago en RodiProp!*\n\n"
        "👤 " + str(nombre) + " (" + str(email) + ")\n"
        "📦 Plan: " + plan.upper() + "\n"
        "💵 Monto: ARS " + str(int(monto)) + "\n"
        "✅ Plan activado automáticamente"
    )
    enviar_whatsapp(MI_WHATSAPP, msg)

# ─── SCRAPERS ────────────────────────────────────────────────────────────────

LOCALIDADES_CORDOBA = [
    "cordoba",
    "villa-allende", "unquillo", "salsipuedes", "mendiolaza", "la-calera",
    "rio-ceballos", "malagueno", "villa-carlos-paz", "cosquin", "alta-gracia",
    "jesus-maria", "colonia-caroya",
    "rio-cuarto", "villa-maria", "san-francisco", "rio-tercero", "bell-ville",
    "marcos-juarez", "morteros", "arroyito",
    "la-falda", "la-cumbre", "capilla-del-monte", "mina-clavero",
    "villa-general-belgrano", "potrero-de-garay",
]

def scrape_ml(paginas=3):
    props = []
    for op in ["venta", "alquiler"]:
        for localidad in LOCALIDADES_CORDOBA:
            for i in range(paginas):
                try:
                    url = ("https://inmuebles.mercadolibre.com.ar/" + op + "/" + localidad
                           + "/_Desde_" + str(i * 48 + 1) + "_DisplayType_G")
                    r = requests.get(url, headers=get_headers(), timeout=15)
                    soup = BeautifulSoup(r.text, "html.parser")
                    for card in soup.select(".ui-search-layout__item"):
                        try:
                            t   = card.select_one(".poly-component__title")
                            p   = card.select_one(".andes-money-amount__fraction")
                            m   = card.select_one(".andes-money-amount__currency-symbol")
                            u   = card.select_one(".poly-component__location")
                            l   = card.select_one("a.poly-component__title")
                            img = card.select_one("img.poly-component__picture")
                            attrs = card.select(".poly-attributes-list__item")
                            if t and p:
                                props.append({
                                    "titulo": t.text.strip(),
                                    "precio": p.text.strip().replace(".", "").replace(",", ""),
                                    "moneda": m.text.strip() if m else "USD",
                                    "ubicacion": u.text.strip() if u else "",
                                    "url": l["href"] if l else "",
                                    "imagen": (img.get("data-src") or img.get("src", "")) if img else "",
                                    "fuente": "MercadoLibre",
                                    "operacion": op,
                                    "atributos": [a.text.strip() for a in attrs],
                                })
                        except Exception:
                            pass
                    print("ML " + op + "/" + localidad + " p" + str(i + 1) + ": " + str(len(props)))
                    time.sleep(random.uniform(1, 2))
                except Exception as e:
                    print("ML " + localidad + " error: " + str(e))
    return props

def scrape_ap(paginas=5):
    props = []
    s = requests.Session()
    try:
        s.get("https://www.argenprop.com", headers=get_headers(), timeout=10)
    except Exception:
        pass
    for op in ["venta", "alquiler"]:
        for i in range(1, paginas + 1):
            try:
                url = ("https://www.argenprop.com/propiedades-en-" + op
                       + "-en-provincia-cordoba--pagina-" + str(i))
                r = s.get(url, headers=get_headers(), timeout=15)
                if r.status_code in [403, 429]:
                    break
                soup = BeautifulSoup(r.text, "html.parser")
                cards = soup.select(".listing__item") or soup.select("article")
                for card in cards:
                    try:
                        t   = card.select_one(".card__title") or card.select_one("h2")
                        p   = card.select_one(".card__price") or card.select_one("[class*='price']")
                        u   = card.select_one(".card__address") or card.select_one("[class*='address']")
                        l   = card.select_one("a[href]")
                        img = card.select_one("img")
                        titulo    = t.text.strip() if t else ""
                        precio_raw = p.text.strip() if p else ""
                        precio    = precio_raw.split(" - ")[0].split("–")[0].strip() if precio_raw else ""
                        href      = l["href"] if l else ""
                        url_prop  = href if href.startswith("http") else "https://www.argenprop.com" + href
                        if titulo or precio:
                            props.append({
                                "titulo": titulo, "precio": precio, "moneda": "USD",
                                "ubicacion": u.text.strip() if u else "Córdoba",
                                "url": url_prop,
                                "imagen": (img.get("data-src") or img.get("src", "")) if img else "",
                                "fuente": "ArgenProp", "operacion": op, "atributos": [],
                            })
                    except Exception:
                        pass
                print("AP " + op + " p" + str(i) + ": " + str(len(cards)))
                time.sleep(random.uniform(1.5, 3))
            except Exception as e:
                print("AP error: " + str(e))
    return props

def scrape_lavoz(paginas=5):
    props = []
    s = requests.Session()
    try:
        s.get("https://clasificados.lavoz.com.ar", headers=get_headers(), timeout=10)
    except Exception:
        pass
    for op, slug in [("venta", "venta"), ("alquiler", "alquiler")]:
        for i in range(1, paginas + 1):
            try:
                url = "https://clasificados.lavoz.com.ar/inmuebles/" + slug + "?page=" + str(i)
                r = s.get(url, headers=get_headers(), timeout=20)
                if r.status_code in [403, 429]:
                    break
                soup = BeautifulSoup(r.text, "html.parser")
                cards = soup.select(".aviso") or soup.select("article") or soup.select(".card")
                for card in cards:
                    try:
                        t   = card.select_one("h2") or card.select_one("h3") or card.select_one("[class*='title']")
                        p   = card.select_one("[class*='price']") or card.select_one("[class*='precio']")
                        u   = card.select_one("[class*='location']") or card.select_one("[class*='address']")
                        l   = card.select_one("a[href]")
                        img = card.select_one("img")
                        titulo = t.text.strip() if t else ""
                        precio = p.text.strip() if p else ""
                        href   = l["href"] if l and l.get("href") else ""
                        url_prop = href if href.startswith("http") else "https://clasificados.lavoz.com.ar" + href
                        ubicacion_raw = u.text.strip() if u else ""
                        if not ubicacion_raw and href:
                            partes = href.split("/")
                            if len(partes) > 3:
                                ubicacion_raw = partes[-1].replace("-", " ").title()
                        if not ubicacion_raw:
                            ubicacion_raw = "Córdoba"
                        if titulo or precio:
                            props.append({
                                "titulo": titulo, "precio": precio, "moneda": "USD",
                                "ubicacion": ubicacion_raw, "url": url_prop,
                                "imagen": (img.get("data-src") or img.get("src", "")) if img else "",
                                "fuente": "LaVoz", "operacion": op, "atributos": [],
                            })
                    except Exception:
                        pass
                print("LaVoz " + op + " p" + str(i) + ": " + str(len(cards)))
                time.sleep(random.uniform(1.5, 3))
            except Exception as e:
                print("LaVoz error: " + str(e))
    return props

# ─── ALERTAS ─────────────────────────────────────────────────────────────────

def chequear_alertas():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id,nombre,email,whatsapp,zona,tipo,operacion,"
            "precio_min,precio_max,ambientes,cocheras,plan,"
            "COALESCE(alertas_enviadas_count,0) as alertas_count"
            " FROM usuarios WHERE activo=TRUE AND whatsapp != ''"
            " AND (plan='premium' OR plan='inversor' OR COALESCE(alertas_enviadas_count,0) < 7)"
        )
        cols = ["id", "nombre", "email", "whatsapp", "zona", "tipo", "operacion",
                "precio_min", "precio_max", "ambientes", "cocheras", "plan", "alertas_count"]
        usuarios = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for u in usuarios:
            try:
                conn2 = get_conn()
                cur2 = conn2.cursor()
                query = ("SELECT titulo,precio,moneda,ubicacion,url,fuente FROM propiedades"
                         " WHERE fecha > NOW() - INTERVAL '3 hours'")
                params = []
                if u.get("zona"):
                    query += " AND (LOWER(ubicacion) LIKE %s OR LOWER(titulo) LIKE %s)"
                    params += ["%" + u["zona"].lower() + "%", "%" + u["zona"].lower() + "%"]
                if u.get("tipo"):
                    query += " AND LOWER(titulo) LIKE %s"
                    params.append("%" + u["tipo"].lower() + "%")
                if u.get("operacion"):
                    query += " AND LOWER(operacion) = %s"
                    params.append(u["operacion"].lower())
                if u.get("precio_min") and int(u["precio_min"]) > 0:
                    query += " AND CAST(NULLIF(precio, '') AS BIGINT) >= %s"
                    params.append(int(u["precio_min"]))
                if u.get("precio_max") and int(u["precio_max"]) < 999999999:
                    query += " AND CAST(NULLIF(precio, '') AS BIGINT) <= %s"
                    params.append(int(u["precio_max"]))
                if u.get("ambientes"):
                    query += " AND (LOWER(titulo) LIKE %s OR LOWER(titulo) LIKE %s)"
                    params += ["%" + u["ambientes"] + " amb%", "%" + u["ambientes"] + " dorm%"]
                query += " AND url NOT IN (SELECT propiedad_url FROM alertas_enviadas WHERE usuario_id=%s) LIMIT 3"
                params.append(u["id"])
                cur2.execute(query, params)
                for prop in cur2.fetchall():
                    titulo, precio, moneda, ubicacion, url, fuente = prop
                    precio_str = (moneda + " " + "{:,}".format(int(precio))
                                  if precio and precio.isdigit() else precio or "Consultar")
                    mensaje = (
                        "🏠 *RodiProp — Nueva propiedad!*\n\n"
                        + titulo + "\n"
                        "📍 " + ubicacion + "\n"
                        "💰 " + precio_str + "\n"
                        "🔗 " + url + "\n\n"
                        "_Fuente: " + fuente + "_\n"
                        "Para pausar respondé STOP"
                    )
                    if enviar_whatsapp(u["whatsapp"], mensaje):
                        cur2.execute(
                            "INSERT INTO alertas_enviadas (usuario_id, propiedad_url)"
                            " VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (u["id"], url)
                        )
                        cur2.execute(
                            "UPDATE usuarios SET alertas_enviadas_count = COALESCE(alertas_enviadas_count,0) + 1"
                            " WHERE id=%s",
                            (u["id"],)
                        )
                conn2.commit()
                cur2.close()
                conn2.close()
            except Exception as e:
                print("Error usuario " + str(u["id"]) + ": " + str(e))
        print("Alertas chequeadas: " + str(len(usuarios)) + " usuarios")
    except Exception as e:
        print("Error alertas: " + str(e))

# ─── SCRAPER LOOP ────────────────────────────────────────────────────────────

def run_scraper():
    print("Scraper iniciando...")
    todas = []
    for fn, name in [(scrape_ml, "ML"), (scrape_ap, "AP"), (scrape_lavoz, "LaVoz")]:
        try:
            r = fn(5)
            todas.extend(r)
            print(name + ": " + str(len(r)))
        except Exception as e:
            print(name + " fail: " + str(e))
    guardar_props(todas)
    print("Total DB: " + str(contar_props()))

def auto_scraper():
    time.sleep(10)
    while True:
        try:
            run_scraper()
            chequear_alertas()
        except Exception as e:
            print("Auto error: " + str(e))
        time.sleep(7200)

# ─── ENDPOINTS PROPIEDADES ───────────────────────────────────────────────────

@app.route("/")
def home():
    return jsonify({
        "status": "RodiProp API OK",
        "version": "8.1",
        "pg_host": PG_HOST,
        "total": contar_props(),
        "mp": "activo" if MP_ACCESS_TOKEN else "pendiente-credenciales",
    })

@app.route("/api/propiedades")
def propiedades():
    props = cargar_props(
        zona=request.args.get("zona", ""),
        tipo=request.args.get("tipo", ""),
        operacion=request.args.get("operacion", ""),
        fuente=request.args.get("fuente", ""),
        limit=request.args.get("limit", 50, type=int),
    )
    return jsonify({"total": len(props), "propiedades": props})

@app.route("/api/stats")
def stats():
    return jsonify(stats_db())

@app.route("/api/scraper/ejecutar", methods=["GET", "POST"])
def trigger():
    threading.Thread(target=run_scraper, daemon=True).start()
    return jsonify({"status": "Scraper iniciado"})

@app.route("/api/alertas/test", methods=["GET", "POST"])
def test_alerta():
    threading.Thread(target=chequear_alertas, daemon=True).start()
    return jsonify({"status": "Chequeando alertas en background"})

# ─── ENDPOINTS USUARIOS ──────────────────────────────────────────────────────

@app.route("/api/usuarios/registro", methods=["POST", "OPTIONS"])
def registro():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json()
    if not data:
        return jsonify({"error": "Datos requeridos"}), 400
    email = data.get("email", "").strip().lower()
    if not email:
        return jsonify({"error": "Email requerido"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO usuarios (nombre,email,whatsapp,zona,tipo,operacion,precio_min,precio_max,ambientes,cocheras)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            " ON CONFLICT (email) DO UPDATE SET nombre=EXCLUDED.nombre, whatsapp=EXCLUDED.whatsapp,"
            " zona=EXCLUDED.zona, tipo=EXCLUDED.tipo, operacion=EXCLUDED.operacion,"
            " precio_min=EXCLUDED.precio_min, precio_max=EXCLUDED.precio_max,"
            " ambientes=EXCLUDED.ambientes, cocheras=EXCLUDED.cocheras, activo=TRUE",
            (data.get("nombre", ""), email, data.get("whatsapp", ""), data.get("zona", ""),
             data.get("tipo", ""), data.get("operacion", "venta"),
             int(data.get("precio_min", 0) or 0), int(data.get("precio_max", 999999999) or 999999999),
             data.get("ambientes", ""), data.get("cocheras", ""))
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "mensaje": "Alerta creada!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/usuarios/lista")
def lista_usuarios():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id,nombre,email,whatsapp,zona,tipo,operacion,plan,alertas_enviadas_count,fecha"
            " FROM usuarios ORDER BY fecha DESC"
        )
        cols = ["id", "nombre", "email", "whatsapp", "zona", "tipo", "operacion",
                "plan", "alertas_enviadas_count", "fecha"]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if r.get("fecha"):
                r["fecha"] = str(r["fecha"])
        return jsonify({"total": len(rows), "usuarios": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/usuarios/stats")
def usuarios_stats():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM usuarios WHERE activo=TRUE")
        total = cur.fetchone()[0]
        cur.execute("SELECT plan, COUNT(*) FROM usuarios WHERE activo=TRUE GROUP BY plan")
        por_plan = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return jsonify({"total_usuarios": total, "por_plan": por_plan})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── ENDPOINTS MERCADO PAGO ──────────────────────────────────────────────────

@app.route("/api/pagos/crear", methods=["POST", "OPTIONS"])
def crear_pago():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json()
    if not data:
        return jsonify({"error": "Datos requeridos"}), 400
    email    = data.get("email", "").strip().lower()
    plan_key = data.get("plan", "premium").strip().lower()
    tipo     = data.get("tipo", "checkout")  # "checkout" o "suscripcion"
    if not email:
        return jsonify({"error": "Email requerido"}), 400
    if plan_key not in PLANES:
        return jsonify({"error": "Plan invalido. Opciones: premium, inversor"}), 400
    if not MP_ACCESS_TOKEN:
        return jsonify({"error": "Mercado Pago no configurado aun (falta MP_ACCESS_TOKEN)"}), 503
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT id, nombre FROM usuarios WHERE email=%s", (email,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"error": "Usuario no encontrado. Registrate primero en /api/usuarios/registro"}), 404
        usuario_id, nombre = row[0], row[1]
    except Exception as e:
        return jsonify({"error": "DB error: " + str(e)}), 500
    try:
        plan_info = PLANES[plan_key]
        if tipo == "suscripcion":
            payload = {
                "reason": plan_info["nombre"],
                "auto_recurring": {
                    "frequency": 1,
                    "frequency_type": "months",
                    "transaction_amount": plan_info["precio"],
                    "currency_id": plan_info["moneda"],
                },
                "payer_email": email,
                "back_url": FRONTEND_URL + "/pago-exitoso?plan=" + plan_key,
                "notification_url": BACKEND_URL + "/api/pagos/webhook",
                "external_reference": str(usuario_id) + "_" + plan_key,
                "status": "pending",
            }
            result = mp_request("POST", "/preapproval", payload)
            return jsonify({
                "tipo": "suscripcion",
                "init_point": result.get("init_point"),
                "preapproval_id": result.get("id"),
            })
        else:
            payload = {
                "items": [{
                    "title": plan_info["nombre"],
                    "description": plan_info["descripcion"],
                    "quantity": 1,
                    "unit_price": plan_info["precio"],
                    "currency_id": plan_info["moneda"],
                }],
                "payer": {"email": email},
                "back_urls": {
                    "success": BACKEND_URL + "/api/pagos/exito?usuario_id=" + str(usuario_id) + "&plan=" + plan_key,
                    "failure": FRONTEND_URL + "/pago-fallido",
                    "pending": FRONTEND_URL + "/pago-pendiente",
                },
                "auto_return": "approved",
                "notification_url": BACKEND_URL + "/api/pagos/webhook",
                "external_reference": str(usuario_id) + "_" + plan_key,
                "statement_descriptor": "RODIPROP",
            }
            result = mp_request("POST", "/checkout/preferences", payload)
            return jsonify({
                "tipo": "checkout",
                "init_point": result.get("init_point"),
                "sandbox_init_point": result.get("sandbox_init_point"),
                "preference_id": result.get("id"),
            })
    except Exception as e:
        return jsonify({"error": "MP error: " + str(e)}), 500

@app.route("/api/pagos/exito")
def pago_exito():
    usuario_id = request.args.get("usuario_id")
    plan       = request.args.get("plan", "premium")
    payment_id = request.args.get("payment_id", "redirect_" + str(int(time.time())))
    status     = request.args.get("status", "approved")
    if status == "approved" and usuario_id:
        try:
            registrar_pago_db(
                usuario_id, plan, payment_id,
                PLANES.get(plan, {}).get("precio", 0), "approved"
            )
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("SELECT nombre, email FROM usuarios WHERE id=%s", (int(usuario_id),))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                notificar_pago_whatsapp(row[0], row[1], plan, PLANES.get(plan, {}).get("precio", 0))
        except Exception as e:
            print("Exito handler error: " + str(e))
    return redirect(FRONTEND_URL + "/pago-exitoso?plan=" + plan)

@app.route("/api/pagos/fallo")
def pago_fallo():
    return redirect(FRONTEND_URL + "/pago-fallido")

@app.route("/api/pagos/pendiente")
def pago_pendiente():
    return redirect(FRONTEND_URL + "/pago-pendiente")

@app.route("/api/pagos/webhook", methods=["POST"])
def webhook_mp():
    data    = request.get_json(silent=True) or {}
    topic   = data.get("type") or request.args.get("topic", "")
    obj_id  = data.get("data", {}).get("id") or request.args.get("id", "")
    if not obj_id:
        return jsonify({"status": "ignored"}), 200
    try:
        if topic in ("payment", ""):
            payment     = mp_request("GET", "/v1/payments/" + str(obj_id))
            estado      = payment.get("status", "")
            monto       = payment.get("transaction_amount", 0)
            ext_ref     = payment.get("external_reference", "")
            partes      = ext_ref.split("_") if ext_ref else []
            usuario_id  = partes[0] if partes else None
            plan        = partes[1] if len(partes) > 1 else "premium"
            if usuario_id:
                registrar_pago_db(usuario_id, plan, str(obj_id), monto, estado)
                if estado == "approved":
                    conn = get_conn()
                    cur  = conn.cursor()
                    cur.execute("SELECT nombre, email FROM usuarios WHERE id=%s", (int(usuario_id),))
                    row = cur.fetchone()
                    cur.close()
                    conn.close()
                    if row:
                        notificar_pago_whatsapp(row[0], row[1], plan, monto)
        elif topic in ("subscription_preapproval", "preapproval"):
            sub         = mp_request("GET", "/preapproval/" + str(obj_id))
            estado      = sub.get("status", "")
            ext_ref     = sub.get("external_reference", "")
            partes      = ext_ref.split("_") if ext_ref else []
            usuario_id  = partes[0] if partes else None
            plan        = partes[1] if len(partes) > 1 else "premium"
            if usuario_id and estado == "authorized":
                registrar_pago_db(
                    usuario_id, plan, str(obj_id),
                    PLANES.get(plan, {}).get("precio", 0), "approved", "suscripcion"
                )
                conn = get_conn()
                cur  = conn.cursor()
                cur.execute("SELECT nombre, email FROM usuarios WHERE id=%s", (int(usuario_id),))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row:
                    notificar_pago_whatsapp(row[0], row[1], plan, PLANES.get(plan, {}).get("precio", 0))
    except Exception as e:
        print("Webhook error: " + str(e))
    return jsonify({"status": "ok"}), 200

@app.route("/api/pagos/lista")
def lista_pagos():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT p.id, u.nombre, u.email, p.plan, p.monto, p.estado, p.tipo, p.fecha"
            " FROM pagos p LEFT JOIN usuarios u ON p.usuario_id=u.id"
            " ORDER BY p.fecha DESC LIMIT 50"
        )
        cols = ["id", "nombre", "email", "plan", "monto", "estado", "tipo", "fecha"]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if r.get("fecha"):
                r["fecha"] = str(r["fecha"])
            if r.get("monto") is not None:
                r["monto"] = float(r["monto"])
        return jsonify({"total": len(rows), "pagos": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── OTROS ───────────────────────────────────────────────────────────────────

@app.route("/api/db/fix-moneda", methods=["GET", "POST"])
def fix_moneda():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE propiedades SET moneda='USD' WHERE fuente='LaVoz' AND moneda='ARS'")
        updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "actualizadas": updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── INIT ─────────────────────────────────────────────────────────────────────

try:
    init_db()
except Exception as e:
    print("DB init error: " + str(e))

threading.Thread(target=auto_scraper, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
