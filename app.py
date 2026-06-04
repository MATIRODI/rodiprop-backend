from flask import Flask, jsonify, request, redirect
from flask_cors import CORS
from functools import wraps
import os, threading, time, requests, random, json, re, urllib.request, urllib.parse, base64
import hmac, hashlib, secrets
from bs4 import BeautifulSoup
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

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
WA_SERVICE_URL = os.environ.get("WA_SERVICE_URL", "https://grateful-unity-production-1f47.up.railway.app")

# Mercado Pago — agregar en Railway: MP_ACCESS_TOKEN, MP_CLIENT_SECRET
MP_ACCESS_TOKEN  = os.environ.get("MP_ACCESS_TOKEN", "")
MP_CLIENT_SECRET = os.environ.get("MP_CLIENT_SECRET", "")
BACKEND_URL  = os.environ.get("BACKEND_URL", "https://web-production-88fd4.up.railway.app")
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://rodiprop.gruporodi.com.ar")
MI_WHATSAPP  = os.environ.get("MI_WHATSAPP", "")
JWT_SECRET   = os.environ.get("JWT_SECRET", "f41ed397354d4e63f0161af7de29fee29c6a32d85c08cbeeefab6dfca87ee4a1")
ADMIN_EMAIL  = os.environ.get("ADMIN_EMAIL", "matias@gruporodi.com.ar")

PLANES = {
    "premium": {
        "nombre": "RodiProp Premium",
        "precio": 4999,
        "moneda": "ARS",
        "descripcion": "Alertas ilimitadas por WhatsApp + portal privado",
    },
    "inversor": {
        "nombre": "RodiProp Inversor",
        "precio": 15000,
        "moneda": "ARS",
        "descripcion": "Todo Premium + analytics de mercado + créditos",
    },
    "analitic": {
        "nombre": "RodiProp Analítics",
        "precio": 25000,
        "moneda": "ARS",
        "descripcion": "Zonas en auge, tendencia de precios, cotizaciones en tiempo real",
    },
}

ANALYTICS_PLANS = {"inversor", "analitic"}

CREDITOS_HIPOTECARIOS = [
    {"banco": "Banco de la Nación Argentina", "sigla": "BNA", "producto": "Crédito UVA Tu Casa",
     "tasa": 3.5, "tipo_tasa": "UVA + 3.5% TNA", "plazo_max": 30, "financiacion_max": 80,
     "descripcion": "Vivienda única, familiar y de ocupación permanente.", "color": "#004A87"},
    {"banco": "Banco de Córdoba", "sigla": "BANCOR", "producto": "Hipotecario UVA",
     "tasa": 4.0, "tipo_tasa": "UVA + 4.0% TNA", "plazo_max": 20, "financiacion_max": 75,
     "descripcion": "Exclusivo residentes de Córdoba. Primera y segunda vivienda.", "color": "#C8102E"},
    {"banco": "Banco Santander", "sigla": "SAN", "producto": "Hipotecario UVA",
     "tasa": 4.5, "tipo_tasa": "UVA + 4.5% TNA", "plazo_max": 30, "financiacion_max": 80,
     "descripcion": "Para clientes y no clientes. Vivienda única y permanente.", "color": "#EC0000"},
    {"banco": "Banco Galicia", "sigla": "GCE", "producto": "Crédito Hipotecario UVA",
     "tasa": 5.0, "tipo_tasa": "UVA + 5.0% TNA", "plazo_max": 25, "financiacion_max": 75,
     "descripcion": "Clientes con cuenta sueldo. Trámite 100% digital.", "color": "#CC0000"},
    {"banco": "BBVA Argentina", "sigla": "BBVA", "producto": "Préstamo Hipotecario UVA",
     "tasa": 4.8, "tipo_tasa": "UVA + 4.8% TNA", "plazo_max": 30, "financiacion_max": 80,
     "descripcion": "Disponible para clientes y no clientes del banco.", "color": "#004B93"},
    {"banco": "Banco Macro", "sigla": "BMA", "producto": "Crédito Hipotecario UVA",
     "tasa": 5.2, "tipo_tasa": "UVA + 5.2% TNA", "plazo_max": 25, "financiacion_max": 70,
     "descripcion": "Personas físicas con relación de dependencia.", "color": "#F5A623"},
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

# ─── SCRAPLING HTTP WRAPPER ─────────────────────────────────────────────────
# Scrapling usa curl_cffi para imitar el TLS fingerprint de Chrome real,
# eludiendo la mayoría de los anti-bot sin necesitar un navegador headless.
# Fallback transparente a requests si no está instalado.

try:
    from scrapling.fetchers import Fetcher as _SF, FetcherSession as _SFS
    _SCRAPLING = True
    print("Scrapling OK — fingerprint anti-bot activo")
except Exception:
    _SCRAPLING = False
    print("Scrapling no disponible — usando requests")

def _http_get(url, sess=None, timeout=15):
    """GET con TLS fingerprint (Scrapling) o requests como fallback. Retorna (html, status)."""
    if _SCRAPLING:
        try:
            if sess and getattr(sess, '_is_scrapling', False):
                resp = sess.get(url, stealthy_headers=True, timeout=timeout)
            else:
                resp = _SF.get(url, stealthy_headers=True, timeout=timeout)
            return resp.html_content, getattr(resp, 'status', 200)
        except Exception as e:
            print("Scrapling get error: " + str(e))
    h = get_headers()
    if sess and not getattr(sess, '_is_scrapling', False):
        r = sess.get(url, headers=h, timeout=timeout)
    else:
        r = requests.get(url, headers=h, timeout=timeout)
    return r.text, r.status_code

def _http_session():
    """Sesión HTTP reutilizable (Scrapling FetcherSession o requests.Session)."""
    if _SCRAPLING:
        try:
            s = _SFS(stealthy_headers=True)
            s._is_scrapling = True
            return s
        except Exception:
            pass
    return requests.Session()

# ─── HELPERS ────────────────────────────────────────────────────────────────

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "es-AR,es;q=0.9"}

def limpiar_precio(raw):
    if not raw:
        return ""
    raw = str(raw).split(" - ")[0].split("–")[0].split("-")[0] if len(str(raw)) > 15 else str(raw)
    nums = re.sub(r'[^0-9]', '', raw)
    return nums if nums else ""

def detectar_moneda(texto):
    """Detecta si el precio está en USD o ARS según el texto del precio."""
    t = str(texto).upper()
    if any(k in t for k in ["US$", "USD", "U$S", "DOLAR", "DÓLAR"]):
        return "USD"
    return "ARS"

def get_imagen(img, card=None):
    """Extrae la URL de imagen probando múltiples atributos de lazy loading y srcset."""
    # picture > source[srcset] (e.g. ArgenProp, ZonaProp)
    if card:
        src_el = card.select_one("picture > source[srcset]") or card.select_one("source[srcset]")
        if src_el:
            srcset = src_el.get("srcset", "").strip()
            if srcset:
                first = srcset.split(",")[0].strip().split()[0]
                if first and not first.startswith("data:"):
                    return first
    if not img:
        return ""
    # srcset on img element
    srcset = img.get("srcset", "").strip()
    if srcset:
        first = srcset.split(",")[0].strip().split()[0]
        if first and not first.startswith("data:") and first not in ("", "about:blank", "#"):
            return first
    for attr in ["data-src", "data-lazy-src", "data-original", "data-lazy", "data-image", "src"]:
        val = img.get(attr, "").strip()
        if val and not val.startswith("data:") and val not in ("", "about:blank", "#"):
            return val
    return ""

def get_conn():
    import pg8000.dbapi as pg
    print("Connecting to " + PG_HOST + ":" + str(PG_PORT) + " db=" + PG_DB)
    return pg.connect(user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT, database=PG_DB)

# ─── AUTH ────────────────────────────────────────────────────────────────────

def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = (request.headers.get("X-Admin-Key") or
               request.args.get("key", "") or
               (request.get_json(silent=True) or {}).get("key", ""))
        admin_pwd = os.environ.get("ADMIN_PASSWORD", "")
        if not admin_pwd or key != admin_pwd:
            return jsonify({"error": "No autorizado"}), 401
        return f(*args, **kwargs)
    return decorated

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
        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS password_hash TEXT",
    ]:
        try:
            cur.execute(col_sql)
        except Exception:
            pass
    # Corrección de datos históricos: LaVoz publica en ARS, no USD
    try:
        cur.execute("UPDATE propiedades SET moneda='ARS' WHERE fuente='LaVoz' AND moneda='USD'")
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
        url = WA_SERVICE_URL + "/send"
        numero_limpio = numero.replace("+549", "").replace("+54", "").replace("+", "")
        data = json.dumps({"numero": numero_limpio, "mensaje": mensaje}).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
        print("WA enviado a " + str(numero) + ": " + str(result))
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
            from datetime import timedelta
            cur.execute(
                "UPDATE usuarios SET plan=%s, activo=TRUE,"
                " plan_vence=NOW() + INTERVAL '31 days' WHERE id=%s",
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
                    html, status = _http_get(url, timeout=15)
                    soup = BeautifulSoup(html, "html.parser")
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
                                    "imagen": get_imagen(img, card),
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

def scrape_ap(paginas=20):
    props = []
    s = _http_session()
    try:
        _http_get("https://www.argenprop.com", sess=s, timeout=10)
    except Exception:
        pass

    AP_LOCALIDADES = [
        ("cordoba", "cordoba-capital"),
        ("villa-allende", "villa-allende"),
        ("unquillo", "unquillo"),
        ("salsipuedes", "salsipuedes"),
        ("mendiolaza", "mendiolaza"),
        ("la-calera", "la-calera"),
        ("rio-ceballos", "rio-ceballos"),
        ("villa-carlos-paz", "villa-carlos-paz"),
        ("cosquin", "cosquin"),
        ("alta-gracia", "alta-gracia"),
        ("jesus-maria", "jesus-maria"),
        ("colonia-caroya", "colonia-caroya"),
        ("rio-cuarto", "rio-cuarto"),
        ("villa-maria", "villa-maria"),
        ("malagueño", "malagueño"),
    ]

    def parsear_cards(cards, op):
        resultado = []
        for card in cards:
            try:
                t   = card.select_one(".card__title") or card.select_one("h2") or card.select_one("h3")
                p   = card.select_one(".card__price") or card.select_one("[class*='price']")
                u   = card.select_one(".card__address") or card.select_one("[class*='address']") or card.select_one("[class*='location']")
                l   = card.select_one("a[href]")
                img = card.select_one("img")
                titulo     = t.text.strip() if t else ""
                precio_raw = p.text.strip() if p else ""
                precio     = precio_raw.split(" - ")[0].split("–")[0].strip() if precio_raw else ""
                moneda     = detectar_moneda(precio_raw)
                href       = l["href"] if l else ""
                url_prop   = href if href.startswith("http") else "https://www.argenprop.com" + href
                if titulo or precio:
                    resultado.append({
                        "titulo": titulo, "precio": precio, "moneda": moneda,
                        "ubicacion": u.text.strip() if u else "Córdoba",
                        "url": url_prop,
                        "imagen": get_imagen(img, card),
                        "fuente": "ArgenProp", "operacion": op, "atributos": [],
                    })
            except Exception:
                pass
        return resultado

    for op in ["venta", "alquiler"]:
        for loc_nombre, loc_slug in AP_LOCALIDADES:
            for i in range(1, paginas + 1):
                try:
                    url = ("https://www.argenprop.com/propiedades-en-" + op
                           + "-en-" + loc_slug + "--pagina-" + str(i))
                    html, status = _http_get(url, sess=s, timeout=15)
                    if status in [403, 429]:
                        time.sleep(random.uniform(5, 10))
                        break
                    soup = BeautifulSoup(html, "html.parser")
                    cards = soup.select(".listing__item") or soup.select("article.card")
                    if not cards:
                        break
                    nuevas = parsear_cards(cards, op)
                    props.extend(nuevas)
                    print("AP " + op + "/" + loc_nombre + " p" + str(i) + ": " + str(len(nuevas)))
                    if len(cards) < 10:
                        break
                    time.sleep(random.uniform(1.5, 2.5))
                except Exception as e:
                    print("AP " + loc_nombre + " error: " + str(e))
                    break

    for op in ["venta", "alquiler"]:
        for i in range(1, 30):
            try:
                url = ("https://www.argenprop.com/propiedades-en-" + op
                       + "-en-provincia-cordoba--pagina-" + str(i))
                html, status = _http_get(url, sess=s, timeout=15)
                if status in [403, 429]:
                    break
                soup = BeautifulSoup(html, "html.parser")
                cards = soup.select(".listing__item") or soup.select("article.card")
                if not cards:
                    break
                nuevas = parsear_cards(cards, op)
                props.extend(nuevas)
                print("AP-prov " + op + " p" + str(i) + ": " + str(len(nuevas)))
                if len(cards) < 10:
                    break
                time.sleep(random.uniform(2, 3))
            except Exception as e:
                print("AP-prov error: " + str(e))
                break

    return props

def scrape_lavoz(paginas=15):
    props = []
    s = _http_session()
    try:
        _http_get("https://clasificados.lavoz.com.ar", sess=s, timeout=10)
    except Exception:
        pass
    for op, slug in [("venta", "venta"), ("alquiler", "alquiler")]:
        for i in range(1, paginas + 1):
            try:
                url = "https://clasificados.lavoz.com.ar/inmuebles/" + slug + "?page=" + str(i)
                html, status = _http_get(url, sess=s, timeout=20)
                if status in [403, 429]:
                    break
                soup = BeautifulSoup(html, "html.parser")
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
                                "titulo": titulo, "precio": precio, "moneda": "ARS",
                                "ubicacion": ubicacion_raw, "url": url_prop,
                                "imagen": get_imagen(img, card),
                                "fuente": "LaVoz", "operacion": op, "atributos": [],
                            })
                    except Exception:
                        pass
                print("LaVoz " + op + " p" + str(i) + ": " + str(len(cards)))
                time.sleep(random.uniform(1.5, 3))
            except Exception as e:
                print("LaVoz error: " + str(e))
    return props

def scrape_zonaprop(paginas=10):
    props = []
    s = _http_session()
    try:
        _http_get("https://www.zonaprop.com.ar", sess=s, timeout=10)
    except Exception:
        pass
    for op, slug in [("venta", "venta"), ("alquiler", "alquiler")]:
        for i in range(1, paginas + 1):
            try:
                if i == 1:
                    url = "https://www.zonaprop.com.ar/inmuebles-" + slug + "-cordoba.html"
                else:
                    url = ("https://www.zonaprop.com.ar/inmuebles-" + slug
                           + "-cordoba-pagina-" + str(i) + ".html")
                html, status = _http_get(url, sess=s, timeout=20)
                if status in [403, 429]:
                    time.sleep(random.uniform(10, 20))
                    break
                soup = BeautifulSoup(html, "html.parser")
                nuevas = []

                # Intentar extraer datos del script __NEXT_DATA__ (Next.js)
                next_data = soup.find("script", id="__NEXT_DATA__")
                if next_data and next_data.string:
                    try:
                        data = json.loads(next_data.string)
                        page_props = data.get("props", {}).get("pageProps", {})
                        listings = (
                            page_props.get("listings") or
                            page_props.get("listPostings") or
                            page_props.get("results") or
                            []
                        )
                        for item in listings:
                            try:
                                titulo = (item.get("title") or
                                          item.get("propertyType", {}).get("name", "") or "Propiedad")
                                precio_raw = item.get("price") or item.get("priceFormatted") or ""
                                precio = re.sub(r'[^0-9]', '', str(precio_raw))
                                moneda = item.get("currency", "USD")
                                ubicacion = (
                                    item.get("address") or
                                    str((item.get("location") or {}).get("name", "")) or
                                    "Córdoba"
                                )
                                prop_id = item.get("id") or item.get("postingId") or ""
                                url_prop = ("https://www.zonaprop.com.ar" + item.get("url", "")
                                            if item.get("url", "").startswith("/")
                                            else item.get("url", ""))
                                fotos = item.get("photos") or item.get("images") or []
                                imagen = ""
                                if fotos:
                                    primera = fotos[0]
                                    imagen = (primera if isinstance(primera, str)
                                              else primera.get("url", primera.get("src", "")))
                                atributos = []
                                for attr in (item.get("attributes") or item.get("features") or []):
                                    if isinstance(attr, dict):
                                        atributos.append(attr.get("label", "") + ": " + str(attr.get("value", "")))
                                    elif isinstance(attr, str):
                                        atributos.append(attr)
                                if titulo or precio:
                                    nuevas.append({
                                        "titulo": titulo, "precio": precio, "moneda": moneda,
                                        "ubicacion": ubicacion, "url": url_prop, "imagen": imagen,
                                        "fuente": "ZonaProp", "operacion": op, "atributos": atributos,
                                    })
                            except Exception:
                                pass
                    except Exception as e:
                        print("ZP JSON parse: " + str(e))

                # Fallback HTML si no se extrajo nada del JSON
                if not nuevas:
                    cards = soup.select("[data-id]")
                    for card in cards:
                        try:
                            t_el  = card.select_one(".postingCardTitle")
                            p_el  = card.select_one("[data-price]")
                            u_el  = (card.select_one(".postingCardLocation") or
                                     card.select_one("[class*='location']"))
                            l_el  = (card.select_one("a[href*='/propiedades/']") or
                                     card.select_one("a[href]"))
                            img   = card.select_one("img")
                            attrs = card.select(".postingCardAttribute")
                            titulo = t_el.text.strip() if t_el else ""
                            precio_raw = ""
                            if p_el:
                                precio_raw = p_el.get("data-price", "") or p_el.text
                            precio = re.sub(r'[^0-9]', '', precio_raw)
                            moneda = detectar_moneda(precio_raw)
                            ubicacion = u_el.text.strip() if u_el else "Córdoba"
                            href = l_el["href"] if l_el else ""
                            url_prop = ("https://www.zonaprop.com.ar" + href
                                        if href and not href.startswith("http") else href)
                            if titulo or precio:
                                nuevas.append({
                                    "titulo": titulo, "precio": precio, "moneda": moneda,
                                    "ubicacion": ubicacion, "url": url_prop, "imagen": get_imagen(img, card),
                                    "fuente": "ZonaProp", "operacion": op,
                                    "atributos": [a.text.strip() for a in attrs],
                                })
                        except Exception:
                            pass

                props.extend(nuevas)
                print("ZP " + op + " p" + str(i) + ": " + str(len(nuevas)))
                if not nuevas:
                    break
                time.sleep(random.uniform(2, 3))
            except Exception as e:
                print("ZP error: " + str(e))
                break
    return props

def _remax_parse_item(item, op):
    """Extrae una propiedad de un objeto JSON de Remax."""
    try:
        tipo = ""
        pt = item.get("propertyType") or item.get("property_type") or {}
        if isinstance(pt, dict):
            tipo = pt.get("name", "") or pt.get("descripcion", "")
        titulo = item.get("title") or item.get("titulo") or tipo or "Propiedad RE/MAX"
        precio_raw = (item.get("price") or item.get("precio") or
                      item.get("listingPrice") or "")
        precio = re.sub(r'[^0-9]', '', str(precio_raw))
        moneda = (item.get("currency") or item.get("currencySymbol") or
                  item.get("moneda") or "USD")
        if moneda in ("$", "ARS", "Pesos"):
            moneda = "ARS"
        loc = item.get("location") or item.get("address") or {}
        if isinstance(loc, dict):
            ubicacion = (loc.get("name") or loc.get("nombre") or
                         loc.get("address") or loc.get("fullAddress") or
                         str(loc.get("city", "")) or "Córdoba")
        else:
            ubicacion = str(loc) if loc else "Córdoba"
        if not ubicacion or ubicacion == "None":
            ubicacion = item.get("address", "") or "Córdoba"
        prop_id = (item.get("id") or item.get("listingId") or
                   item.get("listing_id") or item.get("codigo") or "")
        url_slug = item.get("url") or item.get("slug") or item.get("permalink") or ""
        if url_slug and url_slug.startswith("/"):
            url_prop = "https://www.remax.com.ar" + url_slug
        elif url_slug and url_slug.startswith("http"):
            url_prop = url_slug
        elif prop_id:
            url_prop = "https://www.remax.com.ar/propiedades/" + str(prop_id)
        else:
            url_prop = ""
        fotos = item.get("photos") or item.get("images") or item.get("fotos") or []
        imagen = ""
        if fotos:
            f = fotos[0]
            imagen = f if isinstance(f, str) else (f.get("url") or f.get("src") or f.get("photo") or "")
        return {
            "titulo": str(titulo)[:200], "precio": precio, "moneda": moneda,
            "ubicacion": str(ubicacion)[:200], "url": url_prop, "imagen": imagen,
            "fuente": "Remax", "operacion": op, "atributos": [],
        } if (titulo or precio) else None
    except Exception:
        return None


def _remax_explore_json(data, op, depth=0):
    """Recorre recursivamente el JSON de Remax buscando arrays de listings."""
    if depth > 6:
        return []
    found = []
    if isinstance(data, list):
        for item in data[:200]:
            if isinstance(item, dict) and ("price" in item or "titulo" in item or
                                            "propertyType" in item or "listingId" in item):
                parsed = _remax_parse_item(item, op)
                if parsed:
                    found.append(parsed)
        if found:
            return found
        for item in data:
            sub = _remax_explore_json(item, op, depth + 1)
            if sub:
                return sub
    elif isinstance(data, dict):
        for key in ["listings", "results", "items", "data", "properties",
                    "propiedades", "listPostings", "posts", "records"]:
            if key in data and data[key]:
                sub = _remax_explore_json(data[key], op, depth + 1)
                if sub:
                    return sub
        for v in data.values():
            if isinstance(v, (dict, list)):
                sub = _remax_explore_json(v, op, depth + 1)
                if sub:
                    return sub
    return []


def scrape_remax(paginas=10):
    props = []
    s = _http_session()

    # URL patterns Remax Argentina (cambian frecuentemente)
    URL_PATTERNS = [
        ("venta",    ["https://www.remax.com.ar/venta/propiedades/en-cordoba",
                      "https://www.remax.com.ar/comprar/propiedades/cordoba--provincia",
                      "https://www.remax.com.ar/propiedades-en-venta-en-cordoba-capital.html"]),
        ("alquiler", ["https://www.remax.com.ar/alquiler/propiedades/en-cordoba",
                      "https://www.remax.com.ar/alquilar/propiedades/cordoba--provincia",
                      "https://www.remax.com.ar/propiedades-en-alquiler-en-cordoba-capital.html"]),
    ]

    # Intentar primero la API REST de Remax (si está disponible)
    try:
        _http_get("https://www.remax.com.ar", sess=s, timeout=10)
    except Exception:
        pass

    for op, url_list in URL_PATTERNS:
        base_url = ""
        # Detectar qué URL funciona
        for candidate in url_list:
            try:
                html_test, st = _http_get(candidate, sess=s, timeout=15)
                if st < 400 and len(html_test) > 2000:
                    base_url = candidate
                    break
            except Exception:
                continue

        if not base_url:
            print("Remax: no URL funcional para " + op)
            continue

        for i in range(1, paginas + 1):
            try:
                if i == 1:
                    url = base_url
                else:
                    sep = "&" if "?" in base_url else "?"
                    url = base_url + sep + "page=" + str(i)

                html, status = _http_get(url, sess=s, timeout=20)
                if status in [403, 429]:
                    time.sleep(random.uniform(10, 20))
                    break
                soup = BeautifulSoup(html, "html.parser")
                nuevas = []

                # __NEXT_DATA__ — exploración recursiva del JSON completo
                for script in soup.find_all("script"):
                    txt = script.string or ""
                    if "__NEXT_DATA__" in (script.get("id") or "") or (
                            '"listings"' in txt or '"results"' in txt or '"listPostings"' in txt):
                        try:
                            raw = txt.strip()
                            if raw.startswith("self.__next_f"):
                                continue
                            data = json.loads(raw)
                            found = _remax_explore_json(data, op)
                            if found:
                                nuevas.extend(found)
                                break
                        except Exception:
                            pass

                # Fallback: scripts con JSON embebido (window.__INITIAL_STATE__ etc.)
                if not nuevas:
                    for script in soup.find_all("script"):
                        txt = script.string or ""
                        for prefix in ["window.__INITIAL_STATE__=",
                                       "window.__STATE__=",
                                       "var initialState="]:
                            if prefix in txt:
                                try:
                                    raw = txt[txt.index(prefix) + len(prefix):].split(";\n")[0].strip()
                                    data = json.loads(raw)
                                    found = _remax_explore_json(data, op)
                                    if found:
                                        nuevas.extend(found)
                                        break
                                except Exception:
                                    pass
                        if nuevas:
                            break

                # Fallback HTML con selectores amplios
                if not nuevas:
                    selectors = [
                        "[class*='ListingCard']", "[class*='listing-card']",
                        "[class*='property-card']", "[class*='PropertyCard']",
                        "article.card", "article[data-id]", "article",
                        "[class*='card'][class*='listing']",
                    ]
                    cards = []
                    for sel in selectors:
                        cards = soup.select(sel)
                        if len(cards) >= 3:
                            break
                    for card in cards:
                        try:
                            t_el = card.select_one("h2,h3,[class*='title'],[class*='Title']")
                            p_el = card.select_one("[class*='price'],[class*='Price'],[class*='valor']")
                            u_el = card.select_one("[class*='address'],[class*='location'],[class*='Address']")
                            l_el = card.select_one("a[href]")
                            img  = card.select_one("img")
                            titulo = t_el.text.strip() if t_el else ""
                            precio_raw = p_el.text.strip() if p_el else ""
                            precio = re.sub(r'[^0-9]', '', precio_raw)
                            moneda = detectar_moneda(precio_raw)
                            ubicacion = u_el.text.strip() if u_el else "Córdoba"
                            href = l_el["href"] if l_el else ""
                            url_prop = (href if href.startswith("http")
                                        else "https://www.remax.com.ar" + href)
                            if titulo or precio:
                                nuevas.append({
                                    "titulo": titulo, "precio": precio, "moneda": moneda,
                                    "ubicacion": ubicacion, "url": url_prop,
                                    "imagen": get_imagen(img, card),
                                    "fuente": "Remax", "operacion": op, "atributos": [],
                                })
                        except Exception:
                            pass

                props.extend(nuevas)
                print("RM " + op + " p" + str(i) + ": " + str(len(nuevas)) + " (url=" + url + ")")
                if not nuevas:
                    break
                time.sleep(random.uniform(2, 4))
            except Exception as e:
                print("RM " + op + " error: " + str(e))
                break
    return props

def scrape_navent(paginas=10):
    """Inmuebles.com (grupo Navent) — fuente adicional para Córdoba."""
    props = []
    s = _http_session()
    try:
        _http_get("https://www.inmuebles.com", sess=s, timeout=10)
    except Exception:
        pass
    for op, slug in [("venta", "venta"), ("alquiler", "alquiler")]:
        for i in range(1, paginas + 1):
            try:
                if i == 1:
                    url = "https://www.inmuebles.com/propiedades-en-" + slug + "-en-cordoba.html"
                else:
                    url = ("https://www.inmuebles.com/propiedades-en-" + slug
                           + "-en-cordoba-pagina-" + str(i) + ".html")
                html, status = _http_get(url, sess=s, timeout=20)
                if status in [403, 429]:
                    time.sleep(random.uniform(10, 20))
                    break
                soup = BeautifulSoup(html, "html.parser")
                nuevas = []

                next_data = soup.find("script", id="__NEXT_DATA__")
                if next_data and next_data.string:
                    try:
                        data = json.loads(next_data.string)
                        page_props = data.get("props", {}).get("pageProps", {})
                        listings = (
                            page_props.get("listings") or
                            page_props.get("listPostings") or
                            page_props.get("results") or []
                        )
                        for item in listings:
                            try:
                                titulo = (item.get("title") or
                                          (item.get("propertyType") or {}).get("name", "") or "Propiedad")
                                precio_raw = item.get("price") or item.get("priceFormatted") or ""
                                precio = re.sub(r'[^0-9]', '', str(precio_raw))
                                moneda = item.get("currency", "USD")
                                ubicacion = (item.get("address") or
                                             str((item.get("location") or {}).get("name", "")) or "Córdoba")
                                url_rel = item.get("url", "")
                                url_prop = ("https://www.inmuebles.com" + url_rel
                                            if url_rel.startswith("/") else url_rel)
                                fotos = item.get("photos") or item.get("images") or []
                                imagen = ""
                                if fotos:
                                    primera = fotos[0]
                                    imagen = (primera if isinstance(primera, str)
                                              else primera.get("url", primera.get("src", "")))
                                atributos = []
                                for attr in (item.get("attributes") or item.get("features") or []):
                                    if isinstance(attr, dict):
                                        atributos.append(attr.get("label", "") + ": " + str(attr.get("value", "")))
                                    elif isinstance(attr, str):
                                        atributos.append(attr)
                                if titulo or precio:
                                    nuevas.append({
                                        "titulo": titulo, "precio": precio, "moneda": moneda,
                                        "ubicacion": ubicacion, "url": url_prop, "imagen": imagen,
                                        "fuente": "Navent", "operacion": op, "atributos": atributos,
                                    })
                            except Exception:
                                pass
                    except Exception as e:
                        print("Navent JSON: " + str(e))

                if not nuevas:
                    cards = soup.select("[data-id]") or soup.select("article[class*='card']")
                    for card in cards:
                        try:
                            t_el = card.select_one("[class*='title'],[class*='Title'],h2,h3")
                            p_el = card.select_one("[data-price],[class*='price'],[class*='Price']")
                            u_el = card.select_one("[class*='location'],[class*='address']")
                            l_el = card.select_one("a[href]")
                            img  = card.select_one("img")
                            titulo = t_el.text.strip() if t_el else ""
                            precio_raw = (p_el.get("data-price", "") or p_el.text) if p_el else ""
                            precio = re.sub(r'[^0-9]', '', precio_raw)
                            moneda = detectar_moneda(precio_raw)
                            ubicacion = u_el.text.strip() if u_el else "Córdoba"
                            href = l_el["href"] if l_el else ""
                            url_prop = (href if href.startswith("http")
                                        else "https://www.inmuebles.com" + href)
                            if titulo or precio:
                                nuevas.append({
                                    "titulo": titulo, "precio": precio, "moneda": moneda,
                                    "ubicacion": ubicacion, "url": url_prop, "imagen": get_imagen(img, card),
                                    "fuente": "Navent", "operacion": op, "atributos": [],
                                })
                        except Exception:
                            pass

                props.extend(nuevas)
                print("Navent " + op + " p" + str(i) + ": " + str(len(nuevas)))
                if not nuevas:
                    break
                time.sleep(random.uniform(2, 3))
            except Exception as e:
                print("Navent error: " + str(e))
                break
    return props

# ─── ALERTAS ─────────────────────────────────────────────────────────────────

def chequear_alertas():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id,nombre,email,whatsapp,zona,tipo,operacion,"
            "precio_min,precio_max,ambientes,cocheras,plan,"
            "COALESCE(alertas_enviadas_count,0) as alertas_count,"
            "plan_vence"
            " FROM usuarios WHERE activo=TRUE AND whatsapp != ''"
            " AND (plan='premium' OR plan='inversor' OR COALESCE(alertas_enviadas_count,0) < 7)"
        )
        cols = ["id", "nombre", "email", "whatsapp", "zona", "tipo", "operacion",
                "precio_min", "precio_max", "ambientes", "cocheras", "plan", "alertas_count",
                "plan_vence"]
        usuarios = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        for u in usuarios:
            try:
                # Verificar vencimiento del plan
                plan_actual = u["plan"]
                if plan_actual in ("premium", "inversor") and u.get("plan_vence"):
                    try:
                        vence = u["plan_vence"]
                        vence_dt = vence if isinstance(vence, datetime) else datetime.fromisoformat(str(vence))
                        if vence_dt < datetime.now():
                            plan_actual = "gratis"
                    except Exception:
                        pass
                u["plan"] = plan_actual

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
                # Filtro de precio con pre-validación numérica para evitar errores de CAST
                precio_min = int(u["precio_min"]) if u.get("precio_min") else 0
                precio_max = int(u["precio_max"]) if u.get("precio_max") else 999999999
                if precio_min > 0 or precio_max < 999999999:
                    query += " AND precio ~ '^[0-9]+$'"
                if precio_min > 0:
                    query += " AND CAST(precio AS BIGINT) >= %s"
                    params.append(precio_min)
                if precio_max < 999999999:
                    query += " AND CAST(precio AS BIGINT) <= %s"
                    params.append(precio_max)
                if u.get("ambientes"):
                    query += " AND (LOWER(titulo) LIKE %s OR LOWER(titulo) LIKE %s)"
                    params += ["%" + u["ambientes"] + " amb%", "%" + u["ambientes"] + " dorm%"]
                if u.get("cocheras") and str(u["cocheras"]).strip():
                    query += " AND (LOWER(titulo) LIKE %s OR LOWER(atributos) LIKE %s)"
                    params += ["%cochera%", "%cochera%"]
                query += " AND url NOT IN (SELECT propiedad_url FROM alertas_enviadas WHERE usuario_id=%s) LIMIT 3"
                params.append(u["id"])
                cur2.execute(query, params)
                props_encontradas = cur2.fetchall()
                alertas_count_actual = int(u.get("alertas_count", 0))

                for prop in props_encontradas:
                    titulo, precio, moneda, ubicacion, url, fuente = prop

                    if u["plan"] == "gratis" and alertas_count_actual >= 7:
                        link_premium = FRONTEND_URL + "/premium?email=" + u.get("email", "")
                        msg_limite = (
                            "⚠️ *RodiProp — Llegaste a tu límite gratuito*\n\n"
                            "Usaste tus 7 alertas del plan gratuito.\n\n"
                            "🚀 Pasate a *Premium* por solo *$4.999/mes* y recibí alertas ilimitadas:\n"
                            "👉 " + link_premium + "\n\n"
                            "_Las mejores propiedades duran menos de 24hs — no te las pierdas._"
                        )
                        enviar_whatsapp(u["whatsapp"], msg_limite)
                        break

                    precio_str = (moneda + " " + "{:,}".format(int(precio))
                                  if precio and precio.isdigit() else precio or "Consultar")

                    es_ultima = (u["plan"] == "gratis" and alertas_count_actual == 6)
                    aviso_limite = (
                        "\n\n⚠️ _Esta es tu última alerta gratuita. Upgrade a Premium:_\n"
                        + FRONTEND_URL + "/premium?email=" + u.get("email", "")
                    ) if es_ultima else "\nPara pausar respondé STOP"

                    mensaje = (
                        "🏠 *RodiProp — Nueva propiedad!*\n\n"
                        + titulo + "\n"
                        "📍 " + ubicacion + "\n"
                        "💰 " + precio_str + "\n"
                        "🔗 " + url + "\n\n"
                        "_Fuente: " + fuente + "_"
                        + aviso_limite
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
                        alertas_count_actual += 1
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
    for fn, name, kwargs in [
        (scrape_ml,       "ML",       {"paginas": 5}),
        (scrape_ap,       "AP",       {}),
        (scrape_lavoz,    "LaVoz",    {"paginas": 15}),
        (scrape_zonaprop, "ZonaProp", {"paginas": 10}),
        (scrape_remax,    "Remax",    {"paginas": 10}),
        (scrape_navent,   "Navent",   {"paginas": 10}),
    ]:
        try:
            r = fn(**kwargs)
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
        "version": "10.0",
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

@app.route("/api/imagen")
def proxy_imagen():
    from flask import Response
    from urllib.parse import urlparse
    url = request.args.get("url", "").strip()
    if not url or not url.startswith("http"):
        return jsonify({"error": "URL requerida"}), 400
    allowed_hosts = [
        "mlstatic.com", "http2.mlstatic.com",
        "zonaprop.com.ar", "cdn1.zonaprop.com.ar",
        "argenprop.com", "cdn.argenprop.com",
        "remax.com.ar",
        "lavoz.com.ar", "clasificados.lavoz.com.ar",
        "mercadolibre.com.ar", "mercadolibre.com",
        "photos.zoocdn.com", "i.ibb.co",
    ]
    try:
        parsed = urlparse(url)
        if not any(h in parsed.netloc for h in allowed_hosts):
            return jsonify({"error": "Host no permitido"}), 403
        req = urllib.request.Request(url)
        req.add_header("User-Agent", random.choice(USER_AGENTS))
        req.add_header("Referer", parsed.scheme + "://" + parsed.netloc + "/")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read()
            ct = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
        return Response(data, mimetype=ct, headers={"Cache-Control": "public, max-age=86400"})
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/scraper/ejecutar", methods=["GET", "POST"])
@require_admin
def trigger():
    threading.Thread(target=run_scraper, daemon=True).start()
    return jsonify({"status": "Scraper iniciado"})

@app.route("/api/alertas/test", methods=["GET", "POST"])
@require_admin
def test_alerta():
    threading.Thread(target=chequear_alertas, daemon=True).start()
    return jsonify({"status": "Chequeando alertas en background"})

@app.route("/admin")
def admin_panel():
    return app.send_static_file("admin.html")

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
@require_admin
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
@require_admin
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

@app.route("/api/usuarios/perfil")
def perfil_usuario():
    email = request.args.get("email", "").strip().lower()
    if not email:
        return jsonify({"error": "Email requerido"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT nombre, email, whatsapp, zona, tipo, operacion,"
            " precio_min, precio_max, ambientes, cocheras, plan,"
            " alertas_enviadas_count, plan_vence, fecha"
            " FROM usuarios WHERE email=%s AND activo=TRUE",
            (email,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"error": "Usuario no encontrado"}), 404
        cols = ["nombre", "email", "whatsapp", "zona", "tipo", "operacion",
                "precio_min", "precio_max", "ambientes", "cocheras", "plan",
                "alertas_enviadas_count", "plan_vence", "fecha"]
        u = dict(zip(cols, row))
        for k in ["plan_vence", "fecha"]:
            if u.get(k):
                u[k] = str(u[k])
        return jsonify(u)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/usuarios/actualizar", methods=["POST", "OPTIONS"])
def actualizar_usuario():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json() or {}
    email = data.get("email", "").strip().lower()
    if not email:
        return jsonify({"error": "Email requerido"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE usuarios SET zona=%s, tipo=%s, operacion=%s,"
            " precio_min=%s, precio_max=%s, ambientes=%s, cocheras=%s, whatsapp=%s"
            " WHERE email=%s AND activo=TRUE",
            (data.get("zona", ""), data.get("tipo", ""),
             data.get("operacion", "venta"),
             int(data.get("precio_min", 0) or 0),
             int(data.get("precio_max", 999999999) or 999999999),
             data.get("ambientes", ""), data.get("cocheras", ""),
             data.get("whatsapp", ""), email)
        )
        updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if not updated:
            return jsonify({"error": "Usuario no encontrado"}), 404
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/usuarios/baja", methods=["POST", "OPTIONS"])
def baja_usuario():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json() or {}
    email = data.get("email", "").strip().lower()
    if not email:
        return jsonify({"error": "Email requerido"}), 400
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE usuarios SET activo=FALSE WHERE email=%s", (email,))
        updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if not updated:
            return jsonify({"error": "Usuario no encontrado"}), 404
        return jsonify({"status": "ok", "mensaje": "Cuenta desactivada"})
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
    tipo     = data.get("tipo", "checkout")
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
@require_admin
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
@require_admin
def fix_moneda():
    try:
        conn = get_conn()
        cur = conn.cursor()
        # LaVoz publica en ARS, no USD
        cur.execute("UPDATE propiedades SET moneda='ARS' WHERE fuente='LaVoz'")
        lavoz = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "lavoz_corregidas": lavoz})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/auth", methods=["POST", "OPTIONS"])
def admin_auth():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json()
    pwd = data.get("password", "")
    admin_pwd = os.environ.get("ADMIN_PASSWORD", "")
    if pwd == admin_pwd:
        return jsonify({"ok": True})
    return jsonify({"ok": False}), 401

@app.route("/api/whatsapp/status")
def wa_status():
    try:
        r = requests.get(WA_SERVICE_URL + "/status", timeout=5)
        return jsonify(r.json())
    except Exception:
        return jsonify({"ready": False})

# ─── AUTH ─────────────────────────────────────────────────────────────────────

def _make_token(user_id):
    ts = str(int(time.time()))
    uid = str(user_id)
    payload = f"{uid}:{ts}"
    sig = hmac.new(JWT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:24]
    raw = f"{payload}:{sig}"
    return base64.b64encode(raw.encode()).decode().rstrip("=")

def _verify_token(token):
    try:
        pad = (4 - len(token) % 4) % 4
        decoded = base64.b64decode(token + "=" * pad).decode()
        parts = decoded.rsplit(":", 1)
        sig = parts[1]
        payload = parts[0]
        expected = hmac.new(JWT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:24]
        if not hmac.compare_digest(sig, expected):
            return None
        uid_str, ts_str = payload.split(":", 1)
        if int(time.time()) - int(ts_str) > 30 * 24 * 3600:
            return None
        return int(uid_str)
    except Exception:
        return None

def _get_token():
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return request.args.get("token") or (request.get_json(silent=True) or {}).get("token")

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == "OPTIONS":
            return jsonify({}), 200
        token = _get_token()
        user_id = _verify_token(token) if token else None
        if not user_id:
            return jsonify({"error": "No autorizado"}), 401
        request.user_id = user_id
        return f(*args, **kwargs)
    return decorated

@app.route("/api/auth/register", methods=["POST", "OPTIONS"])
def auth_register():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json() or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "").strip()
    nombre = data.get("nombre", "").strip()
    whatsapp = data.get("whatsapp", "").strip()
    if not email or not password:
        return jsonify({"error": "Email y contraseña requeridos"}), 400
    if len(password) < 6:
        return jsonify({"error": "La contraseña debe tener al menos 6 caracteres"}), 400
    pw_hash = generate_password_hash(password)
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, plan FROM usuarios WHERE email=%s", (email,))
        existing = cur.fetchone()
        if existing:
            user_id, plan = existing
            cur.execute("UPDATE usuarios SET password_hash=%s WHERE email=%s", (pw_hash, email))
        else:
            cur.execute(
                "INSERT INTO usuarios (nombre, email, whatsapp, zona, tipo, operacion, plan, password_hash)"
                " VALUES (%s,%s,%s,'','','venta','gratis',%s) RETURNING id, plan",
                (nombre or email.split("@")[0], email, whatsapp, pw_hash)
            )
            user_id, plan = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        token = _make_token(user_id)
        is_admin = email.lower() == ADMIN_EMAIL.lower()
        return jsonify({"token": token, "user": {"id": user_id, "email": email, "plan": plan, "nombre": nombre, "is_admin": is_admin}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/auth/login", methods=["POST", "OPTIONS"])
def auth_login():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json() or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "").strip()
    if not email or not password:
        return jsonify({"error": "Email y contraseña requeridos"}), 400
    try:
        is_admin_login = email.lower() == ADMIN_EMAIL.lower()
        admin_pwd = os.environ.get("ADMIN_PASSWORD", "")

        # Admin puede ingresar con ADMIN_PASSWORD sin necesidad de registrarse
        if is_admin_login and admin_pwd and password == admin_pwd:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, nombre, plan FROM usuarios WHERE email=%s", (email,))
            row = cur.fetchone()
            if not row:
                # Auto-crear cuenta admin si no existe
                cur.execute(
                    "INSERT INTO usuarios (nombre, email, whatsapp, zona, tipo, operacion, plan)"
                    " VALUES (%s,%s,'','','','venta','inversor') RETURNING id, nombre, plan",
                    ("Administrador", email)
                )
                row = cur.fetchone()
                conn.commit()
            user_id, nombre, plan = row
            # Asegurar que el admin siempre tenga plan inversor
            if plan not in ("inversor", "analitic"):
                cur.execute("UPDATE usuarios SET plan='inversor' WHERE id=%s", (user_id,))
                conn.commit()
                plan = "inversor"
            cur.close(); conn.close()
            token = _make_token(user_id)
            return jsonify({"token": token, "user": {"id": user_id, "email": email, "plan": plan, "nombre": nombre or "Administrador", "is_admin": True}})

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, nombre, email, plan, password_hash FROM usuarios WHERE email=%s AND activo=TRUE",
            (email,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"error": "Usuario no encontrado"}), 404
        user_id, nombre, email_db, plan, pw_hash = row
        if not pw_hash:
            return jsonify({"error": "Esta cuenta no tiene contraseña. Usá 'Crear cuenta' para establecer una."}), 400
        if not check_password_hash(pw_hash, password):
            return jsonify({"error": "Contraseña incorrecta"}), 401
        token = _make_token(user_id)
        is_admin = email_db.lower() == ADMIN_EMAIL.lower()
        return jsonify({"token": token, "user": {"id": user_id, "email": email_db, "plan": plan, "nombre": nombre, "is_admin": is_admin}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/auth/me")
@require_auth
def auth_me():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, nombre, email, whatsapp, zona, tipo, operacion,"
            " precio_min, precio_max, ambientes, cocheras, plan, alertas_enviadas_count, plan_vence"
            " FROM usuarios WHERE id=%s AND activo=TRUE",
            (request.user_id,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"error": "Usuario no encontrado"}), 404
        cols = ["id", "nombre", "email", "whatsapp", "zona", "tipo", "operacion",
                "precio_min", "precio_max", "ambientes", "cocheras", "plan",
                "alertas_enviadas_count", "plan_vence"]
        u = dict(zip(cols, row))
        if u.get("plan_vence"):
            u["plan_vence"] = str(u["plan_vence"])
        u["is_admin"] = u.get("email", "").lower() == ADMIN_EMAIL.lower()
        return jsonify(u)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/auth/update", methods=["POST", "OPTIONS"])
@require_auth
def auth_update():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json() or {}
    try:
        conn = get_conn()
        cur = conn.cursor()
        fields = ["zona", "tipo", "operacion", "precio_min", "precio_max",
                  "ambientes", "cocheras", "whatsapp", "nombre"]
        updates = {k: data[k] for k in fields if k in data}
        if not updates:
            return jsonify({"error": "Nada que actualizar"}), 400
        sets = ", ".join(f"{k}=%s" for k in updates)
        vals = list(updates.values()) + [request.user_id]
        cur.execute(f"UPDATE usuarios SET {sets} WHERE id=%s", vals)
        if "new_password" in data and len(data["new_password"]) >= 6:
            cur.execute("UPDATE usuarios SET password_hash=%s WHERE id=%s",
                        (generate_password_hash(data["new_password"]), request.user_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── MARKET DATA ──────────────────────────────────────────────────────────────

@app.route("/api/dolar")
def cotizacion_dolar():
    try:
        req = urllib.request.Request(
            "https://dolarapi.com/v1/dolares",
            headers={"User-Agent": "RodiProp/1.0", "Accept": "application/json"}
        )
        data = json.loads(urllib.request.urlopen(req, timeout=6).read())
        return jsonify({"dolares": data, "actualizado": datetime.now().isoformat()})
    except Exception as e:
        return jsonify({"error": str(e), "dolares": []}), 200

@app.route("/api/creditos")
def creditos_hipotecarios():
    return jsonify({"creditos": CREDITOS_HIPOTECARIOS})

# ─── ANALYTICS ────────────────────────────────────────────────────────────────

def _analytics_check(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT plan, email FROM usuarios WHERE id=%s", (user_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    is_admin = row and row[1].lower() == ADMIN_EMAIL.lower()
    allowed = row and (row[0] in ANALYTICS_PLANS or is_admin)
    return allowed, is_admin

@app.route("/api/analytics/zonas")
@require_auth
def analytics_zonas():
    try:
        allowed, _ = _analytics_check(request.user_id)
        if not allowed:
            return jsonify({"error": "Requiere plan Analítics o Inversor", "upgrade": True}), 403
        conn = get_conn()
        cur = conn.cursor()

        # Demanda: zonas más buscadas por usuarios registrados
        cur.execute("""
            SELECT zona,
                COUNT(*) AS usuarios_buscando,
                ROUND(AVG(CASE WHEN precio_max < 999999999 THEN precio_max::NUMERIC END)) AS presupuesto_promedio
            FROM usuarios
            WHERE activo = TRUE AND zona IS NOT NULL AND LENGTH(TRIM(zona)) > 2
            GROUP BY zona
            ORDER BY usuarios_buscando DESC
            LIMIT 12
        """)
        demand_rows = cur.fetchall()

        # Oferta: listados por fuente (más confiable que parsear ubicacion)
        cur.execute("""
            SELECT fuente,
                COUNT(*) AS total,
                COUNT(CASE WHEN fecha > NOW() - INTERVAL '7 days' THEN 1 END) AS esta_semana,
                COUNT(CASE WHEN fecha > NOW() - INTERVAL '30 days' THEN 1 END) AS este_mes,
                MAX(fecha) AS ultima_actualizacion
            FROM propiedades
            GROUP BY fuente
            ORDER BY esta_semana DESC, total DESC
        """)
        supply_rows = cur.fetchall()

        # Zonas más mencionadas en propiedades (usando ubicacion limpia)
        cur.execute("""
            SELECT
                TRIM(REGEXP_REPLACE(
                    CASE WHEN ubicacion ~ '[0-9]'
                         THEN COALESCE(NULLIF(TRIM(SPLIT_PART(ubicacion,',',2)),''), TRIM(SPLIT_PART(ubicacion,',',1)))
                         ELSE TRIM(SPLIT_PART(ubicacion,',',1))
                    END,
                '\\s+', ' ', 'g')) AS zona,
                COUNT(*) AS total_props
            FROM propiedades
            WHERE ubicacion IS NOT NULL AND ubicacion != ''
            GROUP BY zona
            HAVING COUNT(*) >= 3
              AND LENGTH(TRIM(REGEXP_REPLACE(
                    CASE WHEN ubicacion ~ '[0-9]'
                         THEN COALESCE(NULLIF(TRIM(SPLIT_PART(ubicacion,',',2)),''), TRIM(SPLIT_PART(ubicacion,',',1)))
                         ELSE TRIM(SPLIT_PART(ubicacion,',',1))
                    END, '\\s+', ' ', 'g'))) > 3
            ORDER BY total_props DESC
            LIMIT 15
        """)
        zona_rows = cur.fetchall()
        cur.close(); conn.close()

        demanda = [{"zona": r[0], "usuarios_buscando": r[1],
                    "presupuesto_promedio": float(r[2]) if r[2] else 0}
                   for r in demand_rows]
        oferta_fuentes = [{"fuente": r[0], "total": r[1], "esta_semana": r[2],
                           "este_mes": r[3], "ultima_actualizacion": str(r[4]) if r[4] else None}
                          for r in supply_rows]
        zonas_props = [{"zona": r[0], "total_props": r[1]} for r in zona_rows]

        return jsonify({"demanda": demanda, "oferta_fuentes": oferta_fuentes, "zonas_props": zonas_props})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/analytics/precios")
@require_auth
def analytics_precios():
    try:
        allowed, _ = _analytics_check(request.user_id)
        if not allowed:
            return jsonify({"error": "Requiere plan Analítics o Inversor", "upgrade": True}), 403
        conn = get_conn()
        cur = conn.cursor()
        # Precios actuales (ventana amplia para plataforma nueva)
        cur.execute("""
            WITH actual AS (
                SELECT operacion, moneda,
                    ROUND(AVG(NULLIF(REGEXP_REPLACE(precio,'[^0-9]','','g'),'')::BIGINT)::NUMERIC) AS avg_precio,
                    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                        ORDER BY NULLIF(REGEXP_REPLACE(precio,'[^0-9]','','g'),'')::BIGINT
                    )::NUMERIC) AS mediana,
                    COUNT(*) AS cnt,
                    MIN(NULLIF(REGEXP_REPLACE(precio,'[^0-9]','','g'),'')::BIGINT) AS precio_min,
                    MAX(NULLIF(REGEXP_REPLACE(precio,'[^0-9]','','g'),'')::BIGINT) AS precio_max
                FROM propiedades
                WHERE precio ~ '[0-9]{4,}'
                  AND fecha > NOW() - INTERVAL '60 days'
                GROUP BY operacion, moneda
            ),
            historico AS (
                SELECT operacion, moneda,
                    ROUND(AVG(NULLIF(REGEXP_REPLACE(precio,'[^0-9]','','g'),'')::BIGINT)::NUMERIC) AS avg_precio
                FROM propiedades
                WHERE precio ~ '[0-9]{4,}'
                  AND fecha BETWEEN NOW() - INTERVAL '120 days' AND NOW() - INTERVAL '60 days'
                GROUP BY operacion, moneda
            )
            SELECT a.operacion, a.moneda,
                   a.avg_precio, a.mediana, a.cnt, a.precio_min, a.precio_max,
                   h.avg_precio AS avg_historico,
                   CASE WHEN h.avg_precio > 0
                        THEN ROUND(((a.avg_precio - h.avg_precio)/h.avg_precio*100)::NUMERIC,1)
                   END AS pct_cambio
            FROM actual a
            LEFT JOIN historico h USING (operacion, moneda)
            WHERE a.cnt >= 2
            ORDER BY a.operacion, a.moneda
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        cols = ["operacion","moneda","avg_precio","mediana","cnt","precio_min","precio_max","avg_historico","pct_cambio"]
        result = []
        for r in rows:
            d = dict(zip(cols, r))
            for k in cols:
                if d[k] is not None and hasattr(d[k],'real'):
                    d[k] = float(d[k])
            pct = d.get("pct_cambio")
            d["tendencia"] = "alza" if pct and pct > 3 else ("baja" if pct and pct < -3 else "estable")
            result.append(d)
        return jsonify({"precios": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── BÚSQUEDA CON IA ──────────────────────────────────────────────────────────

PAID_PLANS = {"premium", "inversor", "analitic"}

@app.route("/api/buscar-ia", methods=["POST", "OPTIONS"])
@require_auth
def buscar_ia():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT plan, email FROM usuarios WHERE id=%s", (request.user_id,))
    row = cur.fetchone()
    is_admin = row and row[1].lower() == ADMIN_EMAIL.lower()
    if not row or (row[0] not in PAID_PLANS and not is_admin):
        cur.close(); conn.close()
        return jsonify({"error": "Función exclusiva para usuarios con plan pago", "upgrade": True}), 403

    data = request.get_json() or {}
    operacion  = data.get("operacion", "").lower().strip()
    tipo       = data.get("tipo", "").strip()
    zona       = data.get("zona", "").strip()
    ambientes  = data.get("ambientes", "").strip()
    precio_max = int(data.get("precio_max") or 0)
    moneda     = data.get("moneda", "USD").upper()
    cochera    = bool(data.get("cochera"))
    descripcion= data.get("descripcion", "").strip()

    # Construir query de candidatos (filtros amplios)
    conds = ["fecha > NOW() - INTERVAL '60 days'"]
    params = []
    if operacion:
        conds.append("LOWER(operacion) = %s"); params.append(operacion)
    if zona:
        conds.append("LOWER(ubicacion) LIKE %s"); params.append(f"%{zona.lower()}%")
    if precio_max > 0:
        conds.append("NULLIF(REGEXP_REPLACE(precio,'[^0-9]','','g'),'')::BIGINT <= %s")
        params.append(int(precio_max * 1.4))

    where = " AND ".join(conds)
    try:
        cur.execute(
            f"SELECT titulo, precio, moneda, ubicacion, url, imagen, fuente, operacion, atributos"
            f" FROM propiedades WHERE {where} ORDER BY fecha DESC LIMIT 40",
            params
        )
        rows = cur.fetchall()
        cur.close(); conn.close()
    except Exception as e:
        cur.close(); conn.close()
        return jsonify({"error": str(e)}), 500

    props = []
    for r in rows:
        titulo, precio, mon, ubicacion, url, imagen, fuente, op, atributos_raw = r
        try:
            attrs = json.loads(atributos_raw) if atributos_raw else []
        except Exception:
            attrs = [atributos_raw] if atributos_raw else []
        props.append({"titulo": titulo or "", "precio": precio or "", "moneda": mon or "",
                      "ubicacion": ubicacion or "", "url": url or "", "imagen": imagen or "",
                      "fuente": fuente or "", "operacion": op or "", "atributos": attrs})

    if not props:
        return jsonify({"propiedades": [], "ia_resumen": "No encontré propiedades con esos criterios. Probá ampliando zona o presupuesto.", "ia_powered": False})

    # IA ranking via Claude
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        try:
            import anthropic as anth
            client = anth.Anthropic(api_key=api_key)
            criterios_txt = "\n".join(filter(None, [
                f"- Operación: {operacion}" if operacion else "",
                f"- Tipo: {tipo}" if tipo else "",
                f"- Zona: {zona}" if zona else "",
                f"- Ambientes: {ambientes}" if ambientes else "",
                f"- Presupuesto máximo: {precio_max:,} {moneda}" if precio_max else "",
                f"- Con cochera" if cochera else "",
                f"- Descripción adicional: {descripcion}" if descripcion else "",
            ]))
            sample = props[:20]
            props_txt = "\n".join(
                f"{i+1}. {p['titulo'][:55]} | {p['precio']} {p['moneda']} | {p['ubicacion'][:45]} | {' · '.join(str(a) for a in p['atributos'][:4])}"
                for i, p in enumerate(sample)
            )
            prompt = (
                "Sos un asistente inmobiliario experto en Córdoba, Argentina.\n\n"
                f"El cliente busca:\n{criterios_txt or 'Sin criterios específicos'}\n\n"
                f"Propiedades disponibles:\n{props_txt}\n\n"
                f"Seleccioná las mejores 5 opciones para el cliente. "
                f"Respondé ÚNICAMENTE con JSON válido (sin markdown):\n"
                f'{{\"ranking\":[{{\"indice\":1,\"explicacion\":\"...\"}}],\"resumen\":\"...\"}}\n'
                f"Índices del 1 al {len(sample)}. Explicación en 1 oración en español."
            )
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=700,
                messages=[{"role":"user","content":prompt}]
            )
            raw = resp.content[0].text.strip()
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            result_ai = json.loads(raw.strip())
            ranked = []
            for item in result_ai.get("ranking", []):
                idx = item.get("indice", 0) - 1
                if 0 <= idx < len(sample):
                    p = dict(sample[idx])
                    p["ia_explicacion"] = item.get("explicacion", "")
                    ranked.append(p)
            # Completar con props no rankeadas si quedan menos de 8
            seen = {p["url"] for p in ranked}
            for p in props:
                if p["url"] not in seen and len(ranked) < 8:
                    ranked.append(p); seen.add(p["url"])
            return jsonify({"propiedades": ranked[:8], "ia_resumen": result_ai.get("resumen",""), "ia_powered": True})
        except Exception as e:
            print(f"AI error: {e}")

    # Fallback sin IA
    return jsonify({"propiedades": props[:8],
                    "ia_resumen": f"Se encontraron {len(props)} propiedades que coinciden con tu búsqueda.",
                    "ia_powered": False})

# ─── INIT ─────────────────────────────────────────────────────────────────────

try:
    init_db()
except Exception as e:
    print("DB init error: " + str(e))

threading.Thread(target=auto_scraper, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
