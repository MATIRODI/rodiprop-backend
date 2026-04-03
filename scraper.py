import requests
from bs4 import BeautifulSoup
import json
import time
import random
from datetime import datetime

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

def pausa():
    time.sleep(random.uniform(1.5, 3.5))


def scrape_mercadolibre(paginas=5):
    propiedades = []
    categorias = [
        ("venta", "https://inmuebles.mercadolibre.com.ar/venta/cordoba/_Desde_{offset}_DisplayType_G"),
        ("alquiler", "https://inmuebles.mercadolibre.com.ar/alquiler/cordoba/_Desde_{offset}_DisplayType_G"),
    ]
    for operacion, url_template in categorias:
        for pagina in range(paginas):
            offset = pagina * 48 + 1
            url = url_template.format(offset=offset)
            try:
                resp = requests.get(url, headers=get_headers(), timeout=20)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                cards = soup.select(".ui-search-layout__item")
                for card in cards:
                    try:
                        prop = {}
                        titulo = card.select_one(".poly-component__title")
                        prop["titulo"] = titulo.text.strip() if titulo else ""
                        precio = card.select_one(".andes-money-amount__fraction")
                        moneda = card.select_one(".andes-money-amount__currency-symbol")
                        prop["precio"] = precio.text.strip().replace(".", "").replace(",", "") if precio else ""
                        prop["moneda"] = moneda.text.strip() if moneda else "USD"
                        ubicacion = card.select_one(".poly-component__location")
                        prop["ubicacion"] = ubicacion.text.strip() if ubicacion else ""
                        attrs = card.select(".poly-attributes-list__item")
                        prop["atributos"] = [a.text.strip() for a in attrs]
                        link = card.select_one("a.poly-component__title")
                        prop["url"] = link["href"] if link else ""
                        img = card.select_one("img.poly-component__picture")
                        prop["imagen"] = (img.get("data-src") or img.get("src", "")) if img else ""
                        prop["fuente"] = "MercadoLibre"
                        prop["operacion"] = operacion
                        prop["fecha_scrape"] = datetime.now().isoformat()
                        if prop["titulo"] and prop["precio"]:
                            propiedades.append(prop)
                    except:
                        continue
                print(f"✅ ML {operacion} pág {pagina+1}: {len(cards)} props")
                pausa()
            except Exception as e:
                print(f"❌ ML error: {e}")
    return propiedades


def scrape_zonaprop(paginas=5):
    propiedades = []
    categorias = [
        ("venta", "https://www.zonaprop.com.ar/inmuebles-venta-cordoba-capital-pagina-{pagina}.html"),
        ("alquiler", "https://www.zonaprop.com.ar/inmuebles-alquiler-cordoba-capital-pagina-{pagina}.html"),
    ]
    session = requests.Session()
    try:
        session.get("https://www.zonaprop.com.ar", headers=get_headers(), timeout=15)
        pausa()
    except:
        pass
    for operacion, url_template in categorias:
        for pagina in range(1, paginas + 1):
            url = url_template.format(pagina=pagina)
            try:
                resp = session.get(url, headers=get_headers(), timeout=20)
                if resp.status_code in [403, 429]:
                    print(f"ZP bloqueado pág {pagina}")
                    break
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                cards = soup.select("div[data-id]") or soup.select(".postingCardLayout") or soup.select("[class*='postingCard']")
                for card in cards:
                    try:
                        prop = {}
                        precio_el = card.select_one("[data-price]") or card.select_one(".firstPrice") or card.select_one("[class*='price']")
                        prop["precio"] = (precio_el.get("data-price") or precio_el.text.strip().replace(".", "").replace("$", "").strip()) if precio_el else ""
                        prop["moneda"] = "USD"
                        titulo = card.select_one(".postingCardTitle") or card.select_one("[class*='Title']") or card.select_one("h2")
                        prop["titulo"] = titulo.text.strip() if titulo else ""
                        ubicacion = card.select_one(".postingCardLocation") or card.select_one("[class*='Location']")
                        prop["ubicacion"] = ubicacion.text.strip() if ubicacion else ""
                        attrs = card.select(".postingCardAttribute") or card.select("[class*='Attribute']")
                        prop["atributos"] = [a.text.strip() for a in attrs]
                        link = card.select_one("a[href*='/propiedades/']") or card.select_one("a")
                        if link and link.get("href"):
                            href = link["href"]
                            prop["url"] = href if href.startswith("http") else "https://www.zonaprop.com.ar" + href
                        else:
                            prop["url"] = ""
                        img = card.select_one("img")
                        prop["imagen"] = (img.get("data-src") or img.get("src", "")) if img else ""
                        prop["fuente"] = "ZonaProp"
                        prop["operacion"] = operacion
                        prop["fecha_scrape"] = datetime.now().isoformat()
                        if prop["titulo"] or prop["precio"]:
                            propiedades.append(prop)
                    except:
                        continue
                print(f"✅ ZP {operacion} pág {pagina}: {len(cards)} props")
                pausa()
            except Exception as e:
                print(f"❌ ZP error: {e}")
    return propiedades


def scrape_argenprop(paginas=5):
    propiedades = []
    categorias = [
        ("venta", "https://www.argenprop.com/departamento-y-casa-y-ph-en-venta-en-cordoba--pagina-{pagina}"),
        ("alquiler", "https://www.argenprop.com/departamento-y-casa-y-ph-en-alquiler-en-cordoba--pagina-{pagina}"),
    ]
    session = requests.Session()
    try:
        session.get("https://www.argenprop.com", headers=get_headers(), timeout=15)
        pausa()
    except:
        pass
    for operacion, url_template in categorias:
        for pagina in range(1, paginas + 1):
            url = url_template.format(pagina=pagina)
            try:
                resp = session.get(url, headers=get_headers(), timeout=20)
                if resp.status_code in [403, 429]:
                    print(f"AP bloqueado pág {pagina}")
                    break
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                cards = soup.select(".listing__item") or soup.select("[class*='listing-item']") or soup.select("article")
                for card in cards:
                    try:
                        prop = {}
                        precio_el = card.select_one(".card__price") or card.select_one("[class*='price']")
                        prop["precio"] = precio_el.text.strip().replace(".", "").replace("$", "").replace("USD", "").strip() if precio_el else ""
                        prop["moneda"] = "USD"
                        titulo = card.select_one(".card__title") or card.select_one("h2") or card.select_one("[class*='title']")
                        prop["titulo"] = titulo.text.strip() if titulo else ""
                        ubicacion = card.select_one(".card__address") or card.select_one("[class*='address']")
                        prop["ubicacion"] = ubicacion.text.strip() if ubicacion else ""
                        attrs = card.select(".card__common-data li") or card.select("[class*='feature']")
                        prop["atributos"] = [a.text.strip() for a in attrs]
                        link = card.select_one("a[href]")
                        if link:
                            href = link["href"]
                            prop["url"] = href if href.startswith("http") else "https://www.argenprop.com" + href
                        else:
                            prop["url"] = ""
                        img = card.select_one("img")
                        prop["imagen"] = (img.get("data-src") or img.get("src", "")) if img else ""
                        prop["fuente"] = "ArgenProp"
                        prop["operacion"] = operacion
                        prop["fecha_scrape"] = datetime.now().isoformat()
                        if prop["titulo"] or prop["precio"]:
                            propiedades.append(prop)
                    except:
                        continue
                print(f"✅ AP {operacion} pág {pagina}: {len(cards)} props")
                pausa()
            except Exception as e:
                print(f"❌ AP error: {e}")
    return propiedades


def guardar_json(propiedades, archivo="propiedades.json"):
    vistas = set()
    unicas = []
    for p in propiedades:
        key = p.get("url") or p.get("titulo", "") + p.get("precio", "")
        if key and key not in vistas:
            vistas.add(key)
            unicas.append(p)
    with open(archivo, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False, indent=2)
    print(f"\n💾 Guardadas {len(unicas)} propiedades únicas en {archivo}")
    return unicas


if __name__ == "__main__":
    print("🔍 RodiProp Scraper v2")
    print("=" * 50)
    props_ml = scrape_mercadolibre(paginas=5)
    print(f"ML: {len(props_ml)}")
    props_zp = scrape_zonaprop(paginas=5)
    print(f"ZP: {len(props_zp)}")
    props_ap = scrape_argenprop(paginas=5)
    print(f"AP: {len(props_ap)}")
    todas = props_ml + props_zp + props_ap
    guardar_json(todas)
