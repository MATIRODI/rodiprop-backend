import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-AR,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def scrape_mercadolibre(zona="cordoba", paginas=3):
    """Scraper de MercadoLibre Inmuebles para Córdoba"""
    propiedades = []
    
    for pagina in range(paginas):
        offset = pagina * 48
        url = f"https://inmuebles.mercadolibre.com.ar/venta/_Desde_{offset + 1}_DisplayType_G"
        if zona == "cordoba":
            url = f"https://inmuebles.mercadolibre.com.ar/venta/cordoba/_Desde_{offset + 1}_DisplayType_G"
        
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                print(f"Error {resp.status_code} en página {pagina + 1}")
                continue
                
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select(".ui-search-layout__item")
            
            for card in cards:
                try:
                    prop = {}
                    
                    # Título
                    titulo = card.select_one(".poly-component__title")
                    prop["titulo"] = titulo.text.strip() if titulo else ""
                    
                    # Precio
                    precio = card.select_one(".andes-money-amount__fraction")
                    moneda = card.select_one(".andes-money-amount__currency-symbol")
                    prop["precio"] = precio.text.strip().replace(".", "") if precio else ""
                    prop["moneda"] = moneda.text.strip() if moneda else "USD"
                    
                    # Ubicación
                    ubicacion = card.select_one(".poly-component__location")
                    prop["ubicacion"] = ubicacion.text.strip() if ubicacion else ""
                    
                    # Atributos (ambientes, m2, etc)
                    attrs = card.select(".poly-attributes-list__item")
                    prop["atributos"] = [a.text.strip() for a in attrs]
                    
                    # Link
                    link = card.select_one("a.poly-component__title")
                    prop["url"] = link["href"] if link else ""
                    
                    # Imagen
                    img = card.select_one("img.poly-component__picture")
                    prop["imagen"] = img.get("src") or img.get("data-src", "") if img else ""
                    
                    # Fuente y fecha
                    prop["fuente"] = "MercadoLibre"
                    prop["fecha_scrape"] = datetime.now().isoformat()
                    
                    if prop["titulo"] and prop["precio"]:
                        propiedades.append(prop)
                        
                except Exception as e:
                    continue
            
            print(f"✅ Página {pagina + 1}: {len(cards)} propiedades encontradas")
            time.sleep(2)  # Pausa para no ser bloqueado
            
        except Exception as e:
            print(f"❌ Error en página {pagina + 1}: {e}")
    
    return propiedades


def scrape_zonaprop(zona="cordoba", paginas=3):
    """Scraper de ZonaProp para Córdoba"""
    propiedades = []
    
    for pagina in range(1, paginas + 1):
        url = f"https://www.zonaprop.com.ar/inmuebles-venta-cordoba-pagina-{pagina}.html"
        
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                print(f"ZonaProp error {resp.status_code} en página {pagina}")
                continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select("[data-id]")
            
            for card in cards:
                try:
                    prop = {}
                    
                    # Precio
                    precio_el = card.select_one("[data-price]")
                    prop["precio"] = precio_el["data-price"] if precio_el else ""
                    prop["moneda"] = "USD"
                    
                    # Título/tipo
                    titulo = card.select_one(".postingCardTitle")
                    prop["titulo"] = titulo.text.strip() if titulo else ""
                    
                    # Ubicación
                    ubicacion = card.select_one(".postingCardLocation")
                    prop["ubicacion"] = ubicacion.text.strip() if ubicacion else ""
                    
                    # Atributos
                    attrs = card.select(".postingCardAttribute")
                    prop["atributos"] = [a.text.strip() for a in attrs]
                    
                    # Link
                    link = card.select_one("a.go-to-posting")
                    prop["url"] = "https://www.zonaprop.com.ar" + link["href"] if link else ""
                    
                    # Imagen
                    img = card.select_one("img")
                    prop["imagen"] = img.get("src", "") if img else ""
                    
                    prop["fuente"] = "ZonaProp"
                    prop["fecha_scrape"] = datetime.now().isoformat()
                    
                    if prop["titulo"] or prop["precio"]:
                        propiedades.append(prop)
                        
                except Exception as e:
                    continue
            
            print(f"✅ ZonaProp página {pagina}: {len(cards)} propiedades")
            time.sleep(2)
            
        except Exception as e:
            print(f"❌ ZonaProp error: {e}")
    
    return propiedades


def guardar_json(propiedades, archivo="propiedades.json"):
    with open(archivo, "w", encoding="utf-8") as f:
        json.dump(propiedades, f, ensure_ascii=False, indent=2)
    print(f"\n💾 Guardadas {len(propiedades)} propiedades en {archivo}")


if __name__ == "__main__":
    print("🔍 Iniciando scraper RodiProp...")
    print("=" * 50)
    
    print("\n📦 MercadoLibre Córdoba:")
    props_ml = scrape_mercadolibre(zona="cordoba", paginas=2)
    
    print("\n📦 ZonaProp Córdoba:")
    props_zp = scrape_zonaprop(zona="cordoba", paginas=2)
    
    todas = props_ml + props_zp
    print(f"\n✅ TOTAL: {len(todas)} propiedades obtenidas")
    
    guardar_json(todas)
