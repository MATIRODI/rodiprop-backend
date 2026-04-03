# RodiProp Backend 🏠

API de scraping de propiedades para Córdoba.

## Deploy en Railway
1. Crear cuenta en railway.app
2. New Project → Deploy from GitHub
3. Conectar este repositorio
4. Deploy automático ✅

## Endpoints
- GET /api/propiedades — Lista propiedades con filtros
- GET /api/stats — Estadísticas generales  
- POST /api/scraper/ejecutar — Forzar actualización

## Filtros disponibles
- ?zona=nueva cordoba
- ?tipo=departamento
- ?precio_min=50000&precio_max=150000
- ?fuente=MercadoLibre
- ?limit=20
