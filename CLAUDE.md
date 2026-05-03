# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RodiProp is a real estate listing aggregator for Córdoba, Argentina. It scrapes property listings from multiple sources, stores them in PostgreSQL, and sends WhatsApp alerts to registered users when new matching listings appear. It includes a freemium subscription model via Mercado Pago.

## Running Locally

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run development server
python app.py
# Listens on PORT env var, defaults to 5000

# Run the WhatsApp microservice separately
cd whatsapp-service
npm install
node index.js
# Listens on port 3000
```

There is no test suite. Syntax validation is done in CI via `ast.parse`.

## Deployment Flow

**Never push directly to `main`.** The deploy pipeline is:

1. Push to the `deploy` branch
2. GitHub Actions (`.github/workflows/deploy.yml`) validates Python syntax and checks for required functions/routes in `app.py`
3. On success, the workflow auto-merges `deploy` → `main`
4. Railway detects the push to `main` and redeploys automatically

Railway runs: `gunicorn app:app --bind 0.0.0.0:$PORT` (see `Procfile`).

## Architecture

The backend is a single-file Flask app (`app.py`) — all logic lives there, not split into modules. `scraper.py` is a standalone prototype/dev script not used in production.

**Two deployed services on Railway:**
- Python/Flask backend (`app.py`) — scraping, DB, API, payments
- Node.js WhatsApp microservice (`whatsapp-service/`) — manages a WhatsApp Web session via `whatsapp-web.js` + Puppeteer; exposes `POST /send`, `GET /status`, `GET /qr`

The Flask app calls the WhatsApp service via HTTP (`WA_SERVICE_URL` env var). It never sends WhatsApp messages directly.

**Database** is PostgreSQL accessed via raw `pg8000` (no ORM). Tables:
- `propiedades` — scraped listings (URL is unique key)
- `usuarios` — alert subscribers with filter preferences and plan
- `alertas_enviadas` — deduplication log (user_id + property URL)
- `pagos` — Mercado Pago payment records

`init_db()` runs at startup and creates tables if missing. Schema migrations are done inline with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.

**Scraping** runs in a background daemon thread (`auto_scraper`) every 2 hours. Three scrapers: `scrape_ml` (MercadoLibre), `scrape_ap` (ArgenProp), `scrape_lavoz` (La Voz Clasificados). All use `requests` + `BeautifulSoup`. Properties are upserted by URL.

**Alerts** (`chequear_alertas`) runs after each scrape. Users on the `gratis` plan get 7 free alerts total; on alert 7 they receive an upgrade prompt. `premium` and `inversor` plans get unlimited alerts.

**Payments** use Mercado Pago — supports both one-time checkout (`/checkout/preferences`) and recurring subscriptions (`/preapproval`). The `external_reference` field encodes `{usuario_id}_{plan}`, parsed in the webhook handler.

## Key Environment Variables

| Variable | Purpose |
|---|---|
| `PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGPORT` | PostgreSQL connection |
| `WA_SERVICE_URL` | URL of the WhatsApp Node.js microservice |
| `MP_ACCESS_TOKEN`, `MP_CLIENT_SECRET` | Mercado Pago credentials |
| `BACKEND_URL` | This service's public URL (used in MP callback URLs) |
| `FRONTEND_URL` | Frontend URL for redirects and paywall links |
| `MI_WHATSAPP` | Owner's WhatsApp number for payment notifications |
| `ADMIN_PASSWORD` | Password for `/admin` panel |

## API Endpoints

- `GET /` — health check with DB count
- `GET /api/propiedades` — list properties; query params: `zona`, `tipo`, `operacion`, `fuente`, `limit`
- `GET /api/stats` — property counts by source
- `POST /api/scraper/ejecutar` — trigger scrape in background
- `POST /api/alertas/test` — trigger alert check in background
- `POST /api/usuarios/registro` — register/update alert subscription
- `GET /api/usuarios/lista` — all users
- `GET /api/usuarios/stats` — user counts by plan
- `POST /api/pagos/crear` — create MP checkout or subscription (`tipo`: `checkout`|`suscripcion`)
- `POST /api/pagos/webhook` — Mercado Pago IPN handler
- `GET /api/pagos/lista` — payment history
- `GET /admin` — serves `static/admin.html`
- `POST /api/admin/auth` — password check for admin panel

## Subscription Plans

Defined in the `PLANES` dict in `app.py`:
- `gratis` — default, 7 alerts lifetime
- `premium` — ARS 4,999/month, unlimited alerts
- `inversor` — ARS 15,000/month, everything + analytics

Plan is stored on `usuarios.plan`. It is updated to the paid plan when a Mercado Pago payment is `approved`.
