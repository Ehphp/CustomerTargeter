# CustumerTarget — Architettura e Guida al Progetto

## Obiettivo

CustumerTarget integra dati geospaziali pubblici (OSM/Overpass) e li normalizza in un modello consultabile via API e UI per analisi e scoring di attività/luoghi. Il focus è: importare POI e strade, calcolare contesti/metriche e offrirli a un front‑end veloce per esplorazione.

## Panoramica dell'architettura

- Database (Docker): PostgreSQL + PostGIS per storage e calcolo geospaziale.
- ETL (Python + SQL):
  - `etl/osm_overpass.py`: estrazione POI/strade da Overpass, upsert su tabelle OSM.
  - `etl/sql_blocks/*.sql`: normalizzazione, contesto e scoring in step idempotenti.
  - `etl/run_all.py`: orchestration runner per gli script SQL.
- API (FastAPI): `api/main.py` espone endpoint di health, contatori e ricerca/sort su punteggi.
- UI (React + Vite): `ui/` interroga l’API e visualizza risultati, filtri e breakdown degli score.
- Adminer (Docker): console DB per ispezione rapida.

## Flusso dati end‑to‑end

1) Sorgente OSM (Overpass)
- `etl/osm_overpass.py` invia query ai mirror Overpass (con retry/backoff).
- Inserisce/aggiorna: `osm_business` (POI con `location`, `category/subtype`, contatti) e `osm_roads` (strade principali con geometrie).

2) Normalizzazione e arricchimento (SQL)
- `build_places_raw.sql`: popola `places_raw` a partire da `osm_business` (unificazione campi base).
- `normalize_osm.sql`: scrive in `places_clean` (campi puliti/usabili, flags, location) con upsert.
- `context_density.sql`: densità entro 500m (conteggio vicini) in `place_context`.
- `context_poi.sql`: distanza media da POI (centralità) in `place_context`.
- `scoring_popularity.sql`: calcolo popularità (visibilità su strada + centralità) -> `company_scores`.
- `scoring_access.sql`: accessibilità (orari + contatti) -> `company_scores`.
- `scoring_total.sql`: somma pesata nei `total_score`.

3) Servizio API
- `GET /health`: ping DB.
- `GET /counts`: contatori principali (tabelle chiave) per stato pipeline.
- `GET /places`: filtro opzionale per `city`, `category`, con `min_score` e `limit`. Join su `company_scores` per ordinare per punteggio.

4) UI
- `ui/src/App.tsx` (React):
  - Configurazione `API_DEFAULT` (`http://127.0.0.1:8000`).
  - Pannello filtri, badge di stato API, tabella risultati con breakdown (popularity/territory/accessibility).
  - Chiama `GET /health`, `GET /counts`, `GET /places`.

## Tecnologie e motivazioni

- PostgreSQL + PostGIS: modellazione e query geospaziali (buffer, distanza, within) direttamente in SQL; robusto e standard.
- Overpass API (OSM): dati POI e viabilità aggiornati e open; uso di mirror + retry per resilienza.
- Python (requests, psycopg2, dotenv): semplicità di scripting, connessione DB e config `.env` coerente tra servizi.
- FastAPI + Uvicorn: API performante e tipizzata, autoreload in dev, CORS configurato per UI locale.
- React + Vite + Tailwind: UI snella per esplorazione rapida; vite dev server e HMR per produttività.
- Docker Compose: isola lo strato dati (PostGIS) e Adminer; riproducibilità locale.

## Schema dati (estratto)

- `osm_business(osm_id, name, category, subtype, tags, phone, website, opening_hours, location)`
- `osm_roads(osm_id, highway, name, geom)`
- `places_raw(...)` -> staging unificato dei luoghi
- `places_clean(...)` -> versione normalizzata per consumo applicativo
- `place_context(density_500m, distance_poi_avg)` -> feature di contesto
- `company_scores(popularity_score, territory_score, accessibility_score, total_score)`

Le definizioni sono inizializzate da `sql/shema.sql` (PostGIS extension inclusa).

## Come avviare (dev)

1) Database + Adminer
- `docker-compose up -d` dalla root (crea PostGIS + Adminer su `:8080`).
- Variabili `.env` alla root definiscono utente/DB/host/porta e chiavi esterne (es. Google).

2) API
- Da `api/`: attiva venv Python del progetto `etl` o la tua venv attuale.
- Avvio: `uvicorn main:app --reload --host 0.0.0.0 --port 8000`.

3) UI
- Da `ui/`: `npm run dev` (o `pnpm dev`).
- L’UI punta per default a `http://127.0.0.1:8000` (modificabile da input in pagina).

4) ETL
- Overpass: `python etl/osm_overpass.py` (popola `osm_business` e `osm_roads`).
- Pipeline SQL: `python etl/run_all.py` (esegue `etl/sql_blocks` in ordine).

Ordine tipico in locale: avvia DB → esegui `osm_overpass.py` → esegui `run_all.py` → avvia API → avvia UI → esplora risultati.

Nota: nel repository non esiste uno script chiamato "run_overpass"; lo script di estrazione è `etl/osm_overpass.py`.

## Considerazioni su qualità e performance

- Resilienza Overpass: uso mirror e status retry (429/5xx), tile del bbox per ridurre timeout e duplicati.
- Idempotenza SQL: step con `ON CONFLICT` per aggiornamenti incrementali.
- Geospaziale nel DB: calcoli (buffer/distanza/dwithin) eseguiti in PostGIS per efficienza.
- CORS in dev: regex `http://(localhost|127.0.0.1):<porta>` per semplicità di sviluppo UI.

## Estensioni future

- Scheduling ETL (es. cron/worker), logging strutturato e metriche.
- Inserimento di ulteriori sorgenti (es. Google Places via `etl/google_places.py`).
- Enrichment anagrafico (collegamento a `istat_comuni`, popolazione, turismo) e mappe in UI.
- Contenitori separati per API/UI in compose per sviluppo full‑docker.

## Troubleshooting rapido

- DB vuoto: verifica `sql/shema.sql` applicato (viene caricato automaticamente da Docker al primo avvio) e `.env`.
- Overpass lento: riduci bbox o aumenta splitting in `split_bbox`, verifica mirror e backoff.
- UI non vede l’API: controlla CORS e `API base URL` nella UI; verifica `GET /health`.

