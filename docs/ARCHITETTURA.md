# CustomerTarget - Architettura 2025

## Obiettivo
CustomerTarget raccoglie i punti vendita italiani partendo da Google Places e li arricchisce con un micro-servizio LLM per produrre le **metriche Brello**: densita settoriale, distribuzione geografica, dimensione stimata, banda budget pubblicitario, affinita al mezzo ombrello, presenza digitale e livello di fiducia sui dati.

## Panoramica componenti
- **Database (PostgreSQL + PostGIS)**: contiene `places_raw`, `places_clean`, `place_sector_density`, `business_facts`, `business_metrics`, `brello_stations`, `geo_zones`, `enrichment_request` e `enrichment_response`.
- **ETL base (`etl/`)**:
  - `google_places.py`: usa le API Text Search + Details di Google Places per popolare `places_raw`.
  - `sql_blocks/normalize_places.sql`: pulisce i record di Google (`places_clean`).
  - `sql_blocks/context_sector_density.sql`: calcola la densita di competitor (`place_sector_density`).
  - `run_all.py`: orchestration runner che esegue gli step SQL in ordine.
- **LLM Enrichment (`etl/enrich/`)**: seleziona i business senza fatti aggiornati, costruisce il prompt (nome, categoria, coordinate, rating, flags Google), invia la richiesta al provider LLM e popola `business_facts` + `enrichment_*` con risposta raw/parse.
- **Feature Builder (`feature_builder/build_metrics.py`)**: unisce `places_clean`, `place_sector_density`, `business_facts`, `brello_stations` e `geo_zones` per calcolare le metriche Brello e salvarle in `business_metrics`.
- **API (`api/main.py`)**: FastAPI espone `/health`, `/counts`, `/places` e gli endpoint per avviare gli job (`/etl/google_places/start`, `/etl/pipeline/start`, `/automation/auto_refresh/start`). Il modulo coordina i processi come thread dedicati e registra log/exit code.
- **UI (`ui/src/App.tsx`)**: dashboard React/Vite che consente di lanciare l'import Google, la pipeline SQL, l'auto-refresh e di esplorare le metriche Brello con filtri avanzati.

## Data flow
1. **Import Google Places**  
   `python -m etl.google_places --location "Citta" --queries ristorante bar ...` scrive/upserta `places_raw` con coordinate, rating, telefono, sito, tipi, orari.
2. **Normalizzazione SQL**  
   `python etl/run_all.py` esegue:
   - `normalize_places.sql` per ripulire indirizzi/citta e incrociare `istat_comuni` (censimento).
   - `context_sector_density.sql` per misurare densita e numero concorrenti entro il raggio configurato.
3. **LLM Enrichment**  
   `python -m etl.enrich.run_enrichment --limit 100` costruisce prompt con dati Google, avvia l'LLM (OpenAI o Perplexity) e salva output in `business_facts` + `enrichment_request/response`.
4. **Feature Builder**  
   `python -m feature_builder.build_metrics` calcola metriche Brello (affinita, budget, digitale, geo label) e scrive `business_metrics` con `updated_at` aggiornato.
5. **API & UI**  
   - `uvicorn api.main:app --reload` espone gli endpoint.
   - `npm run dev` avvia la UI per filtrare i business, monitorare gli job e vedere indicatori.
6. **Automazione opzionale**  
   `python -m automation.auto_refresh` controlla staleness di `business_facts`/`business_metrics` e, se necessario, lancia enrichment + feature builder in batch.

## Schema dati (estratto)
- `places_raw(place_id, name, formatted_address, phone, website, types, rating, user_ratings_total, opening_hours_json, location, source_ts)`
- `places_clean(place_id, name, address, city, category, rating, user_ratings_total, hours_weekly, has_phone, has_website, location, istat_code)`
- `place_sector_density(place_id, sector, neighbor_count, density_score, computed_at)`
- `business_facts(business_id, size_class, is_chain, website_url, social, marketing_attitude, umbrella_affinity, ad_budget_band, budget_source, confidence, provenance, updated_at, source_provider, source_model)`
- `business_metrics(business_id, sector_density_neighbors, sector_density_score, geo_distribution_label, geo_distribution_source, size_class, is_chain, ad_budget_band, umbrella_affinity, digital_presence, digital_presence_confidence, marketing_attitude, facts_confidence, updated_at)`
- `enrichment_request(request_id, business_id, provider, input_hash, input_payload, status, created_at, started_at, finished_at, error)`
- `enrichment_response(response_id, request_id, model, raw_response, parsed_response, prompt_tokens, completion_tokens, cost_cents, created_at)`
- Tabelle di supporto: `brello_stations` (coordinate stazioni), `geo_zones` (poligoni geospaziali), `istat_comuni` (comuni italiani con geometria e popolazione).

## Runbook operativo
1. **Avviare il database**  
   `docker-compose up -d` (Postgres/PostGIS + Adminer).
2. **Import Google Places**  
   Configurare `GOOGLE_PLACES_API_KEY` nel `.env`, poi lanciare `python -m etl.google_places ...` oppure usare la UI (sezione *Google Places Import*).
3. **Pipeline SQL**  
   `python etl/run_all.py` per aggiornare `places_clean` e `place_sector_density`.
4. **Enrichment LLM**  
   `python -m etl.enrich.run_enrichment --limit 100` (rispettare TTL di default o usare `--force`).
5. **Feature builder**  
   `python -m feature_builder.build_metrics` per scrivere/aggiornare `business_metrics`.
6. **API + UI**  
   `uvicorn api.main:app --reload` e `npm run dev`; configurare la base URL nella UI se necessario.
7. **Automazione (facoltativa)**  
   `python -m automation.auto_refresh --dry-run` per verificare, poi senza flag per eseguire enrichment+metriche quando servono.

## Troubleshooting rapido
- **Nessun dato in UI**: controllare che `business_metrics` contenga righe (`SELECT COUNT(*) FROM business_metrics`). Se vuoto, eseguire pipeline + enrichment + feature builder.
- **Enrichment in errore**: verificare `enrichment_request` (status `error`) e i log del job in API/UI. Controllare `LLM_PROVIDER` e chiavi nel `.env`.
- **Geo label sempre altro**: assicurarsi di avere `brello_stations` e `geo_zones` popolati; rilanciare `feature_builder` dopo eventuali aggiornamenti.
- **Import Google lento/costante**: ridurre numero di query o raggio; usare `--limit`, `--sleep-seconds`, oppure impostare lat/lng manualmente.
- **CORS o UI offline**: aggiornare `API_CORS_ORIGINS` e verificare che l'API sia in esecuzione (`/health`).

Questa architettura rimuove del tutto la dipendenza da OSM/Overpass: l'unica sorgente primaria di POI e Google Places e l'intero stack (LLM, feature builder, UI) lavora sulle tabelle `places_*` alimentate da quell'import.
