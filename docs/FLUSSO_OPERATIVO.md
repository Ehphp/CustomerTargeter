# Flusso Operativo End-to-End

Questo documento riassume l'intero ciclo dati di CustomerTarget, dalla raccolta tramite Google Places fino alla visualizzazione delle metriche Brello nella UI.

## 0. Prerequisiti
- **Ambiente Python**: attiva `.venv\Scripts\activate` (oppure crea una nuova venv e installa `requirements/`).
- **Variabili `.env`** (root del repo):
  - Parametri Postgres (`POSTGRES_*`).
  - `GOOGLE_PLACES_API_KEY` per la sorgente principale dei punti vendita.
  - Parametri prompt: `ENRICHMENT_PROMPT_VERSION` (default 2) e `ENRICHMENT_SEARCH_RADIUS_M` (default 200 m).
  - Credenziali LLM: `LLM_PROVIDER` + `OPENAI_API_KEY` oppure `PERPLEXITY_API_KEY`, opzionalmente `LLM_MODEL`.

## 1. Ingest Google Places
```powershell
(.venv) python -m etl.google_places --location "Alatri, Italia" --queries ristorante bar negozio --limit 200
```
- Esegue la Text Search API di Google Places e upserta i risultati in `places_raw`.
- Puoi indicare le query da CLI (`--queries`) oppure da file (`--queries-file`).
- `--location "Nome città"` usa il geocoding Google per ottenere lat/lon e bounding box; volendo puoi ancora passare manualmente `--lat`/`--lng`.
- `--radius` è facoltativo: se omesso viene usato il raggio stimato dal geocoding (minimo 3000 m); se presente, il codice prende il max tra il tuo valore e quello calcolato. `--limit` aiuta a controllare i costi.

## 2. Pipeline SQL di normalizzazione
```powershell
(.venv) python etl/run_all.py
```
Esegue in sequenza gli step di `etl/sql_blocks/`:
1. `00_setup_brello.sql` → garantisce la presenza delle tabelle di supporto.
2. `normalize_places.sql` → normalizza `places_raw` in `places_clean`, stimando la città con `formatted_address` + `istat_comuni` e impostando i flag phone/website.
3. `context_sector_density.sql` → calcola `place_sector_density` (conteggio vicini e score densità).

> Output atteso dopo questa fase (verificabile da UI > counts o via SQL):  
> `places_raw` > 0, `places_clean` > 0, `place_sector_density` > 0.

## 3. Enrichment LLM
```powershell
(.venv) python -m etl.enrich.run_enrichment --limit 100
```
- Seleziona i business senza fatti recenti (`business_facts` vuoto o `updated_at` oltre TTL).
- Costruisce il prompt con `build_prompt`, includendo nome, categoria, coordinate, bounding box (~200 m) e altri metadati.
- **Importante**: le istruzioni chiedono all'LLM di restituire solo dati fattuali. I campi metrici (`size_class`, `is_chain`, `marketing_attitude`, `umbrella_affinity`, `ad_budget_band`, `confidence`) devono restare `null`: vengono calcolati internamente tramite le regole deterministiche.
- `ENRICHMENT_PROMPT_VERSION` (default 2) controlla il versioning: aumenta il valore quando modifichi prompt o logica per forzare un nuovo enrichment sui record esistenti.
- Il client LLM (OpenAI/Perplexity) è scelto da `load_client_from_env`. Le risposte valide sono salvate in `business_facts` e `enrichment_response`.

Controlli consigliati:
- UI > badge `business_facts` oppure `SELECT COUNT(*) FROM business_facts`.
- `SELECT status, COUNT(*) FROM enrichment_request GROUP BY status` per eventuali errori.

## 4. Feature Builder (metriche Brello)
```powershell
(.venv) python -m feature_builder.build_metrics
```
- Aggrega `places_clean`, `place_sector_density`, `business_facts`, `brello_stations` e le eventuali `geo_zones`.
- Applica le funzioni di `common/business_rules.py` per stimare `size_class`, `is_chain`, `ad_budget_band`, `umbrella_affinity`, `marketing_attitude` e `confidence` in modo deterministico (se l'LLM li ha lasciati `null`).
- Calcola `digital_presence`, `geo_distribution_label` e upserta tutto in `business_metrics`.

## 4-bis. Automazione Enrichment + Metriche
```powershell
(.venv) python -m automation.auto_refresh [--dry-run]
```
- Analizza il DB: se ci sono record oltre TTL o mancanti, lancia enrichment e `build_metrics`.
- Parametri utili: `--force-enrichment`, `--enrich-limit`, `--max-enrich-batches`, `--metrics-each-batch`, `--always-run-metrics`.
- Se il log riporta “data is within TTL”, usa `--force-enrichment` (o abbassa `ENRICHMENT_TTL_DAYS` / `--enrich-ttl-days`) per forzare un nuovo giro; `--always-run-metrics`/`--metrics-each-batch` obbligano il ricalcolo delle metriche.

## 5. API FastAPI
```powershell
(.venv) uvicorn api.main:app --reload
```
Endpoint principali:
- `GET /health` → check connessione DB.
- `GET /counts` → riepilogo tabelle chiave (usato per i badge UI).
- `POST /etl/pipeline/start`, `POST /automation/auto_refresh/start` → avviano i job ETL/auto-refresh in thread.
- `GET /places` → join `places_clean` + `business_metrics` + `business_facts` con filtri su città/categoria/geo/size/budget/catena e soglie minime.

## 6. UI Dashboard
```bash
cd ui
npm install   # solo la prima volta
npm run dev
```
Funzionalità principali:
- Campo `API base URL` (default `http://127.0.0.1:8000`).
- Filtri avanzati sui campi `/places`.
- Bottoni `Run Pipeline`, `Run Auto Refresh`, `Refresh Status`.
- Tabella con badge Affinità / Digitale / Densità e metadati business.

## 7. Sequenza consigliata per un load completo
1. `docker-compose up -d`
2. `python -m etl.google_places --location "Città..." --queries ...`
3. `python etl/run_all.py`
4. (Facoltativo ma consigliato) verifica counts via UI o SQL.
5. `python -m etl.enrich.run_enrichment --limit 100`
6. `python -m feature_builder.build_metrics`
7. Avvia `uvicorn` e `npm run dev`, poi usa “Search” in UI.

## 8. Verifiche e troubleshooting rapidi
- `SELECT COUNT(*) FROM business_facts` > 0 e `business_metrics` > 0 dopo gli step 3-4.
- `SELECT status, COUNT(*) FROM enrichment_request GROUP BY status` → eventuali fallimenti con relativo messaggio.
- `SELECT COUNT(*) FROM place_sector_density WHERE density_score IS NULL` → normalizzazione ok?
- Nessun dato in UI? Spesso `business_metrics` è vuota o l’API è offline (badge rosso). Rilancia step 4 o controlla `/health`.
- Problemi di city/address nel prompt? Assicurati che `normalize_places.sql` sia stato eseguito e che `istat_comuni` sia presente.

## 9. Componenti e tabelle (produttori → consumatori)
- `etl/google_places.py` → `places_raw`.
- `etl/sql_blocks/normalize_places.sql` → `places_clean`.
- `etl/sql_blocks/context_sector_density.sql` → `place_sector_density`.
- `etl/enrich/run_enrichment.py` → `enrichment_request`, `enrichment_response`, `business_facts`.
- `feature_builder/build_metrics.py` → `business_metrics`.
- `api/main.py` → `/counts`, `/places`, job ETL/auto-refresh.
- `ui/src/App.tsx` → interfaccia utente, filtri e controlli.

Seguendo questo flusso, la UI mostrerà le metriche Brello aggiornate per ogni attività rilevata tramite Google Places.
