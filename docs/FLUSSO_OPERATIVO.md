# Flusso Operativo End-to-End

Questo documento riassume l'intero ciclo dati di CustomerTarget, dalla raccolta OSM fino alla visualizzazione delle metriche Brellé in UI. Mantiene le istruzioni in ordine cronologico e richiama i componenti coinvolti a ogni passo.

## 0. Prerequisiti
- **Ambiente Python**: attiva `.\.venv\Scripts\activate` (oppure crea una nuova venv e installa `requirements/`).
- **Variabili `.env`** (root del repo):
  - Parametri Postgres (`POSTGRES_*`).
  - Credenziali Overpass/Google se disponibili (`GOOGLE_PLACES_API_KEY`).
  - Per l’arricchimento: `LLM_PROVIDER` + `OPENAI_API_KEY` oppure `PERPLEXITY_API_KEY`, opzionalmente `LLM_MODEL`.
- **Container DB**: `docker-compose up -d` avvia PostGIS e Adminer (`docs/ARCHITETTURA.md` descrive lo schema completo).

## 1. Ingest OSM
```
(.venv) python etl/osm_overpass.py
```
- Splitta la bounding box configurata e interroga Overpass con retry.
- Scrive/aggiorna `osm_business` e `osm_roads`.
- Log progressivo: tile scaricati, deduplice, conteggio finale di POI e strade.

## 2. Pipeline SQL di normalizzazione
```
(.venv) python etl/run_all.py
```
Esegue in sequenza gli step di `etl/sql_blocks/`:
1. `build_places_raw.sql` → staging `places_raw` (merge OSM + eventuali API terze).
2. `normalize_osm.sql` → `places_clean` con indirizzo ripulito, città stimata, flag phone/website.
3. `context_sector_density.sql` → `place_sector_density` (conteggio vicini e score densità).

> Output atteso dopo questa fase (verificabile da UI > counts o via SQL):  
> `places_raw` > 0, `places_clean` > 0, `place_sector_density` > 0.

## 3. Enrichment LLM
```
(.venv) python -m etl.enrich.run_enrichment --limit 100
```
- Seleziona i business senza fatti recenti (`business_facts` vuoto o `updated_at` oltre TTL).
- Costruisce il prompt con `build_prompt` includendo:
  - Nome, categoria, tags OSM, flag phone/website.
  - Indirizzo/città risolti: combina `places_clean.address`, `formatted_address`, `tags addr:*` e, se serve, un fallback geospaziale su `istat_comuni`.
- Chiama l’LLM tramite `load_client_from_env` (`OpenAIChatClient` o `PerplexityClient`).
- Valida l’output JSON con `EnrichedFacts` (`etl/enrich/schema.py`); se valido:
  - Aggiorna `enrichment_request`/`enrichment_response`.
  - Upserta `business_facts` (size_class, is_chain, budget, affinity, presenza digitale, confidenza, metadati provider/modello).

Controlli consigliati:
- UI > badge `business_facts` oppure `SELECT COUNT(*) FROM business_facts`.
- `SELECT status, COUNT(*) FROM enrichment_request GROUP BY status` per eventuali errori.

## 4. Feature Builder (metriche Brellé)
```
(.venv) python -m feature_builder.build_metrics
```
Aggrega le sorgenti principali:
- `places_clean`, `place_sector_density`.
- `business_facts` (output LLM).
- Dati contestuali (`osm_roads`, `brello_stations`, `geo_zones`).

E calcola:
- `sector_density_score / neighbors`.
- `geo_distribution_label` + source.
- Dimensione e budget (usando LLM o fallback rule-based).
- `umbrella_affinity`, `digital_presence` (blend sito, social, marketing), `marketing_attitude`.
- `facts_confidence` (dalla confidenza LLM, con boost per sito/social).

Risultato finale in `business_metrics` (upsert con `ON CONFLICT`). Verifica: `SELECT COUNT(*) FROM business_metrics`.

## 4-bis. Automazione Enrichment + Metriche
```
(.venv) python -m automation.auto_refresh [--dry-run]
```
- Carica `.env`, controlla il database e verifica se ci sono business oltre il TTL (`ENRICHMENT_TTL_DAYS`, default 30) o senza `business_facts`.
- Esegue automaticamente `python -m etl.enrich.run_enrichment --limit 100` (limite personalizzabile con `--enrich-limit` oppure variabile `AUTO_REFRESH_ENRICH_LIMIT`).
- Interroga Postgres e manda al LLM solo i business privi di `business_facts` oppure con un `updated_at` oltre il TTL (a meno di usare `--force-enrichment`).
- Lancia `python -m feature_builder.build_metrics` subito dopo l'enrichment (anche se non c'erano record mancanti puoi forzare con `--always-run-metrics`).
- Usa `--dry-run` per vedere il report senza avviare i job; `--force-enrichment` ignora il TTL ma aggiorna comunque le metriche.
- L'arricchimento viene eseguito a batch: `--enrich-limit` (o `AUTO_REFRESH_ENRICH_LIMIT`) definisce la dimensione di ogni chunk, che viene ripetuto finché restano candidati o fino a `AUTO_REFRESH_MAX_ENRICH_BATCHES`. Con `--metrics-each-batch` (o `AUTO_REFRESH_METRICS_EACH_BATCH=1`) il builder delle metriche parte dopo ogni batch invece di attendere la fine di tutto il giro.

## 5. API FastAPI
```
(.venv) uvicorn api.main:app --reload
```
Endpoint principali:
- `GET /health` → connessione DB.
- `GET /counts` → riepilogo tabelle chiave (UI lo usa per i badge).
- `POST /etl/overpass/start`, `/etl/pipeline/start` → lanciano thread che richiamano i comandi Python sopra.
- `GET /etl/status` → stato dei job (polling ogni 3s in UI).
- `GET /places` → join `places_clean` + `business_metrics` + `business_facts` con filtri per city/category/geo/size/budget/catena + minimi per affinity/digital/density.

> Se `business_metrics` è vuota, `/places` restituisce array vuoto anche con `places_clean` popolata.

## 6. UI Dashboard
```
cd ui
npm install (una tantum)
npm run dev
```
Funzionalità:
- Campo `API base URL` (default `http://127.0.0.1:8000`).
- Filtri avanzati sui campi `/places`.
- Bottoni `Run Overpass`, `Run Pipeline`, `Refresh Status`.
- Tabella con badge Affinità / Digitale / Densità e metadati business.
- Messaggi d’errore se fetch falliscono; polling su job ETL finché status `running`.

## 7. Sequenza consigliata per un load completo
1. `docker-compose up -d`
2. `python etl/osm_overpass.py`
3. `python etl/run_all.py`
4. (Facoltativo ma raccomandato) Controllo counts via UI o SQL.
5. `python -m etl.enrich.run_enrichment --limit 100` (ripetibile, TTL 30gg).
6. `python -m feature_builder.build_metrics`
7. Avvia `uvicorn` e `npm run dev`, poi “Search” in UI.

## 8. Verifiche e troubleshooting rapidi
- `SELECT COUNT(*) FROM business_facts` > 0 e `business_metrics` > 0 dopo gli step 3-4.
- `SELECT status, COUNT(*) FROM enrichment_request GROUP BY status` → eventuali fallimenti con relativo messaggio in `error`.
- `SELECT COUNT(*) FROM place_sector_density WHERE density_score IS NULL` → indica se la normalizzazione ha prodotto metriche.
- Nessun dato in UI → quasi sempre `business_metrics` vuota o API offline (badge rosso in UI). Rilancia step 4 oppure controlla `/health`.
- Problemi di city/address nel prompt → il nuovo resolver (`BusinessRow`) usa fallback su `tags addr:*` e `istat_comuni`; assicurati che `normalize_osm.sql` sia stato eseguito e che `istat_comuni` sia presente.

## 9. Componenti e tabelle (produttori → consumatori)
- `etl/osm_overpass.py` → `osm_business`, `osm_roads`.
- `etl/sql_blocks/*.sql` → `places_raw`, `places_clean`, `place_sector_density`.
- `etl/enrich/run_enrichment.py` → `enrichment_request`, `enrichment_response`, `business_facts`.
- `feature_builder/build_metrics.py` → `business_metrics`.
- `api/main.py` → `/counts`, `/places`, job ETL.
- `ui/src/App.tsx` → interfaccia utente, filtri e controlli.

Seguendo questo flusso, la tabella della UI mostrerà le metriche Brellé arricchite per ogni attività rilevata via OSM.
