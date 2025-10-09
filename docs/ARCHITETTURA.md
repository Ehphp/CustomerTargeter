# CustomerTarget · Architettura Aggiornata

## Obiettivo

CustomerTarget porta in un unico stack i dati provenienti da Google Places e li arricchisce tramite un micro-servizio LLM per produrre le **metriche Brellò**: densità settoriale, classificazione geografica, dimensione stimata, banda budget pubblicitario, affinità al mezzo “ombrello” e presenza digitale. I vecchi score MVP (popolarità/contesto/accessibilità) sono stati rimossi.

## Panoramica

- **Database**: PostgreSQL + PostGIS (via Docker) custodisce dati raw OSM, normalizzazioni e nuovi fatti/metrice.
- **ETL base** (`etl/`):
  - `google_places.py`: interroga Google Places (Text Search + Details) e popola `places_raw`.
  - `sql_blocks/`: step idempotenti (`normalize_places`, `context_sector_density`) per costruire `places_clean` e `place_sector_density` a partire da `places_raw`.
  - `run_all.py`: orchestration runner per gli script SQL.
- **LLM Enrichment** (`etl/enrich/`): micro-servizio che interroga GPT/Perplexity (via REST) e scrive `enrichment_request/response` + `business_facts` con i campi mancanti (size, catena, budget, affinity, social…). Gestisce hashing input, retry e parsing JSON con Pydantic.
- **Feature Builder** (`feature_builder/build_metrics.py`): mix Python+SQL che calcola le metriche Brellò e upserta in `business_metrics` (densità settoriale, geo label, affinità, digitale, budget).
- **API** (`api/main.py`): FastAPI espone health/checks, orchestrazione ETL e nuovo endpoint `/places` filtrabile per city/category/geo/size/budget ecc., basato su `business_metrics` + `business_facts`.
- **UI** (`ui/src/App.tsx`): mini dashboard React/Vite che mostra le metriche Brellò, filtri avanzati e controlli per lanciare ETL/enrichment. Non esistono più colonne “total/popularity/territory/accessibility”.

## Data Flow

1. **OSM ingest**  
   `etl/osm_overpass.py` → `osm_business`, `osm_roads`

2. **Normalizzazione SQL** (`run_all.py`)  
   - `normalize_places.sql` → normalizzazione dei record Google → `places_clean`  
   - `normalize_osm.sql` → `places_clean` (campi puliti, city, flags)  
   - `context_sector_density.sql` → `place_sector_density` (conteggio e score di vicini stesso settore a 500 m)

3. **LLM Enrichment** (`python -m etl.enrich.run_enrichment --limit 200`)  
   - Seleziona attività senza fatti o scadute (TTL configurabile, default 30 gg).  
   - Costruisce prompt (nome, categoria, tags) → chiama GPT/Perplexity (via `LLM_PROVIDER`, `OPENAI_API_KEY`/`PERPLEXITY_API_KEY`).  
   - Valida output in `EnrichedFacts` (Pydantic), salva raw/parsed in `enrichment_response`, upserta consolidato in `business_facts`.

4. **Feature Builder** (`python -m feature_builder.build_metrics`)  
   - Unisce `places_clean`, `place_sector_density`, `business_facts`, `osm_roads`, `brello_stations`, `geo_zones`.  
   - Calcola: densità normalizzata, etichetta geografica (`vicino_brello`, `passaggio`, `centro`, …), dimensione stimata, banda budget, affinità (fallback rules), presenza digitale (website/social/marketing attitude), confidenze.  
   - Upsert finale in `business_metrics`.

5. **API & UI**  
   - `/counts`: monitora `places_raw`, `places_clean`, `place_sector_density`, `business_facts`, `business_metrics`, `osm_*`, `brello_stations`, `geo_zones`.  
   - `/places`: filtri `city`, `category`, `geo_label`, `size_class`, `ad_budget`, `is_chain`, soglie minime per `umbrella_affinity`, `digital_presence`, `sector_density_score`. Restituisce record arricchiti.  
   - UI mostra le metriche in tabella (Affinità, Digitale, Densità) + badge dimensione/budget/catena/confidenza. Controlli per avviare ETL e refresh.

## Schema Attuale (estratto)

- `osm_business(osm_id, name, category, subtype, tags, phone, website, opening_hours, location)`  
- `osm_roads(osm_id, highway, name, geom)`  
- `places_raw(...)` → staging  
- `places_clean(...)` → entità pulite  
- `place_sector_density(place_id, sector, neighbor_count, density_score, computed_at)`  
- `brello_stations(station_id, name, geom, metadata)`  
- `geo_zones(zone_id, label, kind, priority, geom)`  
- `enrichment_request/response` → tracking job LLM  
- `business_facts(business_id, size_class, is_chain, website_url, social, marketing_attitude, umbrella_affinity, ad_budget_band, confidence, provenance, source_provider, source_model, …)`  
- `business_metrics(business_id, sector_density_neighbors, sector_density_score, geo_distribution_label, geo_distribution_source, size_class, is_chain, ad_budget_band, umbrella_affinity, digital_presence, digital_presence_confidence, marketing_attitude, facts_confidence, updated_at)`

Le definizioni sono in `sql/shema.sql` e vengono applicate al bootstrap del container PostGIS (o manualmente via `psql`).

## Runbook

1. **Database + Adminer**  
   `docker-compose up -d` dalla root.

2. **Estrarre/normalizzare OSM**  
   ```bash
   (.venv) python etl/osm_overpass.py
   (.venv) python etl/run_all.py
   ```

3. **Enrichment LLM**  
   ```bash
   # Configurare .env con LLM_PROVIDER, OPENAI_API_KEY o PERPLEXITY_API_KEY
   (.venv) python -m etl.enrich.run_enrichment --limit 200
   ```

4. **Costruire metriche Brellò**  
   ```bash
   (.venv) python -m feature_builder.build_metrics
   ```

5. **API & UI**  
   ```bash
   # API
   (.venv) uvicorn main:app --reload --host 0.0.0.0 --port 8000  # dalla cartella api/
   # UI
   npm install   # se non già fatto
   npm run dev   # dalla cartella ui/
   ```

Ordine consigliato: DB → `osm_overpass.py` → `run_all.py` → enrichment → feature builder → API/UI.

## Considerazioni e guardrail

- **LLM**: output forzato JSON, validato da Pydantic; caching via `input_hash`. In dry-run, lo script logga i prompt senza colpire l’API.  
- **Metriche**: densità settoriale limitata a 30 vicini (score ∈ [0,1]); affinità fallback su dizionari categoria; digitale combina website/social/marketing attitude con confidenza.  
- **Geografia**: priorità etichette `vicino_brello` (≤100 m dalle stazioni), poi `passaggio` (≤50 m da strade primarie), poi poligoni `geo_zones`, infine fallback `altro`.  
- **Roll-back**: tutti gli step sono idempotenti (`ON CONFLICT`) e mantengono cronologia minima via `updated_at`.  
- **Configurazione**: parametri di throttling LLM (`ENRICHMENT_REQUEST_DELAY`, `ENRICHMENT_TTL_DAYS`, `ENRICHMENT_PROMPT_VERSION`) e logging (`ENRICHMENT_LOG_LEVEL`, `FEATURE_BUILDER_LOG_LEVEL`) via `.env`.

## Estensioni previste

- Validazione manuale dei fatti (`needs_review`) e tool di correzione manuale.  
- Integrazione di fonti open per digital footprint (es. crawler recensioni).  
- Scheduler (Celery/Cron) per orchestrare Overpass → SQL → Enrichment → Metrics.  
- Arricchimento `geo_zones` e `brello_stations` con layer ufficiali.  
- Endpoints FastAPI dedicati a costi/token e audit dell’enrichment.

## Metriche Brello - dettaglio calcolo

### Densità settoriale
- Fonte: `context_sector_density.sql`.
- Logica: conta i POI con stessa `category` entro 500 m (`ST_DWithin`).  
  - `sector_density_neighbors` = numero di vicini (escluso il soggetto).  
  - `sector_density_score` = `LEAST(neighbor_count / 30.0, 1.0)` per normalizzare 0..1 con tetto a 30.

### Distribuzione geografica
- Valutata in `feature_builder.compute_geo_distribution`.
- Regole in ordine di priorità:
  1. `vicino_brello` se `ST_Distance` da una stazione Brellò ≤ 100 m.
  2. `passaggio` se esiste strada primaria (`highway` ∈ {motorway,trunk,primary,secondary,tertiary}) entro 50 m.
  3. `geo_zones` se il punto ricade in un poligono (usa `label` e `kind` con ordinamento per `priority`).
  4. Fallback `altro`.  
- Il campo `geo_distribution_source` traccia la regola scattata.

### Dimensione stimata (`size_class`)
- Preferisce il valore LLM (`business_facts.size_class`).
- Se assente, applica regole:
  - catene (`is_chain=True`) → minimo `media`.
  - `supermarket`, `hypermarket`, `shopping_centre` → `grande`.
  - `gym`, `fitness_centre`, `car_dealer` → `media`.
  - food & beverage (`restaurant`, `pizzeria`, `bar`, `cafe`, `gelateria`, `fast_food`) → `piccola`.
  - servizi di quartiere (`pharmacy`, `hairdresser`, `beauty_salon`, `optician`) → `piccola`.
  - professionisti (`lawyer`, `notary`, `accountant`) → `micro`.
  - default → `micro`.

### Banda budget pubblicitario (`ad_budget_band`)
- Se il modello restituisce la banda, viene usata e marcata con `budget_source = 'LLM_infer'`.
- Altrimenti combina dimensione e categoria:
  - Mappa base dimensione → {`micro`: basso, `piccola`: medio, `media`: medio, `grande`: alto}.
  - Eccezioni: professionisti massimo medio/basso; grande distribuzione sempre alto; food retail almeno medio.

### Affinità al mezzo Brellò (`umbrella_affinity`)
- Valore LLM (0..1) quando disponibile.
- Fallback da dizionario `AFFINITY_RULES`:
  - Food & beverage ~0.85–0.9; retail moda/beauty 0.7; servizi professionali 0.3–0.4; officine 0.2; default 0.5.
- UI mostra badge percentuale e la query può filtrare con `min_affinity`.

### Presenza digitale (`digital_presence`)
- Calcolata in `compute_digital_presence` combinando:
  - +0.4 se esiste sito (`has_website` o `business_facts.website_url`).
  - + fino a 0.4 per social verificati (`len(social)` limitato a 3 piattaforme).
  - + fino a 0.2 per `marketing_attitude` (0..1).  
- Il punteggio è clampato 0..1.
- `digital_presence_confidence` parte da `business_facts.confidence` (o 0.4) e migliora di +0.1 per sito e +0.1 per social (cap 1.0).

### Marketing attitude & confidence
- `business_metrics.marketing_attitude` replica il valore LLM (proxy 0..1).
- `business_metrics.facts_confidence` riprende `business_facts.confidence` per pesare affidabilità del dato.

### Aggiornamento tabelle
- `feature_builder.build_metrics` usa `INSERT ... ON CONFLICT` per mantenere le metriche aggiornate e imposta `updated_at = now()` su ogni run.
- `business_facts` viene aggiornato dall’enrichment (LLM) con metadata `source_provider`/`source_model`.

## Troubleshooting

- **Nessun dato in UI**: assicurarsi che `business_metrics` sia popolata (`python -m feature_builder.build_metrics`) e che `/places` sia raggiungibile (`curl`).  
- **Enrichment bloccato**: verificare `LLM_PROVIDER` e API key; controllare `enrichment_request.status='error'` per messaggi dal provider.  
- **Geo label sempre “altro”**: popolari `brello_stations`/`geo_zones` e lanciare di nuovo `feature_builder`.  
- **Overpass lento**: restringere BBOX in `osm_overpass.py` o aumentare splitting (`split_bbox`).  
- **UI offline**: controllare CORS dell’API (`allow_origin_regex`) e `API base URL` nella dashboard.
