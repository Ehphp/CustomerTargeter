# Prompt Enrichment & Integrazione

## Struttura del prompt inviato
- Il prompt è costruito da `build_prompt` (`etl/enrich/prompts.py`) ogni volta che il runner deve arricchire un'attività.
- Contiene un contesto iniziale che istruisce l'LLM a comportarsi come analista marketing locale e a produrre **solo** JSON valido.
- Sezione `Attività`: riporta `name`, `category`, `address`, `city`, coordinate lat/lon, bounding box (~200 m) e ulteriori dettagli opzionali (indirizzo formattato, tipologia/subtype dai `types` di Google, presenza sito/telefono, note, ecc.).
- Le regole operative:
  - Impongono il rispetto delle coordinate/bounding box (configurabile via `ENRICHMENT_SEARCH_RADIUS_M`).
  - Chiedono di motivare eventuali discrepanze in `provenance.reasoning` e di elencare le fonti in `provenance.citations`.
  - Indicano esplicitamente di lasciare a `null` i campi dimensionali/metrici (`size_class`, `is_chain`, `marketing_attitude`, `umbrella_affinity`, `ad_budget_band`, `confidence`), che vengono calcolati downstream tramite `common/business_rules.py`.
- Il prompt si chiude con uno **schema esempio** serializzato in JSON (opzione `include_schema`).

## Risposta attesa e parsing
- L'LLM deve restituire **un singolo oggetto JSON** (nessun testo extra).
- `parse_enriched_facts` (`etl/enrich/schema.py`) rimuove eventuali fence ```json, normalizza URL e valida il payload con il modello `EnrichedFacts`.
- In caso di errore la richiesta viene marcata `error` in `enrichment_request` e il log mostra uno snippet della risposta.

## Integrazione nel progetto
1. `python -m etl.enrich.run_enrichment` carica `.env`, seleziona i candidati (rispettando TTL o flag `--force`) e costruisce il prompt.
2. Il client LLM (OpenAI o Perplexity) è scelto da `load_client_from_env`.
3. La risposta validata viene:
   - Salvata in `enrichment_response` (raw JSON + parsed JSON + usage).
   - Upsertata in `business_facts` insieme a provider/modello.
4. `feature_builder/build_metrics.py` combina `business_facts` con le regole di `common/business_rules.py` per calcolare le metriche deterministiche (dimensione, catena, budget, affinity, confidence) e popolare `business_metrics`.
5. L'API FastAPI (`api/main.py`) espone i dati via `/places`, e la UI (`ui/src/App.tsx`) li mostra con i filtri.

> Per verificare se l'LLM è stato realmente chiamato controlla `enrichment_request`/`enrichment_response` (provider valorizzato, log `Enrichment progress ...`). Use `--dry-run` per vedere solo i prompt senza chiamare il provider.
