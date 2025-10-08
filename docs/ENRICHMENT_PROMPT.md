# Prompt Enrichment & Integrazione

## Struttura del prompt inviato
- Il prompt è costruito da `build_prompt` (`etl/enrich/prompts.py`) ogni volta che il runner deve arricchire un'attività.
- Contiene un contesto iniziale che istruisce l'LLM a comportarsi come analista marketing locale e a produrre **solo** un JSON valido.
- Sezione `Attivit??`: riporta `name`, `category`, `address`, `city`, coordinate lat/lon e ulteriori dettagli opzionali (indirizzo formattato, tipologia/subtype OSM, tags categoria, presenza di sito/telefono, CAP/provincia/regione dai tag, cucina dichiarata, brand, note dal dataset).
- Regole operative: enumerano i campi richiesti (size, chain, budget, umbrella affinity ecc.), come trattare l'incertezza (`null` + abbassare `confidence`), e vincoli su formato e piattaforme social.
- Chiusura con uno **schema esempio** (configurabile via `include_schema`) che offre all'LLM una traccia di output atteso; lo schema è serializzato in JSON con caratteri non ASCII abilitati per mantenere glifi italiani.

## Risposta attesa e parsing
- L'LLM deve restituire **un singolo oggetto JSON** (nessun testo extra); in dry-run mostriamo solo un estratto del prompt nei log per evitare rumore.
- Il payload viene caricato in `parse_enriched_facts` (`etl/enrich/schema.py`), che:
  - Rimuove eventuali fence Markdown ```json.
  - Normalizza URL come `website_url` e le voci della mappa `social` (prefisso https se assente).
  - Valida il risultato con il modello `EnrichedFacts` (campo opzionale di default `None`, range clampato 0..1 per punteggi).
  - In caso di errore logga uno snippet della risposta e marca la richiesta come `error` nella tabella `enrichment_request`.

## Integrazione nel progetto
1. `python -m etl.enrich.run_enrichment` carica `.env`, crea il client LLM attraverso `load_client_from_env` e istanzia `EnrichmentRunner` (`etl/enrich/run_enrichment.py`).
2. Il runner seleziona i candidate business dal database (`_fetch_candidates`) usando `psycopg2`, rispettando TTL o flag `--force`.
3. Per ogni attività:
   - Inserisce/aggiorna `enrichment_request` con stato `running` e memorizza l'`input_payload` (prompt + dati di contesto).
   - Costruisce il prompt e chiama l'LLM (`LLMClient.complete`). Ritardi tra chiamate regolati da `ENRICHMENT_REQUEST_DELAY`.
   - Salva la risposta grezza e il JSON validato in `enrichment_response`; aggiorna `business_facts` con i valori arricchiti e metadati (`provider`, `source_model`, `confidence`).
4. Lo script `feature_builder/build_metrics.py` usa `business_facts` per combinare i dati arricchiti con densità, zone e altri segnali, producendo `business_metrics` (size, budget, affinità, presenza digitale, ecc.).
5. L'API FastAPI (`api/main.py`) espone gli score via `/places`, e la UI (`ui/src/App.tsx`) li rende filtrabili mostrando i badge delle metriche Brellé.

> Per verificare se il sistema chiama realmente GPT/Perplexity, controlla `LLM_PROVIDER` e le rispettive API key nel `.env`. Con un client attivo vedrai record in `enrichment_request/response` con `provider` valorizzato e log `Enrichment progress …` senza tag `[dry-run]`.
