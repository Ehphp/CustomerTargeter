# Metriche Brello

Questo documento descrive gli input, le trasformazioni e le tabelle coinvolte nella generazione delle metriche Brello (`business_metrics`) tramite lo script `feature_builder/build_metrics.py`.

## Tabelle di input
- `places_clean`: anagrafica del punto vendita normalizzato (nome, categoria, citta, flag phone/website, geometria).
- `place_sector_density`: numero di competitor nello stesso settore e punteggio di densita.
- `business_facts`: output dell'enrichment LLM con dimensione, catena, budget, affinity, social, confidence e metadati.
- `geo_zones`: poligoni geospaziali con label/kind e priorita, utili per assegnare zone commerciali o centro storico.
- `brello_stations`: stazioni Brello con coordinate, usate per capire se il business e a ridosso di una fermata rilevante.

## Struttura dello script `build_metrics.py`
1. **fetch_base_rows**: apre la connessione Postgres, esegue la query principale e crea un elenco di righe con tutti i campi necessari dai join (`places_clean` + `place_sector_density` + `business_facts` + join laterali su `geo_zones`, `brello_stations`).  
2. **compute_metrics_rows**: trasforma ogni riga in un dataclass `MetricsRow` applicando le funzioni di business (`resolve_size_class`, `infer_budget_band`, `default_affinity`, `compute_digital_presence`, `compute_geo_distribution`).  
3. **upsert_metrics**: scrive i risultati su `business_metrics` con `INSERT ... ON CONFLICT DO UPDATE` e aggiorna `updated_at` a `now()`.

## Regole di calcolo principali
### Densita settoriale
- `sector_density_neighbors`: valore diretto da `place_sector_density.neighbor_count` (fallback 0).
- `sector_density_score`: valore diretto da `place_sector_density.density_score` (fallback 0.0).

### Distribuzione geografica (`compute_geo_distribution`)
Priorita delle fonti:
1. **Stazioni Brello**: se la distanza dalla stazione piu vicina e <= 100 m, label `vicino_brello`, sorgente `brello_station`.
2. **Geo zone**: se esiste un poligono che contiene il punto, usa `geo_zones.label`. Se `kind` appartiene a `{centro, center, historic}` la label finale e `centro`, altrimenti viene mantenuta la label con sorgente `geo_zone:<label>`.
3. **Fallback**: label `altro`, sorgente `fallback`.

### Classe dimensionale (`resolve_size_class`)
- Priorita al dato LLM (`business_facts.size_class`).
- In assenza di valore, applica heuristiche basate su categoria e presenza di catena (es. grande distribuzione -> `grande`, ristorazione -> `piccola`, professionisti -> `micro`).  

### Flag catena
- Usa `business_facts.is_chain` se fornito. In fallback, per alcune categorie e dimensioni (media/grande) marca `True` quando la categoria suggerisce un network strutturato.

### Banda budget (`infer_budget_band`)
- Usa `business_facts.ad_budget_band` se disponibile.
- Altrimenti mappa `size_class` -> {micro: basso, piccola: medio, media: medio, grande: alto} e applica aggiustamenti per categorie specifiche (es. GDO -> alto, professionisti -> mai oltre medio).

### Affinita al mezzo ombrello (`default_affinity`)
- Riporta il valore LLM (`business_facts.umbrella_affinity`) se presente.
- In fallback usa il dizionario `AFFINITY_RULES` che associa categorie a valori 0..1 (es. food 0.85, servizi professionali 0.35, officine 0.2).

### Presenza digitale (`compute_digital_presence`)
Calcola un punteggio 0..1 combinando:
- +0.4 se esiste un sito (`places_clean.has_website` o `business_facts.website_url`).
- +fino a 0.4 dai social confermati (normalizzazione su max 3 canali).
- +fino a 0.2 da `marketing_attitude`.
La confidenza digitale parte da `business_facts.confidence` (o 0.4) e aumenta di +0.1 per sito e +0.1 per social, con limite 1.0.

### Marketing attitude e confidence
- `business_metrics.marketing_attitude` copia direttamente il valore LLM.
- `business_metrics.facts_confidence` riprende `business_facts.confidence` per misurare l'affidabilita delle informazioni usate.

## File e costanti chiave
- `feature_builder/build_metrics.py`: entrypoint con logica di orchestrazione (fetch, compute, upsert).
- `common/business_rules.py`: contiene le funzioni di supporto, le mappe categoria->affinity, gli elenchi di categorie per dimensione e le euristiche di budget.
- Tabelle di supporto (`sql/shema.sql`): definizione dei campi e delle foreign key utilizzate dallo script.

## Relazione con gli altri moduli
- **ETL `run_all.py`**: aggiorna `places_clean` e `place_sector_density`, prerequisiti per calcolare le metriche.
- **Enrichment LLM** (`python -m etl.enrich.run_enrichment`): scrive `business_facts`, principale fonte dei segnali "soft".
- **API/UI**: l'endpoint `/places` legge `business_metrics` (piu `business_facts`) e la UI visualizza badge e filtri su questi campi.
- **Automazione** (`python -m automation.auto_refresh`): controlla la staleness di `business_facts`/`business_metrics` e, se necessario, rilancia enrichment e metriche in sequenza.

## Troubleshooting
- **`business_metrics` vuota o incompleta**: rieseguire `python etl/run_all.py`, poi `python -m feature_builder.build_metrics`. Se mancano `business_facts`, lanciare prima l'enrichment.
- **Geo label sempre "altro"**: verificare che `brello_stations` e `geo_zones` siano popolati e che le geometrie coprano l'area di interesse.
- **Punteggi digitali bassi**: controllare che l'enrichment abbia effettivamente trovato sito/social; in caso contrario aggiornare `business_facts` o completare manualmente.
- **Errori di esecuzione**: controllare le variabili Postgres nel `.env` e leggere i log stampati da `build_metrics.py` (riporta la riga problematica prima di fallire).
