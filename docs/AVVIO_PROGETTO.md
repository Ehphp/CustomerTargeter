# Guida Rapida di Avvio

## Percorso di lavoro
- Apri un terminale e porta la working directory su `c:\Users\EmilioCittadini\Desktop\CustumerTarget`.

## Prerequisiti
- Assicurati che il file `.env` (nella root del progetto) contenga i parametri Postgres e le eventuali API key (Overpass, Google, LLM).
- Attiva l'ambiente virtuale Python: `.\.venv\Scripts\activate`.
- (Solo al primo avvio) verifica che i servizi Docker necessari siano disponibili con `docker-compose up -d`.

## Pipeline dati (eseguite con venv attiva dalla root del progetto)
```powershell
python -m etl.google_places --location "Alatri, Italia" --queries ristorante bar negozio
python etl/run_all.py
python -m etl.enrich.run_enrichment --limit 100
python -m feature_builder.build_metrics
```

## Avvio API
```powershell
uvicorn api.main:app --reload
```

## Avvio UI
1. Apri un nuovo terminale.
2. Porta la directory su `c:\Users\EmilioCittadini\Desktop\CustumerTarget\ui`.
3. Esegui (una tantum) `npm install`.
4. Avvia il server di sviluppo con `npm run dev`.
5. Assicurati che la UI punti all'API `http://127.0.0.1:8000`.

## Automazione (opzionale)
```powershell
python -m automation.auto_refresh --dry-run
```
- Usa `--dry-run` per vedere i job che verrebbero lanciati.
- Rimuovi `--dry-run` per eseguire enrichment e metriche secondo la configurazione corrente.

## Verifiche suggerite
- Endpoint `/counts` o query SQL per confermare record in `places_clean`, `business_metrics`, `business_facts`.
- UI operativa con badge verdi e dati nella tabella.

