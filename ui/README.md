# CustomerTarget UI

Dashboard React/Vite che interroga la FastAPI di CustomerTarget per monitorare lo stato degli job e visualizzare le metriche Brello.

## Installazione rapida
```bash
cd ui
npm install
npm run dev
```
La UI punta di default a `http://127.0.0.1:8000`; e possibile cambiarlo dal campo "API base URL" nell'header.

## Flusso dati di riferimento
```
google_places.py  -->  places_raw
normalize_places.sql  -->  places_clean
context_sector_density.sql  -->  place_sector_density
LLM enrichment  -->  business_facts
feature_builder/build_metrics.py  -->  business_metrics
API /places  -->  UI App.tsx
```

## Funzionalita principali
- Sezione **Google Places Import**: consente di passare location, lat/lng, raggio, limit e lista di query, inviando una POST a `/etl/google_places/start`.
- Pulsanti **Run Pipeline** e **Run Auto Refresh** per avviare la pipeline SQL (`run_all.py`) e l'automazione enrichment+metriche (`automation/auto_refresh.py`).
- Badge di stato per ciascun job (`google_import`, `pipeline`, `auto_refresh`) con ultimo codice di ritorno e log recenti.
- Tabella principale con filtri su city, category, geo label, size class, budget band, oltre alle soglie minime per affinity/digital/density.
- Dettaglio espandibile per ogni business (metadati facts e risposta raw LLM).

## Suggerimenti
- Lancia l'import Google direttamente dalla UI per popolare `places_raw` prima di eseguire la pipeline SQL.
- Dopo ogni run pipeline/enrichment, usa "Refresh Status" per aggiornare i badge e i contatori (`/counts`).
- Se la tabella e vuota, verifica che l'API sia online (badge verde in alto a destra) e che `business_metrics` contenga righe aggiornate.
