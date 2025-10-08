# CustomerTarget UI

Interfaccia React/Vite che interroga le API FastAPI per visualizzare le metriche Brellò.

## Installazione

```bash
cd ui
npm install
npm run dev
```

La UI punta di default a `http://127.0.0.1:8000` (modificabile da input in alto).

## Flusso dati sintetico

```
Overpass → osm_business / osm_roads
          └─ build_places_raw.sql → places_raw
places_raw └─ normalize_osm.sql → places_clean
places_clean └─ context_sector_density.sql → place_sector_density
LLM enrichment ─→ business_facts  ─┐
feature_builder/build_metrics.py ─┴→ business_metrics
```

L’interfaccia consente filtri su città, categoria, etichetta geografica, dimensione stimata, banda budget e soglie minime per affinità/ digitale / densità, oltre a lanciare gli step ETL direttamente dall’app.
