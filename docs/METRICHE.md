# Calcolo delle metriche Brello

Questo documento descrive come vengono calcolate e popolate le colonne della tabella `business_metrics`, quali dati di input sono coinvolti e dove risiedono le logiche applicative. Tutto il codice vive nel modulo `feature_builder/build_metrics.py`, che viene invocato con:

```
(.venv) python -m feature_builder.build_metrics
```

Il comando si occupa di:
1. Caricare le variabili d'ambiente (`.env`) e aprire una connessione Postgres.
2. Estrarre i dati di base aggregando più tabelle (`fetch_base_rows`).
3. Applicare regole e fallback per costruire un `MetricsRow` per ogni business (`compute_metrics_rows`).
4. Scrivere/aggiornare `business_metrics` tramite `INSERT ... ON CONFLICT` (`upsert_metrics`).

## 1. Dati in ingresso

La query di `fetch_base_rows` legge da:

- `places_clean` (business normalizzati) — ID, nome, categoria, città, presenza sito/fascia oraria e geometria.
- `place_sector_density` — numero di vicini e punteggio di densità per il settore.
- `business_facts` — output dell'enrichment LLM (dimensione, catena, sito, social, marketing attitude, budget, affinity, confidence).
- `geo_zones` (JOIN laterale) — etichetta e tipo della zona geospaziale più rilevante (ordinata per `priority`).
- `brello_stations` (JOIN laterale) — distanza minima dalla stazione più vicina.
- `osm_roads` (JOIN laterale) — flag se l'attività è entro 50m da strade ad alto traffico (`motorway`, `trunk`, `primary`, `secondary`, `tertiary`).

Queste sorgenti forniscono tutte le colonne usate dagli step successivi.

## 2. Struttura dati e scrittura su `business_metrics`

Ogni riga elaborata viene trasformata in un dataclass `MetricsRow` che rispecchia lo schema della tabella target:

```python
class MetricsRow:
    business_id: str
    sector_density_neighbors: int
    sector_density_score: float
    geo_distribution_label: str
    geo_distribution_source: str
    size_class: Optional[str]
    is_chain: Optional[bool]
    ad_budget_band: Optional[str]
    umbrella_affinity: Optional[float]
    digital_presence: Optional[float]
    digital_presence_confidence: Optional[float]
    marketing_attitude: Optional[float]
    facts_confidence: Optional[float]
```

La funzione `upsert_metrics` costruisce una lista di tuple e usa `execute_values` per inserire in batch. In caso di conflitto su `business_id` tutti i campi vengono aggiornati e `updated_at` è impostato a `now()`.

## 3. Regole di calcolo per campo

Di seguito il dettaglio delle trasformazioni implementate in `compute_metrics_rows`.

### 3.1 Densità settoriale
- `sector_density_neighbors`: da `place_sector_density.neighbor_count`, default 0.
- `sector_density_score`: da `place_sector_density.density_score`, default 0.0.

### 3.2 Distribuzione geografica (`compute_geo_distribution`)
Priorità delle sorgenti:
1. **Stazioni Brello**: se la distanza (`station_distance`) ≤ 100 metri ⇒ etichetta `vicino_brello`, sorgente `brello_station`.
2. **Strade trafficate**: se `near_highway` è `TRUE` ⇒ etichetta `passaggio`, sorgente `road_high_traffic`.
3. **Zone geografiche**: se presente `geo_zones.label`, etichetta:
   - `centro` se `kind` ∈ {`centro`, `center`, `historic`},
   - altrimenti il valore della label con sorgente `geo_zone:<label>`.
4. **Fallback**: `altro` con sorgente `fallback`.

### 3.3 Classe dimensionale (`resolve_size_class`)
- Priorità al valore arricchito (`business_facts.size_class`).
- In assenza, heuristica basata su categoria e flag catena:
  - Catene con categoria `supermarket`/`shopping_centre` ⇒ `grande`, altrimenti `media`.
  - Categorie specifiche (es. `gym`, `car_dealer`) ⇒ `media`.
  - Ristorazione / personal care ⇒ `piccola`.
  - Professionisti (`lawyer`, `notary`, `accountant`) ⇒ `micro`.
  - Fallback generale ⇒ `micro`.

### 3.4 Flag catena
- `is_chain`: valore LLM se presente (`business_facts.is_chain`).
- Se assente e la `size_class` stimata è `media` o `grande`, il codice marca `True` se la categoria contiene `cooperative`, altrimenti lascia `NULL`.

### 3.5 Fascia budget (`infer_budget_band`)
- Priorità a `business_facts.ad_budget_band`.
- Fallback:
  - Mappa base `size_class` → {`micro`: basso, `piccola`: medio, `media`: medio, `grande`: alto}.
  - Alcune categorie spostano il risultato:
    - Professionisti (`lawyer`, `notary`, `accountant`, `dentist`) ⇒ mai oltre `medio`.
    - `supermarket`, `shopping_centre` ⇒ `alto`.
    - Ristorazione (`bar`, `cafe`, `pizzeria`, `gelateria`, `restaurant`) ⇒ `medio`.

### 3.6 Affinity (`default_affinity`)
- Priorità a `business_facts.umbrella_affinity`.
- Se mancante, lookup nella mappa `AFFINITY_RULES` (es. `bar` = 0.9, `hairdresser` = 0.6).
- Fallback generico ⇒ 0.5.

### 3.7 Presenza digitale (`compute_digital_presence`)
Calcolo punteggio (max 1.0):
- +0.4 se è presente un sito (`places_clean.has_website` o `business_facts.website_url`).
- +fino a 0.4 dai social (`business_facts.social`), normalizzato fino a 3 canali.
- +fino a 0.2 da `marketing_attitude` (valore LLM 0–1).

Livello di confidenza:
- Base: `business_facts.confidence` se numerico, altrimenti 0.4.
- +0.1 se c'è un sito, +0.1 se ci sono social. Il limite è 1.0.

### 3.8 Altri campi
- `marketing_attitude`: direttamente da `business_facts.marketing_attitude`.
- `facts_confidence`: copia di `business_facts.confidence`.

## 4. File e costanti chiave

- `feature_builder/build_metrics.py` — entrypoint e logiche di calcolo.
- `AFFINITY_RULES` — dizionario categoria → affinity di default.
- `HIGH_TRAFFIC_HIGHWAYS` — elenco dei tipi di strada considerati ad alto traffico.
- Funzioni helper:
  - `_category_token` — normalizza la categoria (lowercase).
  - `resolve_size_class`, `infer_budget_band`, `default_affinity`.
  - `compute_geo_distribution` — determina label e sorgente geografica.
  - `compute_digital_presence` — punteggio e confidenza digitale.

## 5. Relazione con gli altri componenti

- **Enrichment LLM** (`python -m etl.enrich.run_enrichment`): popola `business_facts`, la principale fonte di segnali "soft" (dimensione, budget, marketing, affinity, social).
- **SQL di contesto** (`python etl/run_all.py`): mantiene aggiornate `places_clean`, `place_sector_density`, `geo_zones`, `osm_roads`, `brello_stations`.
- **UI/API** (`api/main.py`, `ui/`): leggono `business_metrics` per esporre le metriche calcolate.
- **Automazione** (`python -m automation.auto_refresh`): controlla se i fatti sono obsoleti e, in caso, lancia enrichment e metriche in sequenza.

## 6. Suggerimenti operativi

- Per investigare anomalie sulle metriche, verificare prima che la riga in `business_facts` sia coerente (specie `size_class`, `ad_budget_band`, `confidence`).
- In caso di nuova categoria, aggiornare `AFFINITY_RULES`, `resolve_size_class` o `infer_budget_band` per gestire correttamente i fallback.
- Se si introducono nuove sorgenti geospaziali, estendere `compute_geo_distribution` mantenendo la priorità (stazione Brello > strade trafficate > geo zone > fallback).

Con queste informazioni è possibile capire esattamente come vengono calcolate le metriche e dove intervenire per modificarne il comportamento.

