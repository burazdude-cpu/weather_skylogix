# SkyLogix2 — Weather Data Pipeline

A Python ETL pipeline that fetches live weather data from the OpenWeatherMap API, stores it in MongoDB, and writes cleaned records to PostgreSQL.

## Pipeline Overview

```
OpenWeatherMap API → MongoDB (raw + normalized) → PostgreSQL (clean, deduplicated)
```

1. **Fetch** — `src/weather_client.py` calls the OpenWeatherMap API for each city.
2. **Normalize** — `src/normalize.py` maps raw API fields into a consistent document schema.
3. **Store (Mongo)** — `src/ingest_weather.py` upserts normalized documents into MongoDB, deduplicating on `city + country_code + observed_at`.
4. **Write (Postgres)** — `src/ingest_pg.py` reads from MongoDB, flattens the documents, and inserts into PostgreSQL, skipping duplicates on `city + country + observed_at`.

## PostgreSQL Schema

Table: `weather_observations`

| Column | Type | Description |
|---|---|---|
| city | TEXT | City name |
| country | TEXT | Country code (e.g. `US`, `GB`) |
| longitude | FLOAT | Geographic longitude |
| latitude | FLOAT | Geographic latitude |
| temperature | FLOAT | Temperature (Kelvin by default) |
| humidity | FLOAT | Relative humidity (%) |
| pressure | FLOAT | Atmospheric pressure (hPa) |
| wind_speed | FLOAT | Wind speed (m/s) |
| wind_direction | FLOAT | Wind direction (degrees) |
| observed_at | TIMESTAMP | Observation timestamp (UTC) |
| provider | TEXT | Data source (e.g. `openweathermap`) |

Unique constraint: `(city, country, observed_at)`

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials.
2. Create and activate the virtual environment:
   ```bash
   python -m venv env
   source env/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the pipeline:
   ```bash
   python main.py
   ```

## Project Structure

```
skylogix2/
├── main.py                  # Entry point
├── requirements.txt
├── .env.example
└── src/
    ├── weather_client.py    # OpenWeatherMap API calls
    ├── normalize.py         # Raw → normalized document
    ├── mongo_client.py      # MongoDB connection
    ├── ingest_weather.py    # Fetch + upsert to MongoDB
    └── ingest_pg.py         # MongoDB → PostgreSQL
```

---

## Student To-Do

Work through these tasks in order. Each one builds on the previous.

### 1. Understand the existing pipeline
- [ ] Read through all files in `src/` and trace the data flow from API call to MongoDB document.
- [ ] Identify what fields are stored in MongoDB vs. what columns are needed in PostgreSQL.

### 2. Set up PostgreSQL (`src/ingest_pg.py`)
- [ ] Create a SQLAlchemy engine using connection details from `.env`.
- [ ] Write a `create_table()` function that creates the `weather_observations` table if it does not exist.
- [ ] Add a `UNIQUE` constraint on `(city, country, observed_at)` to prevent duplicate rows.

### 3. Read from MongoDB
- [ ] Write a function that connects to MongoDB and fetches all documents from the weather collection.
- [ ] Consider filtering by a date range so you don't re-process old data every run.

### 4. Transform documents for PostgreSQL
- [ ] MongoDB documents store coordinates in a nested dict (`coordinates.lat`, `coordinates.lon`) and metrics in another (`metrics.temperature`, etc.). Flatten these into the flat PostgreSQL column structure.
- [ ] Make sure `observed_at` is a proper Python `datetime` object (not a string).

### 5. Write to PostgreSQL without duplicates
- [ ] Insert rows using `INSERT ... ON CONFLICT DO NOTHING` (or SQLAlchemy's equivalent) so re-runs are safe.
- [ ] Test by running the pipeline twice and confirming row count does not change.

### 6. Wire it into `main.py`
- [ ] Import and call the PostgreSQL ingest function after `ingest_once()`.
- [ ] Confirm end-to-end: run `python main.py` and query PostgreSQL to see the rows.

### Stretch goals
- [ ] Add a `--cities` CLI argument to `main.py` so you can target specific cities.
- [ ] Log how many rows were inserted vs. skipped each run.
- [ ] Schedule the pipeline to run every hour using `cron` or a simple `while True` loop with `time.sleep`.
