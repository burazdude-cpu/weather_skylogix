from src.ingest_weather import ingest_once, ensure_indexes
from src.ingest_pg import run_pg_ingestion


if __name__ == "__main__":
    ensure_indexes()
    ingest_once(None)

    run_pg_ingestion()
