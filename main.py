from src.ingest_weather import ingest_once, ensure_indexes


if __name__ == "__main__":
    ensure_indexes()
    ingest_once(None)
    # Write clean to pg