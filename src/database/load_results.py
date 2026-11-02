#!/usr/bin/env python3
"""
load_results.py - Parsowanie pliku `results.tsv` pobranego z HDFS (wynik zadania
Hadoop Streaming MapReduce) i zapisanie zagregowanych statystyk pogodowych do
kolekcji w bazie NoSQL MongoDB Atlas (oraz lokalnym MongoDB) za pomocą `insert_many()`.

Wiedza teoretyczna ("Learn as I go"):
- Dokumentacja PyMongo: metoda `collection.insert_many(documents, ordered=False)`
  umożliwia błyskawiczne wstawienie tablicy słowników (dokumentów BSON/JSON) w jednym
  żądaniu sieciowym zamiast wykonywania tysięcy pojedynczych operacji `insert_one()`.
- W architekturze Lambda / Batch Processing wyniki ciężkich obliczeń MapReduce z HDFS
  są eksportowane jako kompaktowe agregaty (Serving Layer) do bazy NoSQL (MongoDB),
  skąd aplikacje analityczne lub dashboardy mogą odpytywać je w czasie rzędu milisekund
  oraz łączyć (`$lookup`) z kolekcją metadanych `stations`.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Wczytanie konfiguracji z pliku .env
load_dotenv(PROJECT_ROOT / ".env")

MONGO_LOCAL_URI = os.getenv("MONGO_LOCAL_URI", "mongodb://localhost:27017/")
MONGO_LOCAL_DB = os.getenv("MONGO_LOCAL_DB", "bigdata_local")

MONGO_ATLAS_URI = os.getenv("MONGO_ATLAS_URI") or os.getenv("MONGODB_URI", "")
MONGO_ATLAS_DB = os.getenv("MONGO_ATLAS_DB", "bigdata_atlas")

DEFAULT_RESULTS_FILE = PROJECT_ROOT / "data/output/results.tsv"
DEFAULT_COLLECTION_NAME = "weather_results"


def parse_results_tsv(tsv_path: Path) -> list[dict]:
    """
    Odczytuje pobrany z HDFS plik `results.tsv` (w formacie:
    `STN_ID \t avg_temp=...,min_temp=...,max_temp=...,avg_wind=...,max_wind=...,total_prcp=...,max_prcp=...,count=...`)
    i tworzy z niego tablicę słowników (dokumentów JSON) gotową do przekazania do `insert_many()`.
    """
    if not tsv_path.exists():
        raise FileNotFoundError(
            f"Plik '{tsv_path}' nie istnieje! Uruchom najpierw zadanie MapReduce (run_mapreduce.sh)."
        )

    documents: list[dict] = []
    now_utc = datetime.now(timezone.utc)

    with open(tsv_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue

            station_id, metrics_str = parts[0].strip(), parts[1].strip()
            metrics: dict[str, float | int] = {}

            for kv in metrics_str.split(","):
                if "=" not in kv:
                    continue
                k, v = kv.split("=", 1)
                k = k.strip()
                v = v.strip()
                try:
                    if k == "count":
                        metrics[k] = int(v)
                    else:
                        metrics[k] = float(v)
                except ValueError:
                    continue

            doc = {
                "station_id": station_id,
                "avg_temp": metrics.get("avg_temp"),
                "min_temp": metrics.get("min_temp"),
                "max_temp": metrics.get("max_temp"),
                "avg_wind": metrics.get("avg_wind"),
                "max_wind": metrics.get("max_wind"),
                "total_prcp": metrics.get("total_prcp"),
                "max_prcp": metrics.get("max_prcp"),
                "count": metrics.get("count", 0),
                # Zagnieżdżone podsumowanie 3 analiz statystycznych (temperatura, wiatr, opady)
                "temperature_stats_f": {
                    "avg": metrics.get("avg_temp"),
                    "min": metrics.get("min_temp"),
                    "max": metrics.get("max_temp"),
                },
                "wind_stats_knots": {
                    "avg": metrics.get("avg_wind"),
                    "max": metrics.get("max_wind"),
                },
                "precipitation_stats_in": {
                    "total": metrics.get("total_prcp"),
                    "max_daily": metrics.get("max_prcp"),
                },
                "loaded_at": now_utc,
            }
            documents.append(doc)

    print(f"[+] Sparsowano {len(documents):,} wierszy z pliku '{tsv_path}' do tablicy słowników.")
    return documents


def insert_results_to_mongodb(
    target_name: str,
    uri: str,
    db_name: str,
    collection_name: str,
    documents: list[dict],
    use_tls: bool = False,
) -> int:
    """
    Łączy się ze wskazaną bazą MongoDB (Atlas lub lokalną), czyści poprzedni stan kolekcji
    wynikowej (aby ponowne uruchomienie skryptu zastępowało zagregowany raport nową wersją)
    i przesyła całą tablicę słowników metodą `collection.insert_many()`.
    """
    print(f"\n{'=' * 65}")
    print(f"[*] Przesyłanie wyników do: {target_name} (baza: '{db_name}', kolekcja: '{collection_name}')")
    print(f"{'=' * 65}")

    if not uri or "<username>" in uri or "<password>" in uri:
        raise ValueError(f"Brak poprawnego URI dla '{target_name}' w pliku .env!")

    client_kwargs = {"serverSelectionTimeoutMS": 10000}
    if use_tls:
        client_kwargs["tls"] = True

    client = MongoClient(uri, **client_kwargs)
    try:
        db = client[db_name]
        collection = db[collection_name]

        # Wyczyszczenie kolekcji przed ponownym wsadowym wstawieniem wyników MapReduce
        deleted = collection.delete_many({})
        if deleted.deleted_count > 0:
            print(f"[*] Usunięto {deleted.deleted_count:,} poprzednich rekordów z kolekcji '{collection_name}'.")

        # Kopia słowników (insert_many dopisuje pole _id in-place do przekazanych obiektów dict)
        docs_to_insert = [dict(d) for d in documents]

        # Wymóg zadania: przesłanie danych przez collection.insert_many()
        result = collection.insert_many(docs_to_insert, ordered=False)
        inserted_count = len(result.inserted_ids)
        print(f"[+] Pomyślnie wstawiono {inserted_count:,} dokumentów przez collection.insert_many()!")

        # Założenie indeksu na station_id ułatwiającego szybkie wyszukiwanie i $lookup ze stacjami
        collection.create_index([("station_id", ASCENDING)], unique=True)

        total_in_col = collection.count_documents({})
        sample_doc = collection.find_one({"station_id": "010030"}, {"_id": 0}) or collection.find_one({}, {"_id": 0})
        print(f"[+] Całkowita liczba dokumentów w '{db_name}.{collection_name}': {total_in_col:,}")
        print(f"[*] Przykładowy dokument z kolekcji '{collection_name}':\n    {sample_doc}")
        return total_in_col

    except PyMongoError as err:
        print(f"[-] BŁĄD MongoDB ({target_name}): {err}", file=sys.stderr)
        raise
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ładowanie wyników MapReduce (results.tsv) do kolekcji MongoDB Atlas (oraz lokalnej) przez insert_many()."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_RESULTS_FILE,
        help="Ścieżka do pliku results.tsv (domyślnie: results.tsv).",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=DEFAULT_COLLECTION_NAME,
        help="Nazwa kolekcji docelowej w MongoDB (domyślnie: weather_results).",
    )
    parser.add_argument(
        "--atlas-only",
        action="store_true",
        help="Zapisz wyniki wyłącznie do chmurowego klastra MongoDB Atlas.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        # 1. Parsowanie pliku results.tsv do tablicy słowników
        documents = parse_results_tsv(args.input)
        if not documents:
            print("[-] Plik wynikowy nie zawiera żadnych rekordów do załadowania!", file=sys.stderr)
            sys.exit(1)

        # 2. Przesłanie danych do chmurowego klastra MongoDB Atlas (wymóg główny)
        atlas_count = insert_results_to_mongodb(
            target_name="MongoDB Atlas (Chmura)",
            uri=MONGO_ATLAS_URI,
            db_name=MONGO_ATLAS_DB,
            collection_name=args.collection,
            documents=documents,
            use_tls=True,
        )

        # 3. Opcjonalnie zapis również do lokalnego MongoDB (Docker), aby obie bazy były spójne
        if not args.atlas_only:
            local_count = insert_results_to_mongodb(
                target_name="Lokalne MongoDB (Docker)",
                uri=MONGO_LOCAL_URI,
                db_name=MONGO_LOCAL_DB,
                collection_name=args.collection,
                documents=documents,
                use_tls=False,
            )
        else:
            local_count = None

        print(f"\n{'=' * 65}")
        print("[SUCCESS] Załadowano wyniki MapReduce do MongoDB!")
        print(f"    - MongoDB Atlas ({MONGO_ATLAS_DB}.{args.collection}): {atlas_count:,} dokumentów")
        if local_count is not None:
            print(f"    - Lokalne Mongo ({MONGO_LOCAL_DB}.{args.collection}): {local_count:,} dokumentów")
        print("Definition of Done (DoD) spełnione.")
        print(f"{'=' * 65}")

    except Exception as exc:
        print(f"[-] BŁĄD KRYTYCZNY: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

