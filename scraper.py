#!/usr/bin/env python3
"""
scraper.py - Pobieranie metadanych stacji meteorologicznych WMO/NOAA z otwartego źródła
w sieci (tabela HTML z serwisu Ogimet / WMO oraz opcjonalnie archiwum NOAA ISD) i zapisanie
ich w chmurze (MongoDB Atlas) oraz w lokalnej bazie MongoDB (Docker).

Wiedza teoretyczna ("Learn as I go"):
- Biblioteka `requests`: pobieranie dokumentu HTML z otwartego katalogu stacji WMO/NOAA.
- Biblioteka `BeautifulSoup` (`bs4`): parsowanie drzewa DOM HTML i wyszukiwanie tagów
  `<table>` oraz wierszy `<tr>` / komórek `<td>`.
- Biblioteka `pymongo`: użycie operacji masowych (`collection.bulk_write(...)`) wraz z
  konstruktorem `UpdateOne({'station_id': ...}, {'$set': ...}, upsert=True)`, co gwarantuje
  idempotentność (brak duplikatów nawet przy wielokrotnym uruchomieniu skryptu).
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import PyMongoError

# Wczytanie zmiennych środowiskowych z pliku .env
load_dotenv()

MONGO_LOCAL_URI = os.getenv("MONGO_LOCAL_URI", "mongodb://localhost:27017/")
MONGO_LOCAL_DB = os.getenv("MONGO_LOCAL_DB", "bigdata_local")

MONGO_ATLAS_URI = os.getenv("MONGO_ATLAS_URI") or os.getenv("MONGODB_URI", "")
MONGO_ATLAS_DB = os.getenv("MONGO_ATLAS_DB", "bigdata_atlas")

COLLECTION_NAME = "stations"

# Domyślna lista krajów do pobrania z internetowego katalogu WMO (Ogimet HTML Table)
DEFAULT_COUNTRIES = [
    "Poland",
    "Germany",
    "France",
    "Spain",
    "Italy",
    "United Kingdom",
    "Norway",
    "Sweden",
    "Czech Republic",
    "Ukraine",
]

OGIMET_URL_TEMPLATE = (
    "https://www.ogimet.com/display_stations.php"
    "?lang=en&tipo=AND&isyn=&oaci=&nombre=&estado={country}&Send=Send"
)


def parse_dms_coordinate(dms_str: str) -> float | None:
    """
    Konwertuje współrzędne ze formatu DMS używanego w tabelach WMO (np. '52-09-56N', '020-58-02E')
    na stopnie dziesiętne (float, np. 52.165556).
    """
    if not dms_str or dms_str == "----":
        return None
    match = re.match(r"^(\d{2,3})-(\d{2})(?:-(\d{2}))?([NSEW])$", dms_str.strip())
    if not match:
        return None
    deg = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3)) if match.group(3) else 0
    hemisphere = match.group(4)

    decimal = deg + (minutes / 60.0) + (seconds / 3600.0)
    if hemisphere in ("S", "W"):
        decimal = -decimal
    return round(decimal, 6)


def fetch_wmo_stations_from_web(countries: list[str]) -> list[dict]:
    """
    Pobiera tabele HTML stacji meteorologicznych WMO/NOAA ze strony Ogimet za pomocą `requests`
    i parsuje tagi `<table>` oraz `<tr>` przy użyciu `BeautifulSoup`.
    Zwraca listę ustrukturyzowanych dokumentów JSON z unikalnym kluczem `station_id`.
    """
    stations_by_id: dict[str, dict] = {}
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
        }
    )

    for country in countries:
        url = OGIMET_URL_TEMPLATE.format(country=requests.utils.quote(country))
        print(f"[*] Pobieranie tabeli HTML stacji dla kraju '{country}' z: {url}")

        response = session.get(url, timeout=20)
        response.raise_for_status()

        # Parsowanie HTML za pomocą BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")

        # Szukamy tabeli zawierającej nagłówek 'WMO INDEX'
        target_table = None
        for table in tables:
            first_row = table.find("tr")
            if first_row and "WMO INDEX" in first_row.get_text():
                target_table = table
                break

        if target_table is None:
            print(f"[!] Ostrzeżenie: Nie znaleziono tabeli stacji dla kraju '{country}'.")
            continue

        rows = target_table.find_all("tr")
        parsed_in_country = 0

        # Pomijamy wiersz nagłówkowy (indeks 0) i iterujemy po wierszach <tr>
        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) < 8:
                continue

            wigos_id, wmo_index, icao, name, country_name, lat_raw, lon_raw, alt_raw = cells[:8]
            established = cells[8] if len(cells) > 8 else "----"
            closed = cells[9] if len(cells) > 9 else "----"

            # Wyznaczenie unikalnego klucza głównego station_id kompatybilnego z GSOD / WMO:
            # W zbiorze GSOD kody stacji (STN--- / USAF) to 6-cyfrowy numer powstały z 5-cyfrowego
            # indeksu WMO z dopisanym '0' na końcu (np. WMO 12375 -> GSOD 123750 dla Warszawy-Okęcie).
            if wmo_index and wmo_index != "-----" and wmo_index.isdigit():
                station_id = f"{wmo_index}0" if len(wmo_index) == 5 else wmo_index
            elif icao and icao != "----":
                station_id = f"ICAO_{icao}"
            else:
                continue

            # Jeśli w tabeli jest wpis historyczny (zamknięta stacja) i aktywny o tym samym WMO ID,
            # preferujemy wpis aktywny (closed == '----')
            if station_id in stations_by_id and closed != "----":
                continue

            try:
                elevation_m = float(alt_raw) if alt_raw and alt_raw != "----" else None
            except ValueError:
                elevation_m = None

            doc = {
                "station_id": station_id,
                "wmo_id": wmo_index if wmo_index != "-----" else None,
                "wigos_id": wigos_id if wigos_id != "0-0-0-MISSING" else None,
                "icao": icao if icao != "----" else None,
                "name": name,
                "country": country_name,
                "latitude": parse_dms_coordinate(lat_raw),
                "longitude": parse_dms_coordinate(lon_raw),
                "latitude_dms": lat_raw,
                "longitude_dms": lon_raw,
                "elevation_m": elevation_m,
                "established": established if established != "----" else None,
                "closed": closed if closed != "----" else None,
                "source": "ogimet_wmo_html",
                "updated_at": datetime.now(timezone.utc),
            }
            stations_by_id[station_id] = doc
            parsed_in_country += 1

        print(f"    [+] Sparsowano {parsed_in_country} wierszy <tr> dla '{country}'.")

    stations_list = list(stations_by_id.values())
    print(f"[+] Łącznie przygotowano {len(stations_list)} unikalnych dokumentów stacji.")
    return stations_list


def enrich_with_local_noaa_isd(stations: list[dict], isd_csv_path: Path, country_txt_path: Path) -> list[dict]:
    """
    Opcjonalnie wzbogaca pobrane ze strony stacje o metadane z oficjalnego pliku
    NOAA `isd-history.csv` oraz `country-list.txt` (jeśli są obecne lokalnie w `archive/`).
    """
    if not isd_csv_path.exists():
        return stations

    country_map: dict[str, str] = {}
    if country_txt_path.exists():
        with open(country_txt_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.strip().split(maxsplit=1)
                if len(parts) == 2 and parts[0] != "FIPS":
                    country_map[parts[0]] = parts[1].strip()

    isd_lookup: dict[str, dict] = {}
    with open(isd_csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            usaf = (row.get("USAF") or "").strip()
            if usaf and usaf != "999999":
                isd_lookup[usaf] = row

    enriched_count = 0
    for doc in stations:
        sid = doc["station_id"]
        if sid in isd_lookup:
            isd_row = isd_lookup[sid]
            doc["wban"] = isd_row.get("WBAN") or "99999"
            doc["noaa_station_name"] = isd_row.get("STATION NAME") or doc["name"]
            ctry_code = isd_row.get("CTRY") or ""
            doc["fips_country_code"] = ctry_code
            if ctry_code in country_map:
                doc["noaa_country_name"] = country_map[ctry_code]
            enriched_count += 1

    print(f"[*] Wzbogacono {enriched_count}/{len(stations)} stacji o powiązania z NOAA ISD (`isd-history.csv`).")
    return stations


def save_stations_bulk_upsert(
    target_name: str,
    uri: str,
    db_name: str,
    stations: list[dict],
    use_tls: bool = False,
) -> int:
    """
    Zapisuje listę stacji do wskazanej bazy MongoDB (lokalnej lub Atlas) z użyciem
    operacji masowej `bulk_write` oraz `UpdateOne({'station_id': ...}, {'$set': ...}, upsert=True)`.
    Zwraca całkowitą liczbę dokumentów w kolekcji `stations` po zakończeniu zapisu.
    """
    print(f"\n{'=' * 65}")
    print(f"[*] Zapisywanie stacji do: {target_name} (baza: '{db_name}', kolekcja: '{COLLECTION_NAME}')")
    print(f"{'=' * 65}")

    if not uri or "<username>" in uri or "<password>" in uri:
        raise ValueError(f"Brak poprawnego URI dla '{target_name}' w pliku .env!")

    client_kwargs = {"serverSelectionTimeoutMS": 10000}
    if use_tls:
        client_kwargs["tls"] = True

    client = MongoClient(uri, **client_kwargs)
    try:
        db = client[db_name]
        collection = db[COLLECTION_NAME]

        # Utworzenie unikalnego indeksu na polu `station_id` dla dodatkowej gwarancji spójności
        collection.create_index([("station_id", ASCENDING)], unique=True)

        # Przygotowanie listy operacji UpdateOne(..., upsert=True)
        operations = [
            UpdateOne(
                {"station_id": station["station_id"]},
                {"$set": station},
                upsert=True,
            )
            for station in stations
        ]

        if operations:
            result = collection.bulk_write(operations, ordered=False)
            print(
                f"[+] Wykonano bulk_write ({len(operations)} operacji UpdateOne z upsert=True):\n"
                f"    - Nowe dokumenty (upserted_count):       {result.upserted_count}\n"
                f"    - Dopasowane dokumenty (matched_count):  {result.matched_count}\n"
                f"    - Zmodyfikowane (modified_count):        {result.modified_count}"
            )

        total_docs = collection.count_documents({})
        print(f"[+] Liczba dokumentów w kolekcji '{db_name}.{COLLECTION_NAME}': {total_docs}")
        return total_docs

    except PyMongoError as err:
        print(f"[-] BŁĄD MongoDB ({target_name}): {err}", file=sys.stderr)
        raise
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scraper metadanych stacji WMO/NOAA zapisujący do lokalnego MongoDB i MongoDB Atlas (bulk_write + UpdateOne upsert)."
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        default=DEFAULT_COUNTRIES,
        help="Lista krajów do pobrania z tabeli HTML WMO (domyślnie 10 krajów europejskich).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Liczba uruchomień cyklu zapisu (np. --runs 3 dla automatycznej weryfikacji DoD).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        for run_idx in range(1, args.runs + 1):
            if args.runs > 1:
                print(f"\n{'#' * 65}")
                print(f"### URUCHOMIENIE #{run_idx} / {args.runs}")
                print(f"{'#' * 65}")

            # 1. Pobranie tabeli HTML z sieci i parsowanie za pomocą BeautifulSoup
            stations = fetch_wmo_stations_from_web(args.countries)

            # 2. Wzbogacenie o powiązania z kodami NOAA GSOD (jeśli plik isd-history.csv jest dostępny)
            stations = enrich_with_local_noaa_isd(
                stations,
                isd_csv_path=Path("archive/isd-history.csv"),
                country_txt_path=Path("archive/country-list.txt"),
            )

            # 3. Masowy zapis (bulk_write + UpdateOne z upsert=True) do lokalnego MongoDB
            local_count = save_stations_bulk_upsert(
                target_name="Lokalne MongoDB (Docker)",
                uri=MONGO_LOCAL_URI,
                db_name=MONGO_LOCAL_DB,
                stations=stations,
                use_tls=False,
            )

            # 4. Masowy zapis (bulk_write + UpdateOne z upsert=True) do MongoDB Atlas
            atlas_count = save_stations_bulk_upsert(
                target_name="MongoDB Atlas (Chmura)",
                uri=MONGO_ATLAS_URI,
                db_name=MONGO_ATLAS_DB,
                stations=stations,
                use_tls=True,
            )

            print(f"\n{'=' * 65}")
            print(f"[*] PODSUMOWANIE (Uruchomienie #{run_idx}):")
            print(f"    - Lokalne MongoDB ({MONGO_LOCAL_DB}.{COLLECTION_NAME}): {local_count} dokumentów")
            print(f"    - MongoDB Atlas   ({MONGO_ATLAS_DB}.{COLLECTION_NAME}): {atlas_count} dokumentów")

            if local_count == atlas_count and local_count == len(stations):
                print("[SUCCESS] Liczba dokumentów w obu bazach jest identyczna (brak duplikatów)!")
            else:
                print(
                    f"[!] UWAGA: Rozbieżność liczby dokumentów (local={local_count}, atlas={atlas_count}, scraped={len(stations)})",
                    file=sys.stderr,
                )
                sys.exit(1)
            print(f"{'=' * 65}")

    except Exception as exc:
        print(f"[-] BŁĄD KRYTYCZNY: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

