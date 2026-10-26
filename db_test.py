#!/usr/bin/env python3
"""
db_test.py - Weryfikacja połączenia z lokalnym MongoDB oraz MongoDB Atlas.

Zgodnie z wymaganiami zadania #2:
- Łączy się z lokalną bazą MongoDB (localhost:27017)
- Łączy się z klastrem MongoDB Atlas z wymuszeniem szyfrowania (tls=True)
- Wyświetla listę kolekcji z obu serwerów bazodanowych
"""

import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Wczytanie konfiguracji z pliku .env
load_dotenv()

MONGO_LOCAL_URI = os.getenv("MONGO_LOCAL_URI", "mongodb://localhost:27017/")
MONGO_LOCAL_DB = os.getenv("MONGO_LOCAL_DB", "bigdata_local")

MONGO_ATLAS_URI = os.getenv("MONGO_ATLAS_URI") or os.getenv("MONGODB_URI", "")
MONGO_ATLAS_DB = os.getenv("MONGO_ATLAS_DB", "bigdata_atlas")


def test_connection(name: str, uri: str, db_name: str, use_tls: bool = False) -> list[str]:
    print(f"\n{'=' * 55}")
    print(f"[*] Testowanie połączenia: {name}")
    print(f"{'=' * 55}")

    if not uri or "<username>" in uri or "<password>" in uri:
        raise ValueError(
            f"Nieprawidłowy lub nieustawiony URI dla {name} w pliku .env!\n"
            f"Aktualna wartość: '{uri}'\n"
            "Uzupełnij zmienną w pliku .env swoimi danymi z MongoDB Atlas."
        )

    # Przygotowanie parametrów MongoClient
    # Wymóg zadania: konstrukcja MongoClient i opcja tls=True dla połączeń szyfrowanych
    client_kwargs = {
        "serverSelectionTimeoutMS": 7000,
    }
    if use_tls:
        client_kwargs["tls"] = True

    try:
        client = MongoClient(uri, **client_kwargs)
        # Test komunikacji (ping)
        client.admin.command("ping")
        server_info = client.server_info()
        print(f"[+] Połączono pomyślnie z {name}!")
        print(f"    Wersja MongoDB: {server_info.get('version', 'nieznana')}")
        print(f"    Szyfrowanie TLS: {'Włączone (tls=True)' if use_tls else 'Domyślne'}")

        db = client[db_name]

        # Jeśli baza jest pusta, utwórzmy wpis w kolekcji testowej,
        # aby upewnić się, że istnieje co najmniej jedna kolekcja do wylistowania
        existing_collections = db.list_collection_names()
        if not existing_collections:
            print(f"    Baza '{db_name}' jest nowa – inicjalizuję próbną kolekcję 'connection_check'...")
            test_col = db["connection_check"]
            test_col.insert_one({
                "test": "ok",
                "timestamp": datetime.now(timezone.utc),
                "source": name
            })
            existing_collections = db.list_collection_names()

        print(f"    Bazy danych na serwerze: {client.list_database_names()}")
        print(f"    Kolekcje w bazie '{db_name}': {existing_collections}")
        return existing_collections

    except (ConnectionFailure, ServerSelectionTimeoutError) as err:
        print(f"[-] BŁĄD POŁĄCZENIA z {name}: {err}", file=sys.stderr)
        raise
    except Exception as err:
        print(f"[-] NIEOCZEKIWANY BŁĄD dla {name}: {err}", file=sys.stderr)
        raise
    finally:
        try:
            client.close()
        except Exception:
            pass


def main():
    print("=== WERYFIKACJA POŁĄCZEŃ MONGODB (Lokalne + Atlas) ===")

    has_error = False

    # 1. Test lokalnego MongoDB
    try:
        local_cols = test_connection(
            name="Lokalne MongoDB (Docker)",
            uri=MONGO_LOCAL_URI,
            db_name=MONGO_LOCAL_DB,
            use_tls=False
        )
        print(f"[OK] Lokalne MongoDB: znaleziono {len(local_cols)} kolekcji.")
    except Exception:
        has_error = True

    # 2. Test MongoDB Atlas
    try:
        atlas_cols = test_connection(
            name="MongoDB Atlas (Cloud)",
            uri=MONGO_ATLAS_URI,
            db_name=MONGO_ATLAS_DB,
            use_tls=True
        )
        print(f"[OK] MongoDB Atlas: znaleziono {len(atlas_cols)} kolekcji.")
    except Exception:
        has_error = True

    print(f"\n{'=' * 55}")
    if has_error:
        print("[!] Co najmniej jeden test zakończył się błędem.")
        sys.exit(1)
    else:
        print("[SUCCESS] Oba połączenia działają poprawnie!")
        print("Definition of Done (DoD) spełnione.")
        sys.exit(0)


if __name__ == "__main__":
    main()
