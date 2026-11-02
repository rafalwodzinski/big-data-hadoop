# Big Data - Projekt Hadoop & MongoDB

Projekt przetwarzania dużych zbiorów danych (dane stacji pogodowych NOAA GSOD) z wykorzystaniem klastra Apache Hadoop (HDFS, YARN MapReduce) oraz bazy NoSQL MongoDB (lokalnej w Dockerze oraz chmurowej MongoDB Atlas).

---

## Struktura projektu

```text
big-data-hadoop/
├── docker-compose.yml              # Konfiguracja klastra Hadoop (HDFS + YARN) i MongoDB
├── hadoop.env                      # Parametry konfiguracyjne demonów Hadoop
├── environment.yml                 # Definicja środowiska Conda (bigdata_hadoop)
├── .env.example                    # Szablon zmiennych środowiskowych dla MongoDB Local & Atlas
│
├── src/
│   ├── database/
│   │   ├── db_test.py              # Weryfikacja połączeń z lokalnym MongoDB i MongoDB Atlas (TLS)
│   │   └── load_results.py         # Eksport zagregowanych wyników MapReduce do MongoDB (insert_many)
│   ├── hdfs/
│   │   └── prepare_hdfs_data.py    # Strumieniowe łączenie archiwów .tar/.op.gz i wysyłka >5GB do HDFS
│   ├── scraper/
│   │   └── scraper.py              # Web scraping metadanych stacji WMO/NOAA (bulk_write + UpdateOne upsert)
│   └── mapreduce/
│       ├── mapper.py               # Mapper (Hadoop Streaming) - czyszczenie i ekstrakcja pomiarów
│       └── reducer.py              # Reducer (Hadoop Streaming) - agregacja 3 analiz statystycznych
│
├── scripts/
│   └── run_mapreduce.sh            # Skrypt uruchamiający zadanie Hadoop Streaming na klastrze YARN
│
├── data/
│   ├── gsod_full.csv               # Złączony plik danych GSOD (>5.5 GB, ignorowany w Git)
│   ├── sample.csv                  # Próbka testowa 1000 wierszy (ignorowana w Git)
│   └── output/
│       └── results.tsv             # Wynik obliczeń MapReduce pobrany z HDFS (14 383 stacji)
│
├── archive/                        # Surowe archiwa roczne NOAA GSOD .tar (ignorowane w Git)
└── docs/
    └── charts/                     # Zrzuty ekranu i wykresy z MongoDB Compass / Atlas Charts
```

---

## Środowisko uruchomieniowe

### 1. Środowisko Conda (Python)
Konfiguracja środowiska zdefiniowana jest w pliku `environment.yml` (Python 3.11, `pymongo`, `pandas`, `requests`, `beautifulsoup4`, `python-dotenv`, `hdfs`).

```bash
# Odtworzenie środowiska od zera
conda env create -f environment.yml

# Aktywacja środowiska
conda activate bigdata_hadoop
```

### 2. Uruchomienie klastra (Docker Compose)
Klaster Hadoop oraz lokalna baza MongoDB uruchamiane są za pomocą Docker Compose:

```bash
docker compose up -d
```

Interfejsy webowe klastra:
- **Hadoop NameNode (HDFS UI):** [http://localhost:9870](http://localhost:9870)
- **Hadoop YARN ResourceManager:** [http://localhost:8088](http://localhost:8088)
- **Hadoop HistoryServer:** [http://localhost:8188](http://localhost:8188)
- **MongoDB Lokalne:** `localhost:27017`

---

## Kolejne etapy potoku przetwarzania (Pipeline)

### Etap 1: Test połączenia baz danych (`src/database/db_test.py`)
Weryfikuje połączenie z lokalnym MongoDB oraz szyfrowane połączenie (`tls=True`) z klastrem MongoDB Atlas:

```bash
cp .env.example .env
# Uzupełnij MONGO_ATLAS_URI w pliku .env
python src/database/db_test.py
```

### Etap 2: Strumieniowe przygotowanie danych i wysyłka do HDFS (`src/hdfs/prepare_hdfs_data.py`)
Strumieniowo odczytuje archiwa roczne `.tar` i zagnieżdżone pliki `.op.gz` (`tarfile.extractfile()` + `gzip.GzipFile`) bez ładowania ich do pamięci RAM, odfiltrowuje zduplikowane nagłówki `STN---`, tworzy plik `data/gsod_full.csv` (> 5.0 GB) i wysyła go do HDFS (`/data/gsod_full.csv`):

```bash
python src/hdfs/prepare_hdfs_data.py

# Weryfikacja rozmiaru w HDFS (Definition of Done > 5.0 GB):
docker exec namenode hdfs dfs -du -h /data/
```

### Etap 3: Scraping metadanych stacji WMO/NOAA (`src/scraper/scraper.py`)
Pobiera tabele HTML stacji meteorologicznych WMO przy użyciu `requests` i `BeautifulSoup` (`<table>` / `<tr>`), wzbogaca je o powiązania z kodami NOAA GSOD (`station_id`) i zapisuje masowo (`bulk_write` + `UpdateOne(..., upsert=True)`) do kolekcji `stations` w lokalnym MongoDB oraz MongoDB Atlas:

```bash
# Pojedyncze uruchomienie
python src/scraper/scraper.py

# Trzykrotne uruchomienie weryfikujące idempotentność / brak duplikatów (DoD)
python src/scraper/scraper.py --runs 3
```

### Etap 4: Przetwarzanie strumieniowe MapReduce (`src/mapreduce/mapper.py` & `reducer.py`)
Skrypty `mapper.py` i `reducer.py` realizują czyszczenie brakujących pomiarów (`9999.9`, `999.9`, `99.99`) oraz obliczanie 3 analiz statystycznych (temperatura, wiatr, opady) bez ładowania pliku do pamięci RAM:

```bash
# Lokalny test potoku Map -> Shuffle & Sort -> Reduce na próbce 1000 wierszy (DoD)
cat data/sample.csv | python3 src/mapreduce/mapper.py | sort | python3 src/mapreduce/reducer.py
```

### Etap 5: Uruchomienie zadania Hadoop Streaming na klastrze YARN (`scripts/run_mapreduce.sh`)
Przetwarza pełny 5.5-gigabajtowy zbiór `/data/gsod_full.csv` (44 splity po 128 MB) na klastrze YARN i pobiera wynik z HDFS (`/output/part-00000`) do pliku `data/output/results.tsv`:

```bash
bash scripts/run_mapreduce.sh
```

### Etap 6: Eksport wyników MapReduce do MongoDB (`src/database/load_results.py`)
Parsuje plik `data/output/results.tsv` i przesyła wszystkie 14 383 zagregowane dokumenty stacji metodą `collection.insert_many()` do kolekcji `weather_results` w MongoDB Atlas oraz lokalnym MongoDB:

```bash
python src/database/load_results.py
```

---

## Wizualizacja w MongoDB Compass / Atlas Charts

Po połączeniu aplikacji **MongoDB Compass** z klastrem Atlas (baza `bigdata_atlas`, kolekcja `weather_results`) można wygenerować 3 wizualizacje:

1. **Analiza 1 – Rozkład i ekstrema temperatur (`avg_temp`, `min_temp`, `max_temp`):**
   - Zakładka **Schema -> Analyze Schema** lub zakładka **Aggregations**:
     ```json
     [
       { "$lookup": { "from": "stations", "localField": "station_id", "foreignField": "station_id", "as": "station" } },
       { "$unwind": "$station" },
       { "$match": { "count": { "$gte": 365 } } },
       { "$project": { "_id": 0, "station_id": 1, "name": "$station.name", "country": "$station.country", "avg_temp": 1, "min_temp": 1, "max_temp": 1 } },
       { "$sort": { "avg_temp": -1 } },
       { "$limit": 20 }
     ]
     ```
2. **Analiza 2 – Analiza prędkości wiatru (`avg_wind`, `max_wind`):**
   - Histogram pól `avg_wind` i `max_wind` w zakładce **Schema**, lub sortowanie `{ "$sort": { "max_wind": -1 } }`.
3. **Analiza 3 – Suma opadów i zestawienie według krajów (`total_prcp`, `max_prcp`):**
   - Potok agregujący w zakładce **Aggregations**:
     ```json
     [
       { "$lookup": { "from": "stations", "localField": "station_id", "foreignField": "station_id", "as": "station" } },
       { "$unwind": "$station" },
       { "$group": { "_id": "$station.country", "avg_country_temp": { "$avg": "$avg_temp" }, "avg_country_wind": { "$avg": "$avg_wind" }, "total_country_prcp": { "$sum": "$total_prcp" }, "stations_count": { "$sum": 1 } } },
       { "$sort": { "total_country_prcp": -1 } }
     ]
     ```
