# Big Data - Projekt Hadoop & MongoDB

Projekt przetwarzania dużych zbiorów danych (dane stacji pogodowych GSOD) z wykorzystaniem klastra Apache Hadoop (HDFS, YARN MapReduce) oraz bazy MongoDB.

## Środowisko uruchomieniowe

### 1. Środowisko Conda (Python)
Konfiguracja środowiska zdefiniowana jest w pliku `environment.yml` (Python 3.11, `pymongo`, `pandas`, `requests`, `beautifulsoup4`, `hdfs`).

Odtworzenie środowiska od zera:
```bash
conda env create -f environment.yml
```

Aktywacja:
```bash
conda activate bigdata_hadoop
```

Aktualizacja pakietów po zmianach w `environment.yml`:
```bash
conda env update -f environment.yml --prune
```

---

### 2. Uruchomienie klastra (Docker Compose)
Klaster Hadoop oraz lokalna baza MongoDB uruchamiane są za pomocą Docker Compose:

```bash
docker compose up -d
```

Zatrzymanie klastra:
```bash
docker compose down
```

Podgląd uruchomionych usług:
```bash
docker ps
```

---

### 3. Interfejsy Webowe i Bazy Danych
- **Hadoop NameNode (HDFS UI):** [http://localhost:9870](http://localhost:9870)
- **Hadoop YARN ResourceManager:** [http://localhost:8088](http://localhost:8088)
- **Hadoop HistoryServer:** [http://localhost:8188](http://localhost:8188)
- **MongoDB Lokalne:** `localhost:27017`
- **MongoDB Atlas (Chmura):** `mongodb+srv://...`

### 4. Test połączenia baz danych (Lokalne + Atlas)
Skrypt `db_test.py` weryfikuje szyfrowane połączenie i wyświetla kolekcje:
```bash
# 1. Skonfiguruj plik .env na podstawie .env.example
cp .env.example .env
# Uzupełnij MONGO_ATLAS_URI w pliku .env

# 2. Uruchom test
python db_test.py
```

---

### 5. Przygotowanie i załadowanie danych GSOD do HDFS (`prepare_hdfs_data.py`)

Skrypt `prepare_hdfs_data.py` strumieniowo rozpakowuje archiwa roczne `.tar` i zagnieżdżone w nich pliki `.op.gz` (z wykorzystaniem `tarfile.extractfile()` oraz wrappera `gzip.GzipFile` bez ładowania danych do pamięci RAM), odfiltrowuje zduplikowane nagłówki `STN---`, zapisuje zbiorczy plik `gsod_full.csv` (> 5.0 GB) na dysk i przesyła go do HDFS (`/data/`):

```bash
conda activate bigdata_hadoop
python prepare_hdfs_data.py
```

Weryfikacja rozmiaru załadowanego pliku w HDFS (Definition of Done):
```bash
docker exec namenode hdfs dfs -du -h /data/
```

---

### 6. Scraping metadanych stacji meteorologicznych WMO/NOAA (`scraper.py`)

Skrypt `scraper.py` pobiera tabele HTML stacji meteorologicznych WMO z otwartego katalogu (Ogimet / WMO) przy użyciu `requests` i `BeautifulSoup` (wyszukiwanie `<table>` i `<tr>`), wzbogaca je o powiązania z kodami NOAA GSOD (`station_id`) i zapisuje masowo (`bulk_write` + `UpdateOne(..., upsert=True)`) do kolekcji `stations` w lokalnym MongoDB oraz w chmurze MongoDB Atlas:

```bash
conda activate bigdata_hadoop

# Pojedyncze uruchomienie
python scraper.py

# Trzykrotne uruchomienie weryfikujące brak duplikatów (Definition of Done)
python scraper.py --runs 3
```

---

### 7. Przetwarzanie strumieniowe MapReduce (`mapper.py` & `reducer.py`)

Skrypty `mapper.py` oraz `reducer.py` realizują logikę czyszczenia danych i obliczania 3 analiz statystycznych (temperatura, wiatr, opady) w architekturze Hadoop Streaming bez ładowania całych plików do pamięci RAM (`for line in sys.stdin:` oraz grupowanie po `current_key`):

```bash
# Utworzenie próbki testowej (1000 wierszy) z gsod_full.csv
head -n 1000 gsod_full.csv > sample.csv

# Lokalny test potoku Map -> Shuffle & Sort -> Reduce (Definition of Done)
cat sample.csv | python3 mapper.py | sort | python3 reducer.py
```

---

### 8. Uruchomienie zadania Hadoop Streaming na klastrze YARN (`run_mapreduce.sh`)

Przetworzenie pełnego 5.5-gigabajtowego pliku `/data/gsod_full.csv` (44 splity po 128 MB) za pomocą Hadoop Streaming na klastrze YARN oraz pobranie wyników z HDFS (`/output/part-00000`) na dysk fizyczny hosta do pliku `results.tsv`:

```bash
# Uruchomienie za pomocą przygotowanego skryptu:
bash run_mapreduce.sh

# Lub bezpośrednio komendami Hadoop Streaming i HDFS:
docker exec -w /workspace namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -files mapper.py,reducer.py \
    -input /data/ \
    -output /output \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"

docker exec -w /workspace namenode hdfs dfs -get -f /output/part-00000 results.tsv
```

Status zadania w YARN ResourceManager UI: [http://localhost:8088](http://localhost:8088) (`Final-State: SUCCEEDED`).

---

### 9. Eksport zagregowanych wyników MapReduce do MongoDB (`load_results.py`)

Skrypt `load_results.py` parsuje pobrany z HDFS plik `results.tsv` do tablicy słowników i przesyła wszystkie 14 383 zagregowane rekordy stacji metodą `collection.insert_many()` do kolekcji `weather_results` w chmurze MongoDB Atlas (`bigdata_atlas.weather_results`) oraz w lokalnej bazie MongoDB (`bigdata_local.weather_results`):

```bash
conda activate bigdata_hadoop
python load_results.py
```

---

### 10. Podstawowe komendy HDFS

Wykonywanie poleceń wewnątrz kontenera `namenode`:
```bash
# Utworzenie katalogu w HDFS
docker exec namenode hdfs dfs -mkdir -p /data

# Wysłanie pliku z zamontowanego katalogu roboczego (/workspace) do HDFS
docker exec -w /workspace namenode hdfs dfs -put gsod_full.csv /data/

# Sprawdzenie rozmiaru plików w katalogu /data/
docker exec namenode hdfs dfs -du -h /data/

# Wyświetlenie zawartości katalogu głównego
docker exec namenode hdfs dfs -ls /

# Raport klastra i DataNode
docker exec namenode hdfs dfsadmin -report
```
