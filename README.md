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

### 4. Podstawowe komendy HDFS

Wykonywanie poleceń wewnątrz kontenera `namenode`:
```bash
# Utworzenie katalogu w HDFS
docker exec namenode hdfs dfs -mkdir -p /dane

# Wyświetlenie zawartości katalogu
docker exec namenode hdfs dfs -ls /

# Raport klastra i DataNode
docker exec namenode hdfs dfsadmin -report
```
