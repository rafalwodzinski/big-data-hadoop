#!/usr/bin/env bash
# run_mapreduce.sh - Uruchomienie zadania MapReduce (Hadoop Streaming) na klastrze YARN
# dla 5.5-gigabajtowego zbioru danych w HDFS (/data/gsod_full.csv) oraz pobranie
# wyniku do pliku results.tsv na maszynę hosta.

set -euo pipefail

echo "[*] 1. Sprawdzanie środowiska Python 3 w kontenerach Hadoop (namenode i nodemanager)..."
for container in namenode nodemanager; do
    if ! docker exec "$container" which python3 >/dev/null 2>&1; then
        echo "    [!] Instalowanie python3 w kontenerze '$container'..."
        docker exec "$container" bash -c "sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g; s|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g; /stretch-updates/d' /etc/apt/sources.list && apt-get -o Acquire::Check-Valid-Until=false update -qq && apt-get install -y -qq --allow-unauthenticated python3"
    else
        echo "    [+] python3 jest już zainstalowany w '$container'."
    fi
done

echo "[*] 2. Przygotowanie dowiązania hadoop-streaming.jar i czyszczenie poprzedniego katalogu /output w HDFS..."
docker exec -w /workspace namenode bash -c "
    ln -sf /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar /opt/hadoop-3.2.1/hadoop-streaming.jar
    hdfs dfs -rm -r -f /output
"

echo "[*] 3. Uruchamianie zadania Hadoop Streaming MapReduce dla katalogu /data/..."
docker exec -w /workspace namenode hadoop jar /opt/hadoop-3.2.1/hadoop-streaming.jar \
    -files mapper.py,reducer.py \
    -input /data/ \
    -output /output \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"

echo "[*] 4. Pobieranie wyniku z HDFS (/output/part-00000) do pliku results.tsv..."
docker exec -w /workspace namenode hdfs dfs -get -f /output/part-00000 results.tsv

echo "[+] SUKCES! Zadanie MapReduce zakończone, a plik results.tsv zapisany na dysku hosta:"
ls -lh results.tsv
head -n 5 results.tsv

