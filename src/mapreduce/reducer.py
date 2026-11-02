#!/usr/bin/env python3
"""
reducer.py - Skrypt Reducera dla Hadoop Streaming.

Wiedza teoretyczna ("Learn as I go"):
- *Hadoop. Kompletny przewodnik*, Rozdział 2 (s. 57-60) oraz Rozdział 7 (s. 202 - Shuffle & Sort):
  Gwarancją frameworka MapReduce (lub potoku `| sort |` w testach lokalnych) jest to, że dane
  trafiające na standardowe wejście Reducera są posortowane według klucza (`STN_ID`).
- Dzięki posortowaniu kluczy Reducer nie musi budować słownika wszystkich stacji w pamięci RAM!
  Pamięta jedynie aktualnie przetwarzany klucz w zmiennej `current_key` i akumuluje statystyki
  w kilku zmiennych skalarnych.
- Gdy na wejściu pojawia się nowy klucz (`key != current_key`), Reducer wypisuje podsumowanie
  dla dotychczasowego `current_key`, resetuje akumulatory i rozpoczyna liczenie dla nowego klucza.

Obliczane 3 analizy statystyczne dla każdej stacji (`STN_ID`):
1. Analiza temperatury (`TEMP`): średnia, minimalna i maksymalna temperatura (`avg_temp`, `min_temp`, `max_temp`).
2. Analiza wiatru (`WIND`): średnia i maksymalna prędkość wiatru (`avg_wind`, `max_wind`).
3. Analiza opadów (`PRCP`): całkowita suma opadów, maksymalny opad dzienny i liczba pomiarów (`total_prcp`, `max_prcp`, `count`).
"""

import sys


def emit_station_stats(
    station_id: str,
    count: int,
    sum_temp: float,
    min_temp: float,
    max_temp: float,
    sum_wind: float,
    max_wind: float,
    sum_prcp: float,
    max_prcp: float,
) -> None:
    """
    Oblicza finalne 3 analizy statystyczne z akumulatorów i wypisuje wynik na standardowe wyjście
    w formacie `STN_ID \t avg_temp,min_temp,max_temp,avg_wind,max_wind,total_prcp,max_prcp,count`.
    """
    if count <= 0:
        return

    avg_temp = sum_temp / count
    avg_wind = sum_wind / count
    total_prcp = sum_prcp

    # Czytelny i łatwy do dalszego parsowania (np. przy eksporcie do MongoDB) format CSV po tabulatorze:
    sys.stdout.write(
        "{}\tavg_temp={:.2f},min_temp={:.2f},max_temp={:.2f},"
        "avg_wind={:.2f},max_wind={:.2f},"
        "total_prcp={:.2f},max_prcp={:.2f},count={}\n".format(
            station_id,
            avg_temp,
            min_temp,
            max_temp,
            avg_wind,
            max_wind,
            total_prcp,
            max_prcp,
            count,
        )
    )


def main() -> None:
    current_key = None

    # Akumulatory do obliczania 3 statystyk na bieżąco (O(1) pamięci RAM)
    count = 0
    sum_temp = 0.0
    min_temp = float("inf")
    max_temp = float("-inf")
    sum_wind = 0.0
    max_wind = float("-inf")
    sum_prcp = 0.0
    max_prcp = float("-inf")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue

        key, val_str = parts[0], parts[1]

        try:
            temp_s, wind_s, prcp_s = val_str.split(",")
            temp = float(temp_s)
            wind = float(wind_s)
            prcp = float(prcp_s)
        except ValueError:
            continue

        # Jeśli klucz się zmienił, wypisujemy wyliczone statystyki dla poprzedniego klucza
        # i resetujemy akumulatory
        if current_key == key:
            count += 1
            sum_temp += temp
            if temp < min_temp:
                min_temp = temp
            if temp > max_temp:
                max_temp = temp

            sum_wind += wind
            if wind > max_wind:
                max_wind = wind

            sum_prcp += prcp
            if prcp > max_prcp:
                max_prcp = prcp
        else:
            if current_key is not None:
                emit_station_stats(
                    current_key,
                    count,
                    sum_temp,
                    min_temp,
                    max_temp,
                    sum_wind,
                    max_wind,
                    sum_prcp,
                    max_prcp,
                )

            # Reset zmiennych dla nowego klucza
            current_key = key
            count = 1
            sum_temp = temp
            min_temp = temp
            max_temp = temp
            sum_wind = wind
            max_wind = wind
            sum_prcp = prcp
            max_prcp = prcp

    # Wypisanie statystyk dla ostatniego klucza po zakończeniu pętli sys.stdin
    if current_key is not None:
        emit_station_stats(
            current_key,
            count,
            sum_temp,
            min_temp,
            max_temp,
            sum_wind,
            max_wind,
            sum_prcp,
            max_prcp,
        )


if __name__ == "__main__":
    main()

