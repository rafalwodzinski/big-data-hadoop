#!/usr/bin/env python3
"""
mapper.py - Skrypt Mappera dla Hadoop Streaming.

Wiedza teoretyczna ("Learn as I go"):
- *Hadoop. Kompletny przewodnik*, Rozdział 2 (s. 57-60): Hadoop Streaming przekazuje dane
  wejściowe do procesu Mappera linia po linii przez standardowe wejście (`sys.stdin`) i
  odbiera wyjście ze standardowego wyjścia (`sys.stdout`).
- Mapper nie wczytuje całego pliku do pamięci RAM (`for line in sys.stdin:` przetwarza
  strumieniowo jedną linię naraz, zużywając O(1) pamięci).
- Czyszczenie danych (Data Cleaning): w formacie NOAA GSOD brakujące pomiary oznaczane są
  ciągami dziewiątek: `9999.9` (dla temperatury TEMP), `999.9` (dla prędkości wiatru WDSP/MXSPD)
  oraz `99.99` (dla opadów PRCP, które dodatkowo posiadają jednoliterową flagę A-I na końcu,
  np. `0.00G` lub `99.99`).
- Format wyjściowy Mappera: `STN_ID \t TEMP,WIND,PRCP`
"""

import sys


def clean_prcp(prcp_raw: str) -> float:
    """
    Oczyszcza pole PRCP (opady w calach, poz. 119-124 w GSOD).
    Usuwa flagę literową z końca (np. '0.09G' -> '0.09').
    Zwraca -1.0, jeśli wartość jest brakująca ('99.99', '999.9', '9999.9').
    """
    val_str = prcp_raw.strip().rstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ*")
    if not val_str or val_str in ("99.99", "999.9", "9999.9"):
        return -1.0
    try:
        val = float(val_str)
        if val >= 99.9:
            return -1.0
        return val
    except ValueError:
        return -1.0


def main() -> None:
    # Strumieniowy odczyt wyłącznie przez `for line in sys.stdin:` (bez ładowania pliku do RAM)
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith("STN---"):
            continue

        parts = line.split()
        # Rekord GSOD zawiera 22 kolumny oddzielone białymi znakami:
        # [0]=STN---, [1]=WBAN, [2]=YEARMODA, [3]=TEMP, [13]=WDSP, [19]=PRCP
        if len(parts) < 20:
            continue

        stn_id = parts[0]
        temp_raw = parts[3]
        wind_raw = parts[13]
        prcp_raw = parts[19]

        # Odrzucenie wpisów z brakującą wartością 9999.9 (oraz odpowiedników braków w GSOD)
        if temp_raw == "9999.9":
            continue

        try:
            temp = float(temp_raw)
        except ValueError:
            continue

        # Dodatkowa asekuracja na wypadek zapisu 9999.90
        if abs(temp - 9999.9) < 1e-4:
            continue

        # Czyszczenie prędkości wiatru (WDSP - brak danych w GSOD to 999.9 lub 9999.9)
        if wind_raw in ("999.9", "9999.9"):
            continue
        try:
            wind = float(wind_raw)
        except ValueError:
            continue
        if wind >= 999.9:
            continue

        # Czyszczenie opadów (PRCP - brak danych to 99.99; jeśli brak raportu opadów,
        # w wielu stacjach GSOD oznacza to dzień bezopadowy 0.00 lub brak pomiaru -
        # odrzucamy wpisy z niepoprawnym PRCP lub normalizujemy)
        prcp = clean_prcp(prcp_raw)
        if prcp < 0.0:
            continue

        # Wypisanie pary klucz \t wartość: STN_ID \t TEMP,WIND,PRCP
        sys.stdout.write(f"{stn_id}\t{temp:.2f},{wind:.2f},{prcp:.2f}\n")


if __name__ == "__main__":
    main()

