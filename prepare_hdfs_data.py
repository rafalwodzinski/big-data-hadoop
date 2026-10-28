#!/usr/bin/env python3
"""
prepare_hdfs_data.py - Strumieniowe rozpakowywanie archiwów GSOD (.tar -> .op.gz)
i łączenie małych plików w duże pliki tekstowe bez ładowania ich do pamięci RAM,
wraz z przesłaniem pliku wynikowego (>5 GB) do klastra HDFS (/data/).

Wiedza teoretyczna ("Learn as I go"):
- Wykorzystuje moduł `tarfile` (metoda `extractfile`) do uzyskania strumienia bajtów
  z archiwum .tar bez wypakowywania małych plików na dysk.
- Wykorzystuje moduł `gzip` (`gzip.open` / `gzip.GzipFile`) jako wrapper strumieniowy
  na obiekcie pliku zwróconym przez `extractfile()`.
- Rozwiązuje "Small Files Problem" w HDFS (Rozdział 11, str. 447-449 książki
  "Przetwarzanie danych w dużej skali"): zamiast setek tysięcy małych plików, które
  obciążyłyby pamięć RAM NameNode'a i utworzyły gigantyczną liczbę zadań Map,
  tworzymy duży plik ciągły dzielony na bloki HDFS (np. 128 MB).
"""

from __future__ import annotations

import argparse
import gzip
import os
import subprocess
import sys
import tarfile
import time
import zipfile
from pathlib import Path

# Domyślne ścieżki i parametry
DEFAULT_ZIP_PATH = Path("archive/gsod_all_years.zip")
DEFAULT_ARCHIVE_DIR = Path("archive/gsod_all_years")
DEFAULT_OUTPUT_FILE = Path("gsod_full.csv")
DEFAULT_HDFS_DIR = "/data/"
DEFAULT_TARGET_GB = 5.5  # Docelowy rozmiar w GiB (> 5.0 GB dla spełnienia DoD)
HEADER_PREFIX = b"STN---"


def ensure_zip_extracted(zip_path: Path, extract_dir: Path) -> Path:
    """
    Sprawdza, czy katalog z plikami .tar już istnieje.
    Jeśli nie, a dostępny jest plik gsod_all_years.zip, rozpakowuje go strumieniowo.
    """
    if extract_dir.exists() and any(extract_dir.glob("*.tar")):
        tar_count = len(list(extract_dir.glob("*.tar")))
        print(f"[*] Katalog '{extract_dir}' już istnieje i zawiera {tar_count} plików .tar.")
        return extract_dir

    if zip_path.exists():
        print(f"[*] Rozpakowywanie archiwum '{zip_path}' do '{extract_dir.parent}'...")
        extract_dir.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir.parent)
        print(f"[+] Rozpakowano '{zip_path}'.")
        return extract_dir

    raise FileNotFoundError(
        f"Nie znaleziono ani rozpakowanego katalogu '{extract_dir}', "
        f"ani archiwum '{zip_path}'!"
    )


def stream_tar_to_merged_files(
    archive_dir: Path,
    output_file: Path,
    target_size_gb: float | None = DEFAULT_TARGET_GB,
    chunk_size_gb: float | None = None,
    newest_first: bool = True,
) -> list[Path]:
    """
    Strumieniowo odczytuje zawartość archiwów .tar i zagnieżdżonych w nich plików .op.gz,
    odfiltrowuje zduplikowane nagłówki (linie zaczynające się od 'STN---') i zapisuje
    dane bezpośrednio na dysk bez ładowania całych plików do pamięci RAM.

    Parametry:
    - archive_dir: katalog z plikami gsod_YYYY.tar
    - output_file: główny plik wynikowy (np. gsod_full.csv)
    - target_size_gb: docelowy rozmiar w GiB, po osiągnięciu którego przerywamy
      (ustaw None lub 0, aby przetworzyć absolutnie wszystkie lata 1901-2019)
    - chunk_size_gb: jeśli podano, dzieli wyjście na kilka dużych plików o zadanym rozmiarze
    - newest_first: jeśli True, zaczyna od najnowszych roczników (największe archiwa),
      dzięki czemu szybciej osiąga wymagany próg > 5.0 GB.
    """
    tar_files = sorted(archive_dir.glob("gsod_*.tar"), reverse=newest_first)
    if not tar_files:
        raise FileNotFoundError(f"Brak plików gsod_*.tar w katalogu '{archive_dir}'!")

    target_bytes = int(target_size_gb * (1024**3)) if target_size_gb and target_size_gb > 0 else None
    chunk_bytes = int(chunk_size_gb * (1024**3)) if chunk_size_gb and chunk_size_gb > 0 else None

    created_files: list[Path] = []
    part_idx = 1

    def get_part_path(idx: int) -> Path:
        if chunk_bytes is None or idx == 1:
            return output_file
        stem = output_file.stem
        suffix = output_file.suffix
        return output_file.with_name(f"{stem}_part{idx}{suffix}")

    current_path = get_part_path(part_idx)
    created_files.append(current_path)

    print(f"\n{'=' * 65}")
    print("[*] Rozpoczynam strumieniowe łączenie plików .op.gz z archiwów .tar")
    print(f"    Źródło:            {archive_dir} ({len(tar_files)} archiwów .tar)")
    print(f"    Plik docelowy:     {current_path}")
    if target_bytes:
        print(f"    Limit rozmiaru:    {target_size_gb:.2f} GiB ({target_bytes:,} bajtów)")
    else:
        print("    Limit rozmiaru:    Brak (przetwarzanie wszystkich roczników)")
    print(f"{'=' * 65}\n")

    total_bytes_written = 0
    current_file_bytes = 0
    total_stations_processed = 0
    total_lines_written = 0
    header_written_in_current_file = False
    start_time = time.time()

    out_f = open(current_path, "wb", buffering=1024 * 1024)  # Bufor zapisu 1 MB

    try:
        for tar_idx, tar_path in enumerate(tar_files, start=1):
            tar_start = time.time()
            stations_in_tar = 0

            # Otwieramy archiwum .tar w trybie odczytu
            with tarfile.open(tar_path, mode="r") as tar:
                for member in tar:
                    if not (member.isfile() and member.name.endswith(".op.gz")):
                        continue

                    # extractfile() zwraca obiekt pliku (strumień) w pamięci bez zapisu na dysk
                    raw_member_stream = tar.extractfile(member)
                    if raw_member_stream is None:
                        continue

                    # Używamy gzip jako wrappera na obiekcie pliku (strumieniowa dekompresja)
                    with gzip.GzipFile(fileobj=raw_member_stream, mode="rb") as gz_stream:
                        for line in gz_stream:
                            # Odfiltrowanie zduplikowanych nagłówków zaczynających się od STN---
                            if line.startswith(HEADER_PREFIX):
                                if not header_written_in_current_file:
                                    out_f.write(line)
                                    line_len = len(line)
                                    total_bytes_written += line_len
                                    current_file_bytes += line_len
                                    total_lines_written += 1
                                    header_written_in_current_file = True
                                continue

                            out_f.write(line)
                            line_len = len(line)
                            total_bytes_written += line_len
                            current_file_bytes += line_len
                            total_lines_written += 1

                    stations_in_tar += 1
                    total_stations_processed += 1

                    # Rotacja pliku, jeśli włączono dzielenie na kilka dużych plików (chunk_size_gb)
                    if chunk_bytes and current_file_bytes >= chunk_bytes:
                        out_f.flush()
                        out_f.close()
                        print(
                            f"[+] Zamknięto plik częściowy '{current_path}' "
                            f"({current_file_bytes / (1024**3):.2f} GiB)"
                        )
                        if target_bytes and total_bytes_written >= target_bytes:
                            break
                        part_idx += 1
                        current_path = get_part_path(part_idx)
                        created_files.append(current_path)
                        out_f = open(current_path, "wb", buffering=1024 * 1024)
                        current_file_bytes = 0
                        header_written_in_current_file = False

                    # Sprawdzenie czy osiągnęliśmy docelowy rozmiar (> 5 GB)
                    if target_bytes and total_bytes_written >= target_bytes:
                        break

            elapsed_tar = time.time() - tar_start
            total_gb = total_bytes_written / (1024**3)
            print(
                f"[{tar_idx:3d}/{len(tar_files)}] Przetworzono {tar_path.name} "
                f"(stacji: {stations_in_tar:5d}, czas: {elapsed_tar:4.1f}s) | "
                f"Łącznie zapisano: {total_gb:.2f} GiB ({total_lines_written:,} linii)"
            )

            if target_bytes and total_bytes_written >= target_bytes:
                print(
                    f"\n[+] Osiągnięto docelowy rozmiar danych: "
                    f"{total_gb:.2f} GiB >= {target_size_gb:.2f} GiB."
                )
                break

    finally:
        if not out_f.closed:
            out_f.flush()
            out_f.close()

    total_elapsed = time.time() - start_time
    print(f"\n{'=' * 65}")
    print("[+] Zakończono generowanie pliku wynikowego na dysku!")
    print(f"    Utworzone pliki:       {[str(p) for p in created_files]}")
    print(f"    Przetworzono stacji:   {total_stations_processed:,}")
    print(f"    Zapisano linii:        {total_lines_written:,}")
    print(f"    Łączny rozmiar:        {total_bytes_written / (1024**3):.2f} GiB ({total_bytes_written:,} B)")
    print(f"    Czas wykonania:        {total_elapsed:.1f} s")
    print(f"{'=' * 65}\n")

    return created_files


def run_hdfs_command(args_list: list[str], use_docker_exec: bool = False) -> subprocess.CompletedProcess:
    """
    Uruchamia komendę `hdfs dfs ...` bezpośrednio (jeśli klient hdfs jest w PATH)
    lub poprzez `docker exec namenode hdfs dfs ...`.
    """
    if use_docker_exec:
        cmd = ["docker", "exec", "namenode"] + args_list
    else:
        cmd = args_list

    print(f"[*] Wykonywanie: {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, text=True, capture_output=True)


def upload_to_hdfs(local_file: Path, hdfs_dir: str = DEFAULT_HDFS_DIR) -> None:
    """
    Wysyła plik wynikowy na klaster HDFS do katalogu /data/ za pomocą komendy
    `hdfs dfs -put gsod_full.csv /data/` oraz weryfikuje DoD komendą `hdfs dfs -du -h /data/`.
    """
    if not local_file.exists():
        raise FileNotFoundError(f"Plik lokalny '{local_file}' nie istnieje!")

    # Sprawdzamy czy jesteśmy wewnątrz kontenera / mamy natywne CLI `hdfs`,
    # czy uruchamiamy ze środowiska hosta przez `docker exec namenode`
    has_local_hdfs_cli = subprocess.run(
        ["which", "hdfs"], capture_output=True, text=True
    ).returncode == 0

    use_docker = not has_local_hdfs_cli
    target_hdfs_path = f"{hdfs_dir.rstrip('/')}/{local_file.name}"

    print(f"[*] Przygotowanie katalogu '{hdfs_dir}' w HDFS...")
    run_hdfs_command(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], use_docker_exec=use_docker)

    if use_docker:
        # Sprawdzamy czy plik jest zamontowany w kontenerze namenode w /workspace
        check_mount = subprocess.run(
            ["docker", "exec", "namenode", "test", "-f", f"/workspace/{local_file.name}"],
            capture_output=True,
        )
        if check_mount.returncode == 0:
            print(
                f"[*] Wykryto zamontowany wolumen /workspace w kontenerze 'namenode'. "
                f"Wysyłanie '{local_file.name}' do HDFS '{hdfs_dir}'..."
            )
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "-w",
                    "/workspace",
                    "namenode",
                    "hdfs",
                    "dfs",
                    "-put",
                    "-f",
                    local_file.name,
                    hdfs_dir,
                ],
                check=True,
            )
        else:
            print(
                f"[*] Strumieniowe przesyłanie '{local_file.name}' z hosta do kontenera "
                f"'namenode' i zapis do HDFS '{target_hdfs_path}'..."
            )
            with open(local_file, "rb") as f_in:
                subprocess.run(
                    [
                        "docker",
                        "exec",
                        "-i",
                        "namenode",
                        "hdfs",
                        "dfs",
                        "-put",
                        "-f",
                        "-",
                        target_hdfs_path,
                    ],
                    stdin=f_in,
                    check=True,
                )
    else:
        run_hdfs_command(
            ["hdfs", "dfs", "-put", "-f", str(local_file), hdfs_dir],
            use_docker_exec=False,
        )

    print(f"[+] Plik '{local_file.name}' został pomyślnie przesłany do '{hdfs_dir}'!")

    # Weryfikacja Definition of Done (DoD)
    print("\n[*] Weryfikacja Definition of Done (hdfs dfs -du -h /data/):")
    res = run_hdfs_command(["hdfs", "dfs", "-du", "-h", hdfs_dir], use_docker_exec=use_docker)
    print(res.stdout.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rozpakowanie i strumieniowe złączenie danych GSOD (.tar -> .op.gz) oraz wysyłka do HDFS."
    )
    parser.add_argument(
        "--zip-path",
        type=Path,
        default=DEFAULT_ZIP_PATH,
        help="Ścieżka do pliku gsod_all_years.zip (jeśli jeszcze nie rozpakowany).",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=DEFAULT_ARCHIVE_DIR,
        help="Ścieżka do katalogu zawierającego archiwa roczne gsod_YYYY.tar.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="Ścieżka zapisu wynikowego pliku zbiorczego (domyślnie: gsod_full.csv).",
    )
    parser.add_argument(
        "--target-gb",
        type=float,
        default=DEFAULT_TARGET_GB,
        help="Docelowy rozmiar pliku wynikowego w GiB (domyślnie: 5.5 GiB; podaj 0 aby przetworzyć wszystkie lata).",
    )
    parser.add_argument(
        "--chunk-gb",
        type=float,
        default=None,
        help="Opcjonalny maksymalny rozmiar pojedynczego pliku w GiB, jeśli chcemy podzielić dane na kilka dużych plików.",
    )
    parser.add_argument(
        "--chronological",
        action="store_true",
        help="Przetwarzaj lata chronologicznie (od 1901) zamiast od najnowszych (2019).",
    )
    parser.add_argument(
        "--skip-hdfs",
        action="store_true",
        help="Tylko wygeneruj plik na dysku lokalnym, pomiń wysyłanie do HDFS.",
    )
    parser.add_argument(
        "--only-hdfs",
        action="store_true",
        help="Pomiń generowanie pliku i wyślij już istniejący plik gsod_full.csv do HDFS.",
    )
    parser.add_argument(
        "--hdfs-dir",
        type=str,
        default=DEFAULT_HDFS_DIR,
        help="Docelowy katalog w HDFS (domyślnie: /data/).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        if not args.only_hdfs:
            archive_dir = ensure_zip_extracted(args.zip_path, args.archive_dir)
            created_files = stream_tar_to_merged_files(
                archive_dir=archive_dir,
                output_file=args.output,
                target_size_gb=args.target_gb,
                chunk_size_gb=args.chunk_gb,
                newest_first=not args.chronological,
            )
        else:
            created_files = [args.output]

        if not args.skip_hdfs:
            for fpath in created_files:
                upload_to_hdfs(fpath, hdfs_dir=args.hdfs_dir)

    except Exception as exc:
        print(f"[-] BŁĄD: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
