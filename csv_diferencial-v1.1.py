#!/usr/bin/env python3
import csv
import sys
import time
from datetime import datetime
from unicodedata import normalize
import signal
import os

# ----------------------
# CONFIG DEFAULTS
# ----------------------
DEFAULT_LANGUAGES = ['Español', 'Inglés']  # Example: ['Español', 'Inglés']
# ----------------------

USAGE = """
CSV Diferencial Script
=====================

This script compares a large CSV file (csv_full_imgs) against a smaller CSV file (Mis libros)
and outputs a filtered CSV containing only the rows that are either not in Mis libros
or have a different 'Revisión' (version). Supports optional language filtering.

Usage:
    python3 csv_diferencial.py <csv_full_imgs> <mis_libros_csv> [-l|--log] [--languages Idioma1,Idioma2,...] [-q]

Parameters:
    <csv_full_imgs>       Path to the main CSV file (big file to filter)
    <mis_libros_csv>      Path to the secondary CSV file (to compare against)
    -l, --log             Enable logging to a timestamped log file
    --languages           Optional comma-separated list of languages to filter (overrides config)
    -q                    Quiet mode. Suppresses progress messages and only prints final summary.
"""

LANGUAGE_MAP = {
    'alemán': 'Alemán', 'aleman': 'Alemán', 'Aleman': 'Alemán',
    'catalán': 'Catalán', 'catalan': 'Catalán', 'Catalan': 'Catalán',
    'español': 'Español', 'espanol': 'Español', 'Espanol': 'Español',
    'esperanto': 'Esperanto',
    'euskera': 'Euskera',
    'francés': 'Francés', 'frances': 'Francés', 'Frances': 'Francés',
    'gallego': 'Gallego',
    'inglés': 'Inglés', 'ingles': 'Inglés', 'Ingles': 'Inglés',
    'italiano': 'Italiano',
    'portugués': 'Portugués', 'portugues': 'Portugués', 'Portugues': 'Portugués'
}

# ----------------------
# Graceful exit handling
# ----------------------
def signal_handler(sig, frame):
    print("\nInterrupted by user. Exiting gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ----------------------
# Parse arguments
# ----------------------
if len(sys.argv) < 3 or '-h' in sys.argv or '--help' in sys.argv:
    print(USAGE)
    sys.exit(0)

big_file = sys.argv[1]
small_file = sys.argv[2]

quiet_mode = "-q" in sys.argv
log_enabled = "-l" in sys.argv or "--log" in sys.argv

# Optional languages
lang_arg = None
for arg in sys.argv[3:]:
    if arg.startswith("--languages"):
        lang_arg = arg.split("=", 1)[1] if "=" in arg else None

if lang_arg:
    language_filter = [LANGUAGE_MAP.get(lang.strip(), lang.strip()) for lang in lang_arg.split(',')]
else:
    language_filter = DEFAULT_LANGUAGES if DEFAULT_LANGUAGES else None

# Output file with timestamp
timestamp = datetime.now().strftime("%y-%m-%d-%H%M")
output_file = f"difference-{timestamp}.csv"
log_file = f"difference-{timestamp}.log" if log_enabled else None

start_time = time.time()

# ----------------------
# Logging function
# ----------------------
def log(msg):
    if quiet_mode:
        return
    print(msg, flush=True)
    if log_enabled:
        with open(log_file, 'a', encoding='utf-8') as lf:
            lf.write(msg + "\n")

log(f"Command: {' '.join(sys.argv)}")
if language_filter:
    log(f"Filtering languages: {language_filter}")

# ----------------------
# Step 1: Read small CSV and build set of (EPL Id, Revisión)
# ----------------------
mis_libros_set = set()
with open(small_file, encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    reader.fieldnames = [name.strip() for name in reader.fieldnames]
    for row in reader:
        try:
            epl_id = int(float(row['#epg_id'].strip()))
            revision = row['#version'].strip()
            mis_libros_set.add((epl_id, revision))
        except (ValueError, KeyError):
            continue

# ----------------------
# Step 2: Process big CSV and write filtered rows
# ----------------------
processed_count = 0
kept_count = 0
skipped_count = 0

# Get total rows for progress percentage
with open(big_file, encoding='utf-8-sig') as f:
    total_rows = sum(1 for _ in f) - 1  # exclude header

with open(big_file, encoding='utf-8-sig') as fin, open(output_file, 'w', encoding='utf-8', newline='') as fout:
    reader = csv.DictReader(fin)
    reader.fieldnames = [name.strip() for name in reader.fieldnames]
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(fout, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()

    for row in reader:
        processed_count += 1
        progress = (processed_count / total_rows) * 100

        if not quiet_mode and processed_count % 1000 == 0:
            log(f"Processed {processed_count}/{total_rows} rows ({progress:.2f}%)... Kept {kept_count} rows so far.")

        try:
            # Language filter
            if language_filter and row.get('Idioma', '').strip() not in language_filter:
                skipped_count += 1
                continue

            epl_id_big = 10000000 + int(row['EPL Id'].strip())
            revision = row['Revisión'].strip()
            if (epl_id_big, revision) not in mis_libros_set:
                filtered_row = {k: v for k, v in row.items() if k in fieldnames}
                writer.writerow(filtered_row)
                kept_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            # Keep row even if malformed
            log(f"Warning: Row {processed_count} caused exception but will be kept: {e}")
            filtered_row = {k: v for k, v in row.items() if k in fieldnames}
            writer.writerow(filtered_row)
            kept_count += 1

elapsed_time = time.time() - start_time
log(f"\nDone! Output file: {output_file}")
log(f"Total rows processed: {processed_count}, kept: {kept_count}, skipped: {skipped_count}")
log(f"Elapsed time: {elapsed_time:.2f} seconds")
