#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import signal
import sys
import time
from datetime import datetime
from unicodedata import normalize
from urllib.parse import quote_plus
import requests

# ----------------------
# CONFIG DEFAULTS
# ----------------------
API_URL = "localhost:8080"
API_USER = "admin"
API_PASS = "adminadmin"
BATCH_SIZE = 400
DELAY = 2
BATCH_DELAY = 5
DEFAULT_TRACKERS = [
    "http://tracker.openbittorrent.com:80/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://tracker.torrent.eu.org:451",
    "udp://open.demonii.com:1337",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://tracker.cyberia.is:6969/announce",
]

LANG_TAG = {
    "Alemán": "ger",
    "Catalán": "cat",
    "Español": "spa",
    "Esperanto": "epo",
    "Euskera": "baq",
    "Francés": "fra",
    "Gallego": "glg",
    "Inglés": "eng",
    "Italiano": "ita",
    "Portugués": "por",
    "Sueco": "swe"
}


# Global flag for graceful exit
stop_requested = False


def handle_sigint(signum, frame):
    global stop_requested
    stop_requested = True
    print("\nInterrupted by user. Exiting gracefully...", flush=True)


signal.signal(signal.SIGINT, handle_sigint)


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(console)


def sanitize_and_encode(row, enlace):
    """Build a proper magnet link with safe dn field"""
    title = normalize(
        "NFKD", str(row.get("Título") or row.get("Titulo") or row.get("titulo", ""))
    ).encode("ASCII", "ignore").decode()

    epl_id = (
        row.get("EPL Id")
        or row.get("Id")
        or row.get("ID")
        or row.get("epl_id")
        or "NA"
    )
    epl_revision = (row.get("Revisión"))

    #dn = f"EPL_[{epl_id}]_{title}".strip()
    dn = f"EPL_[{epl_id}]_{title}_(r{epl_revision})".strip()
    #dn_encoded = quote_plus(dn)

    trackers = "&".join(f"tr={quote_plus(t)}" for t in DEFAULT_TRACKERS)
    #return f"magnet:?xt=urn:btih:{enlace}&dn={dn_encoded}&{trackers}"
    return f"magnet:?xt=urn:btih:{enlace}&dn={dn}&{trackers}"

def read_csv(input_csv, languages=None):
    rows = []
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if languages:
                idioma = row.get("Idioma", "").lower()
                if not any(lang.lower() in idioma for lang in languages):
                    continue
            rows.append(row)
    return rows


def write_text_output(magnets, output_file, log_file):
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        for magnet in magnets:
            f.write(magnet + "\n")
    logging.info(f"Wrote {len(magnets)} magnets to {output_file}")
    if log_file:
        logging.info(f"Log written to {log_file}")


def push_to_qbittorrent(api_url, user, password, magnets, batch_size, delay, batch_delay):
    session = requests.Session()
    login_url = f"{api_url}/api/v2/auth/login"
    resp = session.post(login_url, data={"username": user, "password": password})
    if resp.text != "Ok.":
        logging.error("Failed to authenticate with qBittorrent API")
        sys.exit(1)

    total = len(magnets)
    sent = 0
    add_url = f"{api_url}/api/v2/torrents/add"

    logging.info(f"Connected. Pushing {total} magnets in batches of {batch_size}...")

    for i in range(0, total, batch_size):
        if stop_requested:
            break
        batch = magnets[i:i + batch_size]
        session.post(add_url, data={"urls": "\n".join(batch)})
        sent += len(batch)
        logging.info(f"Sent {sent}/{total}")
        time.sleep(delay)
        if (i // batch_size + 1) % 10 == 0:
            time.sleep(batch_delay)

    logging.info(f"Finished sending. Sent {sent}/{total}")


#def push_to_qbittorrent(api_url, user, password, magnets, batch_size, delay, batch_delay):
#    session = requests.Session()
#    login_url = f"{api_url}/api/v2/auth/login"
#    resp = session.post(login_url, data={"username": user, "password": password})
#    if resp.text != "Ok.":
#        logging.error("Failed to authenticate with qBittorrent API")
#        sys.exit(1)
#
#    total = len(magnets)
#    sent = 0
#    add_url = f"{api_url}/api/v2/torrents/add"
#
#    logging.info(f"Connected. Pushing {total} magnets in batches of {batch_size}...")
#
#    for i in range(0, total, batch_size):
#        if stop_requested:
#            break
#        batch = magnets[i:i + batch_size]
#
#        for m in batch:
#            data = {"urls": m["url"]}
#            data["category"] = "epl"
#            # Map idioma column to tag
#            if "Idioma" in m and m["Idioma"] in LANG_TAG:
#                data["tags"] = LANG_TAG[m["Idioma"]]
#
#            try:
#                session.post(add_url, data=data)
#            except Exception as e:
#                logging.error(f"Failed to add torrent {m.get('url')}: {e}")
#
#        sent += len(batch)
#        logging.info(f"Sent {sent}/{total}")
#        time.sleep(delay)
#        if (i // batch_size + 1) % 10 == 0:
#            time.sleep(batch_delay)
#
#    logging.info(f"Finished sending. Sent {sent}/{total}")



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", help="Input CSV file")
    parser.add_argument("output_file", nargs="?", help="Output file (text mode only)")
    parser.add_argument("--mode", choices=["text", "api"], default="text")
    parser.add_argument("--languages", help="Comma-separated list of languages")
    parser.add_argument("--log", nargs="?", const=True, help="Enable logging")
    parser.add_argument("--api-url", default=API_URL)
    parser.add_argument("--api-user", default=API_USER)
    parser.add_argument("--api-pass", default=API_PASS)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--delay", type=int, default=DELAY)
    parser.add_argument("--batch-delay", type=int, default=BATCH_DELAY)
    args = parser.parse_args()

    # Setup log file
    if args.log:
        if args.log is True:  # no file passed, auto-generate name
            log_file = f"log-{datetime.now().strftime('%y-%m-%d-%H%M')}.txt"
        else:
            log_file = args.log
        setup_logging(log_file)
        logging.info(" ".join(sys.argv))
    else:
        log_file = None
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Languages
    languages = args.languages.split(",") if args.languages else None

    # Read rows
    rows = read_csv(args.input_csv, languages)
    enlaces_count = 0
    magnets = []
    for row in rows:
        enlaces = str(row.get("Enlace(s)", "")).split(",")
        enlaces = [e.strip() for e in enlaces if e.strip()]
        enlaces_count += len(enlaces)
        for enlace in enlaces:
            magnets.append(sanitize_and_encode(row, enlace))

    logging.info(
        f"CSV rows: {len(rows)}, Magnets: {len(magnets)} (from {enlaces_count} enlaces)"
    )

    if args.mode == "text":
        if not args.output_file:
            logging.error("Output file required in text mode")
            sys.exit(1)
        write_text_output(magnets, args.output_file, log_file)
    elif args.mode == "api":
        push_to_qbittorrent(
            api_url=args.api_url,
            user=args.api_user,
            password=args.api_pass,
            magnets=magnets,
            batch_size=args.batch_size,
            delay=args.delay,
            batch_delay=args.batch_delay,
        )


if __name__ == "__main__":
    main()
