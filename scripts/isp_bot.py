
import logging
from pathlib import Path
from datetime import datetime
import hashlib
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://www.ispdados.rj.gov.br/estatistica.html"
TIMEOUT = 60

CSV_WHITELIST = [
    re.compile(r"BaseDPEvolucaoMensalCisp\.csv$", re.I),
    re.compile(r"\.csv$", re.I),
]

def _setup(base_dir: str | None = None):
    root = Path(base_dir).resolve() if base_dir else Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    logs_dir = root / "logs"
    for p in (data_dir, raw_dir, processed_dir, logs_dir):
        p.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=logs_dir / "isp_bot.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    return root, data_dir, raw_dir, processed_dir, logs_dir

def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _matches_whitelist(filename: str) -> bool:
    return any(p.search(filename) for p in CSV_WHITELIST)

def list_csv_links(session: requests.Session | None = None) -> list[tuple[str, str]]:
    s = session or requests.Session()
    resp = s.get(BASE_URL, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip()
        if not href.lower().endswith(".csv"):
            continue
        if href.startswith("http"):
            url = href
        else:
            url = requests.compat.urljoin(BASE_URL, href)
        filename = url.split("/")[-1]
        if _matches_whitelist(filename):
            links.append((text or filename, url))
    return links

def download_if_new(url: str, raw_dir: Path) -> Path | None:
    r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    content = r.content
    h = _hash_bytes(content)
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = url.split("/")[-1]
    target = raw_dir / f"{stamp}__{h[:10]}__{filename}"
    existing = list(raw_dir.glob(f"*__{h[:10]}__{filename}"))
    if existing:
        logging.info("Skip (unchanged): %s", filename)
        return None
    target.write_bytes(content)
    logging.info("Downloaded: %s -> %s", url, target.name)
    return target

def process_csv(csv_path: Path, processed_dir: Path) -> Path:
    encodings = ["utf-8", "latin1", "cp1252"]
    seps = [";", ","]
    df = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(csv_path, encoding=enc, sep=sep)
                if df.shape[1] > 1:
                    break
            except Exception:
                df = None
        if df is not None and df.shape[1] > 1:
            break
    if df is None:
        raise RuntimeError(f"Could not parse CSV: {csv_path.name}")

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    out = processed_dir / (csv_path.stem + "__normalized.csv")
    df.to_csv(out, index=False, encoding="utf-8", sep=";")
    logging.info("Processed: %s -> %s", csv_path.name, out.name)
    return out

def update_all(base_dir: str | None = None) -> dict:
    root, data_dir, raw_dir, processed_dir, logs_dir = _setup(base_dir)
    s = requests.Session()
    links = list_csv_links(s)

    downloaded = []
    for _, url in links:
        p = download_if_new(url, raw_dir)
        if p:
            downloaded.append(p)

    processed = []
    for p in (downloaded or []):
        try:
            out = process_csv(p, processed_dir)
            processed.append(out)
        except Exception as e:
            logging.error("Processing failed for %s: %s", p.name, e)

    master = None
    normalized_files = list(processed_dir.glob("*__normalized.csv"))
    if normalized_files:
        frames = []
        for f in normalized_files:
            try:
                frames.append(pd.read_csv(f, sep=";", encoding="utf-8"))
            except Exception:
                pass
        if frames:
            master = pd.concat(frames, ignore_index=True)
            master_path = data_dir / "master.parquet"
            try:
                import pyarrow as pa  # noqa: F401
                master.to_parquet(master_path, index=False)
            except Exception as e:
                # fallback: also save a consolidated CSV
                master.to_csv(data_dir / "master.csv", index=False, encoding="utf-8", sep=";")
            logging.info("Master updated with %s rows", len(master))

    return {
        "found_links": len(links),
        "downloaded": [p.name for p in downloaded],
        "processed": [p.name for p in processed],
        "master_rows": (0 if master is None else len(master)),
    }

if __name__ == "__main__":
    summary = update_all()
    print(summary)
