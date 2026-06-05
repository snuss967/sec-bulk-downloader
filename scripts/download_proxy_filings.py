#!/usr/bin/env python3

import csv
import json
import os
import re
import signal
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zipfile import ZipFile, ZIP_DEFLATED


SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"
SEC_FULL_INDEX_BASE = "https://www.sec.gov/Archives/edgar/full-index"

STOP_REQUESTED = False


def handle_stop(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    print(f"Received signal {signum}. Will checkpoint and stop after current item.", flush=True)


signal.signal(signal.SIGTERM, handle_stop)
signal.signal(signal.SIGINT, handle_stop)


@dataclass
class FilingRecord:
    form: str
    filing_date: str
    cik: str
    company: str
    sec_filename: str
    sec_url: str
    local_path: str
    downloaded_at_utc: str
    bytes: int


def env_str(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    value = env_str(name)
    if value == "":
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = env_str(name)
    return float(value) if value else default


def current_quarter() -> int:
    return (datetime.now(timezone.utc).month - 1) // 3 + 1


def current_year() -> int:
    return datetime.now(timezone.utc).year


def safe_name(value: str, max_len: int = 80) -> str:
    value = value.strip()
    value = re.sub(r"[^\w\-.& ]+", "", value)
    value = re.sub(r"\s+", "_", value)
    return value[:max_len].strip("_") or "unknown"


class SecClient:
    def __init__(self, user_agent: str, sleep_seconds: float = 0.20, max_retries: int = 5):
        self.user_agent = user_agent
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries
        self.last_request_ts = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_ts
        if elapsed < self.sleep_seconds:
            time.sleep(self.sleep_seconds - elapsed)

    def get_bytes(self, url: str) -> bytes:
        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            self._rate_limit()
            self.last_request_ts = time.time()

            request = Request(
                url,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept-Encoding": "identity",
                    "Host": "www.sec.gov",
                },
            )

            try:
                with urlopen(request, timeout=60) as response:
                    return response.read()

            except HTTPError as exc:
                last_exc = exc
                if exc.code == 404:
                    raise
                if exc.code in {403, 429, 500, 502, 503, 504}:
                    wait = min(120, 2 ** attempt)
                    print(f"HTTP {exc.code} for {url}; retrying in {wait}s", flush=True)
                    time.sleep(wait)
                    continue
                raise

            except URLError as exc:
                last_exc = exc
                wait = min(120, 2 ** attempt)
                print(f"URL error for {url}: {exc}; retrying in {wait}s", flush=True)
                time.sleep(wait)

        raise RuntimeError(f"Failed after {self.max_retries} retries: {url}") from last_exc

    def get_text(self, url: str) -> str:
        data = self.get_bytes(url)
        return data.decode("latin-1", errors="replace")


def atomic_write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def atomic_write_json(path: Path, obj):
    atomic_write_text(path, json.dumps(obj, indent=2, sort_keys=True))


def load_progress(progress_path: Path) -> Dict:
    if not progress_path.exists():
        return {
            "processed_sec_filenames": [],
            "completed_quarters": [],
            "downloaded_count": 0,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "updated_at_utc": None,
        }
    return json.loads(progress_path.read_text(encoding="utf-8"))


def load_manifest(manifest_path: Path) -> List[FilingRecord]:
    if not manifest_path.exists():
        return []

    records: List[FilingRecord] = []
    with manifest_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(
                FilingRecord(
                    form=row["form"],
                    filing_date=row["filing_date"],
                    cik=row["cik"],
                    company=row["company"],
                    sec_filename=row["sec_filename"],
                    sec_url=row["sec_url"],
                    local_path=row["local_path"],
                    downloaded_at_utc=row["downloaded_at_utc"],
                    bytes=int(row["bytes"]),
                )
            )
    return records


def write_manifest(manifest_path: Path, records: List[FilingRecord]):
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = manifest_path.with_suffix(".csv.tmp")

    fieldnames = [
        "form",
        "filing_date",
        "cik",
        "company",
        "sec_filename",
        "sec_url",
        "local_path",
        "downloaded_at_utc",
        "bytes",
    ]

    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))

    tmp.replace(manifest_path)


def append_error(error_path: Path, message: str):
    error_path.parent.mkdir(parents=True, exist_ok=True)
    with error_path.open("a", encoding="utf-8") as f:
        f.write(f"\n--- {datetime.now(timezone.utc).isoformat()} ---\n")
        f.write(message.rstrip() + "\n")


def rebuild_zip(output_dir: Path, zip_path: Path):
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = zip_path.with_suffix(".zip.tmp")

    with ZipFile(tmp, "w", compression=ZIP_DEFLATED, compresslevel=6) as z:
        for path in sorted(output_dir.rglob("*")):
            if path == zip_path or path == tmp or path.is_dir():
                continue
            z.write(path, path.relative_to(output_dir))

    tmp.replace(zip_path)


def checkpoint(
    *,
    output_dir: Path,
    zip_path: Path,
    progress_path: Path,
    manifest_path: Path,
    records: List[FilingRecord],
    progress: Dict,
):
    progress["downloaded_count"] = len(records)
    progress["updated_at_utc"] = datetime.now(timezone.utc).isoformat()

    write_manifest(manifest_path, records)
    atomic_write_json(progress_path, progress)
    rebuild_zip(output_dir, zip_path)

    print(
        f"Checkpoint complete: {len(records)} filings; ZIP at {zip_path}",
        flush=True,
    )


def quarter_iter(start_year: int, end_year: int) -> Iterable[Tuple[int, int]]:
    now_year = current_year()
    now_qtr = current_quarter()

    for year in range(start_year, end_year + 1):
        q_start = 1
        q_end = 4

        if year == now_year:
            q_end = now_qtr

        for qtr in range(q_start, q_end + 1):
            yield year, qtr


def master_index_url(year: int, qtr: int) -> str:
    return f"{SEC_FULL_INDEX_BASE}/{year}/QTR{qtr}/master.idx"


def parse_master_idx(text: str, wanted_forms: Set[str]) -> Iterable[Dict[str, str]]:
    """
    master.idx rows look like:
    CIK|Company Name|Form Type|Date Filed|Filename
    320193|APPLE INC|10-K|2024-11-01|edgar/data/320193/0000320193-24-000123.txt
    """
    lines = text.splitlines()
    in_data = False

    for line in lines:
        if not in_data:
            if line.startswith("CIK|Company Name|Form Type|Date Filed|Filename"):
                in_data = True
            continue

        parts = line.split("|")
        if len(parts) != 5:
            continue

        cik, company, form, filing_date, filename = [p.strip() for p in parts]
        form_upper = form.upper()

        if form_upper in wanted_forms:
            yield {
                "cik": cik,
                "company": company,
                "form": form_upper,
                "filing_date": filing_date,
                "filename": filename,
            }


def sec_file_url(sec_filename: str) -> str:
    return f"{SEC_ARCHIVES_BASE}/{quote(sec_filename)}"


def accession_from_filename(sec_filename: str) -> str:
    return Path(sec_filename).name.replace(".txt", "")


def local_filing_path(output_dir: Path, row: Dict[str, str]) -> Path:
    accession = accession_from_filename(row["filename"])
    company = safe_name(row["company"])
    return (
        output_dir
        / "filings"
        / row["form"]
        / f"{row['filing_date']}_{company}_{row['cik']}_{accession}.txt"
    )


def download_one_filing(
    client: SecClient,
    output_dir: Path,
    row: Dict[str, str],
) -> FilingRecord:
    url = sec_file_url(row["filename"])
    data = client.get_bytes(url)

    path = local_filing_path(output_dir, row)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)

    return FilingRecord(
        form=row["form"],
        filing_date=row["filing_date"],
        cik=row["cik"],
        company=row["company"],
        sec_filename=row["filename"],
        sec_url=url,
        local_path=str(path.relative_to(output_dir)),
        downloaded_at_utc=datetime.now(timezone.utc).isoformat(),
        bytes=len(data),
    )


def main() -> int:
    user_agent = env_str("SEC_USER_AGENT")
    if not user_agent:
        print(
            "WARNING: SEC_USER_AGENT is not set. Set a GitHub secret named "
            "SEC_USER_AGENT, e.g. 'sec-proxy-bulk-downloader you@example.com'.",
            flush=True,
        )
        user_agent = "sec-proxy-bulk-downloader contact@example.com"

    forms = {x.strip().upper() for x in env_str("FORMS", "DEFM14A,PREM14A").split(",") if x.strip()}
    start_year = env_int("START_YEAR", 1994) or 1994
    end_year = env_int("END_YEAR", current_year()) or current_year()
    max_filings = env_int("MAX_FILINGS", None)
    sleep_seconds = env_float("SLEEP_SECONDS", 0.20)
    batch_save_every = env_int("BATCH_SAVE_EVERY", 5) or 5

    output_dir = Path(env_str("OUTPUT_DIR", "output"))
    zip_path = Path(env_str("ZIP_PATH", str(output_dir / "sec_proxy_filings.zip")))

    progress_path = output_dir / "progress.json"
    manifest_path = output_dir / "manifest.csv"
    error_path = output_dir / "errors.log"

    output_dir.mkdir(parents=True, exist_ok=True)

    progress = load_progress(progress_path)
    records = load_manifest(manifest_path)

    processed: Set[str] = set(progress.get("processed_sec_filenames", []))
    completed_quarters: Set[str] = set(progress.get("completed_quarters", []))

    # Also dedupe off manifest in case progress.json is stale.
    for r in records:
        processed.add(r.sec_filename)

    client = SecClient(user_agent=user_agent, sleep_seconds=sleep_seconds)

    print(f"Forms: {sorted(forms)}", flush=True)
    print(f"Years: {start_year}-{end_year}", flush=True)
    print(f"Already downloaded: {len(records)}", flush=True)
    print(f"Max filings: {max_filings if max_filings is not None else 'no explicit limit'}", flush=True)

    added_since_checkpoint = 0

    for year, qtr in quarter_iter(start_year, end_year):
        if STOP_REQUESTED:
            break

        quarter_key = f"{year}Q{qtr}"
        if quarter_key in completed_quarters:
            continue

        url = master_index_url(year, qtr)
        print(f"Reading index {quarter_key}: {url}", flush=True)

        try:
            index_text = client.get_text(url)
        except HTTPError as exc:
            if exc.code == 404:
                print(f"No index found for {quarter_key}; skipping.", flush=True)
                continue
            append_error(error_path, f"Failed reading index {quarter_key}: {repr(exc)}")
            continue
        except Exception as exc:
            append_error(error_path, f"Failed reading index {quarter_key}: {repr(exc)}")
            continue

        quarter_hits = 0

        for row in parse_master_idx(index_text, forms):
            if STOP_REQUESTED:
                break

            if max_filings is not None and len(records) >= max_filings:
                print("Reached MAX_FILINGS.", flush=True)
                STOP_REQUESTED = True
                break

            sec_filename = row["filename"]
            if sec_filename in processed:
                continue

            try:
                record = download_one_filing(client, output_dir, row)
                records.append(record)
                processed.add(sec_filename)
                quarter_hits += 1
                added_since_checkpoint += 1

                progress["processed_sec_filenames"] = sorted(processed)

                print(
                    f"Downloaded #{len(records)}: {row['form']} | {row['filing_date']} | "
                    f"{row['company']} | {sec_filename}",
                    flush=True,
                )

                if added_since_checkpoint >= batch_save_every:
                    checkpoint(
                        output_dir=output_dir,
                        zip_path=zip_path,
                        progress_path=progress_path,
                        manifest_path=manifest_path,
                        records=records,
                        progress=progress,
                    )
                    added_since_checkpoint = 0

            except Exception as exc:
                append_error(
                    error_path,
                    "Filing download failed\n"
                    f"row={json.dumps(row, sort_keys=True)}\n"
                    f"error={repr(exc)}\n"
                    f"{traceback.format_exc()}",
                )
                # Failure mode: keep going.

        completed_quarters.add(quarter_key)
        progress["completed_quarters"] = sorted(completed_quarters)
        progress["processed_sec_filenames"] = sorted(processed)

        checkpoint(
            output_dir=output_dir,
            zip_path=zip_path,
            progress_path=progress_path,
            manifest_path=manifest_path,
            records=records,
            progress=progress,
        )

        print(f"Finished {quarter_key}: {quarter_hits} new matching filings", flush=True)

    checkpoint(
        output_dir=output_dir,
        zip_path=zip_path,
        progress_path=progress_path,
        manifest_path=manifest_path,
        records=records,
        progress=progress,
    )

    print(f"Done. Downloaded filings in manifest: {len(records)}", flush=True)
    print(f"ZIP: {zip_path}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        # Final failure mode: write error, rebuild whatever ZIP we can, exit 0
        # so the GitHub artifact upload step still runs.
        output_dir = Path(env_str("OUTPUT_DIR", "output"))
        zip_path = Path(env_str("ZIP_PATH", str(output_dir / "sec_proxy_filings.zip")))
        output_dir.mkdir(parents=True, exist_ok=True)
        append_error(output_dir / "errors.log", traceback.format_exc())
        try:
            rebuild_zip(output_dir, zip_path)
        except Exception:
            print("Failed to rebuild ZIP during fatal exception handling.", flush=True)
            print(traceback.format_exc(), flush=True)
        print("Fatal error captured; exiting 0 so artifact upload can run.", flush=True)
        raise SystemExit(0)
