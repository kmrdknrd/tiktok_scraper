"""
TikTok bulk video scraper — server-ready.

Downloads TikTok videos by ID using pyktok, with a Playwright (headless Chromium)
fallback for videos whose direct download URL is blocked by anti-bot measures.

Designed to run unattended on a Linux server over a large ID list (~400k):

  * Cookies are loaded from an exported JSON file (export_cookies.py), so NO
    locally logged-in browser is required on the server.
  * A DuckDB ledger makes the job RESUMABLE: re-running skips videos already
    marked 'success' and (optionally) retries previous failures.
  * Worker count, paths, and behaviour are configurable via CLI flags.
  * Output is sharded into subdirectories to avoid ~400k files in one folder.
  * Headless Chromium launches with server-safe flags (--no-sandbox, etc.).
  * Progress and errors are logged to a file as well as the console.

See README.md for full deployment instructions.

Usage (typical server run):
    python tiktok_scraper.py \
        --video-ids video_ids_to_reprocess.txt \
        --cookies tiktok_cookies.json \
        --output-dir video_mp4s \
        --db video_downloads.duckdb \
        --workers 16
"""

import argparse
import contextlib
import logging
import os
import platform
import shutil
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyktok as pyk
import requests
from playwright.sync_api import sync_playwright

# The `cookies` global that pyktok uses for its HTTP requests lives in the
# pyktok.pyktok submodule, NOT the package namespace, so we set it there.
from pyktok import pyktok as _pyk_core
from tqdm import tqdm

LOG = logging.getLogger("tiktok_scraper")

# TikTok ignores the @username portion of a video URL and resolves by ID, so a
# fixed placeholder username keeps pyktok's output filename deterministic
# (@tiktok_video_<id>.mp4), which we rely on below.
URL_TEMPLATE = "https://www.tiktok.com/@tiktok/video/{}"


# ---------------------------------------------------------------------------
# Runtime configuration (populated in main(), read by worker threads)
# ---------------------------------------------------------------------------
class Config:
    output_dir = Path("video_mp4s")
    shard = True
    cookies_pw = []  # Playwright-shaped cookie dicts
    run_id = "run"  # unique per process; namespaces per-worker CSVs
    max_retries = 3
    base_delay = 1.0


CFG = Config()


# ---------------------------------------------------------------------------
# Cookies
# ---------------------------------------------------------------------------
def load_cookies(cookies_path):
    """Load exported TikTok cookies and wire them into both pyktok and Playwright.

    The file is produced once on a machine with a logged-in TikTok session via
    export_cookies.py. Each entry is a dict with name/value/domain/path and
    optional secure/httpOnly/expires.
    """
    import json

    path = Path(cookies_path)
    if not path.exists():
        raise SystemExit(
            f"Cookie file not found: {path}\n"
            "Run export_cookies.py on a machine where you're logged into TikTok "
            "in Chrome, then copy the resulting JSON to the server."
        )

    with open(path, "r") as f:
        raw = json.load(f)

    if not raw:
        raise SystemExit(f"Cookie file {path} is empty — re-export it while logged in.")

    # (1) pyktok: it issues requests.get(..., cookies=<jar>). Build a jar and
    #     assign it to the module global that pyktok reads.
    jar = requests.cookies.RequestsCookieJar()
    for c in raw:
        jar.set(
            c["name"],
            c["value"],
            domain=c.get("domain", ".tiktok.com"),
            path=c.get("path", "/"),
        )
    _pyk_core.cookies = jar

    # (2) Playwright: it wants a list of cookie dicts on context.add_cookies().
    pw_cookies = []
    for c in raw:
        cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ".tiktok.com"),
            "path": c.get("path", "/"),
            "secure": bool(c.get("secure", False)),
            "httpOnly": bool(c.get("httpOnly", False)),
        }
        if c.get("expires") is not None:
            cookie["expires"] = int(c["expires"])
        pw_cookies.append(cookie)

    CFG.cookies_pw = pw_cookies
    LOG.info("Loaded %d TikTok cookies from %s", len(raw), path)


# ---------------------------------------------------------------------------
# User-agent for Playwright to mimic a real Chrome on the host OS
# ---------------------------------------------------------------------------
def _build_chrome_user_agent():
    system = platform.system()
    if system == "Darwin":
        os_token = "Macintosh; Intel Mac OS X 10_15_7"
    elif system == "Windows":
        os_token = "Windows NT 10.0; Win64; x64"
    elif system == "Linux":
        os_token = "X11; Linux x86_64"
    else:
        os_token = "Windows NT 10.0; Win64; x64"  # safe-ish default
    return (
        f"Mozilla/5.0 ({os_token}) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/148.0.7778.97 Safari/537.36"
    )


# ---------------------------------------------------------------------------
# Output paths (sharded so 400k files don't land in one directory)
# ---------------------------------------------------------------------------
_dirs_made = set()
_dirs_lock = threading.Lock()


def _video_path(video_id):
    """Return the destination .mp4 path for a video, creating its shard dir."""
    if CFG.shard:
        sub = CFG.output_dir / str(video_id)[-2:]  # 100 buckets by last 2 chars
    else:
        sub = CFG.output_dir
    if sub not in _dirs_made:
        with _dirs_lock:
            sub.mkdir(parents=True, exist_ok=True)
            _dirs_made.add(sub)
    return sub / f"{video_id}.mp4"


# ---------------------------------------------------------------------------
# Browser fallback (headless Chromium via Playwright)
# ---------------------------------------------------------------------------
def _do_browser_fallback(video_id, save_path, headless, timeout_ms):
    url = URL_TEMPLATE.format(video_id)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            channel="chromium",  # new headless = real-Chrome rendering
            args=[
                "--disable-blink-features=AutomationControlled",
                # Required when running as root / in containers on a Linux server:
                "--no-sandbox",
                # Avoids Chromium crashes from a small /dev/shm on servers:
                "--disable-dev-shm-usage",
            ],
        )
        try:
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1280, "height": 900},
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                user_agent=_build_chrome_user_agent(),
            )
            try:
                context.add_cookies(CFG.cookies_pw)
            except Exception:
                for c in CFG.cookies_pw:
                    try:
                        context.add_cookies([c])
                    except Exception:
                        pass

            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Sensitive content warning
            try:
                watch_anyway = page.get_by_text("Watch anyway", exact=True).first
                watch_anyway.wait_for(state="visible", timeout=500)
                watch_anyway.click(force=True)
                page.wait_for_timeout(500)
            except Exception:
                pass

            # Wait for the video OR a dead-end message. Trailing .first keeps the combined
            # locator single (a stray "Post unavailable"/"Page not available" string elsewhere
            # on the page would otherwise make the or_ match >1 and throw a strict-mode error).
            video = page.locator("video").first
            page_not_available = page.get_by_text(
                "Page not available", exact=True
            ).first
            post_unavailable = page.get_by_text("Post unavailable", exact=True).first
            video.or_(page_not_available).or_(post_unavailable).first.wait_for(
                state="visible", timeout=8000
            )

            # A real video wins: only treat the page as a dead-end if no video is present,
            # so a stray unavailable-string in a sidebar can't disqualify a good video.
            if not video.is_visible():
                if page_not_available.is_visible():
                    raise RuntimeError(
                        "page not available (video deleted or never existed)"
                    )
                if post_unavailable.is_visible():
                    detail = page.locator("body").inner_text()
                    reason = (
                        "age-restricted"
                        if "age-restricted" in detail
                        else "post unavailable"
                    )
                    raise RuntimeError(f"post unavailable ({reason})")
                raise RuntimeError("video element never appeared")

            # Open TikTok's custom menu and trigger the download with a real coordinate mouse.
            # Retry the whole interaction: the right-click sometimes lands the browser's native
            # menu, and the download click sometimes doesn't fire on the first try.
            download_item = page.get_by_text("Download video", exact=True).first
            download_started = False
            menu_ever_opened = False
            for _ in range(3):
                # bounding_box() auto-waits up to 30s for the element to be attached; give it
                # a short fuse and retry the attempt if the video/menu item has been swapped
                # out from under us, rather than hanging on a stale element.
                try:
                    box = video.bounding_box(timeout=3000)
                except Exception:
                    box = None
                if box is None:
                    page.wait_for_timeout(300)
                    continue
                cx, cy = box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
                page.mouse.move(cx, cy, steps=10)
                page.wait_for_timeout(150)
                page.mouse.click(cx, cy, button="right")
                try:
                    download_item.wait_for(state="visible", timeout=1500)
                except Exception:
                    page.keyboard.press("Escape")  # wrong menu — retry the right-click
                    page.wait_for_timeout(200)
                    continue
                menu_ever_opened = True

                page.wait_for_timeout(250)  # let the menu settle before clicking
                try:
                    ib = download_item.bounding_box(timeout=3000)
                except Exception:
                    ib = None
                if ib is None:
                    page.keyboard.press(
                        "Escape"
                    )  # menu slipped away — retry whole thing
                    page.wait_for_timeout(300)
                    continue
                ix, iy = ib["x"] + ib["width"] / 2, ib["y"] + ib["height"] / 2
                page.mouse.move(ix, iy, steps=5)
                page.wait_for_timeout(100)
                try:
                    with page.expect_download(timeout=10000) as dl_info:
                        page.mouse.click(ix, iy)
                    dl_info.value.save_as(str(save_path))
                    download_started = True
                    break
                except Exception:
                    page.keyboard.press(
                        "Escape"
                    )  # download didn't fire — retry whole thing
                    page.wait_for_timeout(300)

            if not download_started:
                if not menu_ever_opened:
                    raise RuntimeError(
                        "downloads disabled by creator (no 'Download video' in right-click menu)"
                    )
                raise RuntimeError(
                    "download did not start (expect_download timed out after retries)"
                )

        finally:
            browser.close()


def _save_video_browser_fallback(video_id, save_path, headless=True, timeout_ms=8000):
    """Run the Playwright fallback in its own thread to isolate the sync API
    from any asyncio loop running in the calling thread."""
    result = {"exc": None}

    def _run():
        try:
            _do_browser_fallback(video_id, save_path, headless, timeout_ms)
        except Exception as e:
            result["exc"] = e

    t = threading.Thread(target=_run)
    t.start()
    t.join()
    if result["exc"] is not None:
        raise result["exc"]


# ---------------------------------------------------------------------------
# Per-worker metadata CSVs. pyktok appends every video's metadata to one file,
# so concurrent workers would race on it. Each thread gets its own file, and we
# namespace by run_id so resuming a job never clobbers a previous run's rows.
# ---------------------------------------------------------------------------
_thread_local = threading.local()
_worker_index_lock = threading.Lock()
_worker_index_counter = [0]


def _worker_metadata_fn():
    fn = getattr(_thread_local, "metadata_fn", None)
    if fn is None:
        with _worker_index_lock:
            idx = _worker_index_counter[0]
            _worker_index_counter[0] += 1
        fn = f"videos_info_{CFG.run_id}_{idx}.csv"
        _thread_local.metadata_fn = fn
    return fn


# ---------------------------------------------------------------------------
# Core per-video download: pyktok first, Playwright fallback on downloadAddr errors
# ---------------------------------------------------------------------------
def _save_video(video_id):
    url = URL_TEMPLATE.format(video_id)
    dest = _video_path(video_id)

    # Resume safety: if the file is already on disk (e.g. the process died after
    # download but before the DB write), don't re-fetch it.
    if dest.exists() and dest.stat().st_size > 0:
        return "cached"

    metadata_fn = _worker_metadata_fn()
    for attempt in range(CFG.max_retries):
        try:
            with contextlib.redirect_stdout(None):
                _ = pyk.save_tiktok(url, True, metadata_fn)
            # pyktok writes to the CWD as @tiktok_video_<id>.mp4; move it into place.
            # shutil.move (not os.rename) handles output dirs on a different mount.
            produced = Path(f"@tiktok_video_{video_id}.mp4")
            shutil.move(str(produced), str(dest))
            return "pyktok"
        except Exception as e:
            # pyktok's direct download URL was rejected (anti-bot) — fall back to
            # driving a real browser, but still grab metadata via pyktok (save_video=False).
            if "downloadAddr" in str(e):
                with contextlib.redirect_stdout(None):
                    _ = pyk.save_tiktok(url, False, metadata_fn)
                _save_video_browser_fallback(video_id, dest)
                return "browser"
            # Video no longer exists.
            elif "itemInfo" in str(e):
                raise RuntimeError("video unavailable (deleted, privated, etc.)")

            # Out of retries — surface the error.
            if attempt == CFG.max_retries - 1:
                raise

            # Brief back-off on the metadata-CSV read/write race; exponential otherwise.
            if "No columns to parse" in str(e):
                time.sleep(0.1 + np.random.rand() * 0.1)
            else:
                time.sleep(CFG.base_delay * (2**attempt))


# ---------------------------------------------------------------------------
# Database (resumable ledger) — DuckDB.
# ---------------------------------------------------------------------------
def init_db(db_path, candidate_ids):
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS downloads (
            video_id TEXT PRIMARY KEY,
            status TEXT,
            method TEXT,
            error TEXT
        )
        """
    )
    # INSERT OR IGNORE (not REPLACE): registers any new IDs as 'pending' while
    # PRESERVING the status of videos already attempted in a previous run.
    ids_df = pd.DataFrame({"video_id": [str(v) for v in candidate_ids]})
    conn.register("candidate_ids_df", ids_df)
    conn.execute(
        "INSERT OR IGNORE INTO downloads (video_id, status) "
        "SELECT DISTINCT video_id, 'pending' FROM candidate_ids_df"
    )
    conn.unregister("candidate_ids_df")
    return conn


def select_work(conn, retry_failures):
    statuses = ["pending"]
    if retry_failures:
        statuses.append("failure")
    placeholders = ",".join("?" * len(statuses))
    rows = conn.execute(
        f"SELECT video_id FROM downloads WHERE status IN ({placeholders})", statuses
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Metadata consolidation (resume-safe: never deletes prior runs' CSVs)
# ---------------------------------------------------------------------------
def consolidate_metadata(conn):
    worker_csvs = sorted(Path(".").glob("videos_info_*.csv"))
    if not worker_csvs:
        return
    frames = []
    for fn in worker_csvs:
        try:
            frames.append(pd.read_csv(fn, keep_default_na=False))
        except Exception:
            pass  # skip a file mid-write or empty
    if not frames:
        return
    videos_info = pd.concat(frames, ignore_index=True)
    if "video_id" in videos_info.columns:
        videos_info = videos_info.drop_duplicates(subset="video_id", keep="last")
    videos_info.to_csv("videos_info.csv", index=False)
    # DuckDB can create a table straight from a registered DataFrame
    # (pandas' to_sql doesn't speak DuckDB connections).
    conn.register("videos_info_df", videos_info)
    conn.execute("CREATE OR REPLACE TABLE videos_info AS SELECT * FROM videos_info_df")
    conn.unregister("videos_info_df")
    LOG.info("Consolidated metadata for %d videos -> videos_info.csv", len(videos_info))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Bulk TikTok video downloader (pyktok + Playwright fallback)."
    )
    p.add_argument(
        "--video-ids",
        default="video_ids_to_reprocess.txt",
        help="Path to newline-delimited file of TikTok video IDs.",
    )
    p.add_argument(
        "--cookies",
        default="tiktok_cookies.json",
        help="Path to exported TikTok cookies JSON (see export_cookies.py).",
    )
    p.add_argument(
        "--output-dir", default="video_mp4s", help="Directory to write .mp4 files into."
    )
    p.add_argument(
        "--db",
        default="video_downloads.duckdb",
        help="DuckDB ledger path (enables resuming). NOTE: a .db file "
        "from the old SQLite version is NOT compatible; see README.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of concurrent download workers. See README on tuning; "
        "this is I/O- and browser-bound, so it is NOT one-per-core.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only consider this many IDs (for testing). Default: all.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=1,
        help="Random seed used when --limit takes a random subset.",
    )
    p.add_argument(
        "--random-subset",
        action="store_true",
        help="With --limit, pick a random subset (default takes the first N).",
    )
    p.add_argument(
        "--retry-failures",
        action="store_true",
        help="Also re-attempt videos previously marked 'failure'.",
    )
    p.add_argument(
        "--flat",
        action="store_true",
        help="Write all .mp4s into one directory (no sharding). "
        "Not recommended for ~400k files.",
    )
    p.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Per-video retry attempts before giving up.",
    )
    p.add_argument("--log-file", default="scrape.log", help="Path to the run log file.")
    p.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable the tqdm progress bar (cleaner for nohup/logs).",
    )
    return p.parse_args(argv)


def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stderr),
        ],
    )


def main(argv=None):
    args = parse_args(argv)
    setup_logging(args.log_file)

    CFG.output_dir = Path(args.output_dir)
    CFG.shard = not args.flat
    CFG.run_id = uuid.uuid4().hex[:8]
    CFG.max_retries = args.max_retries
    CFG.output_dir.mkdir(parents=True, exist_ok=True)

    load_cookies(args.cookies)

    # Load candidate IDs.
    with open(args.video_ids, "r") as f:
        video_ids = [line.strip() for line in f if line.strip()]
    LOG.info("Read %d video IDs from %s", len(video_ids), args.video_ids)

    if args.limit is not None and args.limit < len(video_ids):
        if args.random_subset:
            np.random.seed(args.seed)
            video_ids = list(
                np.random.choice(video_ids, size=args.limit, replace=False)
            )
        else:
            video_ids = video_ids[: args.limit]
        LOG.info(
            "Limited to %d IDs (random_subset=%s)", len(video_ids), args.random_subset
        )

    conn = init_db(args.db, video_ids)
    work = select_work(conn, args.retry_failures)

    done = len(video_ids) - len(work)
    LOG.info(
        "%d already complete; %d to process with %d workers (run_id=%s)",
        done,
        len(work),
        args.workers,
        CFG.run_id,
    )
    if not work:
        LOG.info("Nothing to do. All videos already processed.")
        consolidate_metadata(conn)
        conn.close()
        return

    interrupted = False
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_id = {executor.submit(_save_video, vid): str(vid) for vid in work}
        progress = as_completed(future_to_id)
        if not args.no_progress:
            progress = tqdm(progress, total=len(future_to_id))
        try:
            for future in progress:
                video_id = future_to_id[future]
                try:
                    method = future.result()
                    conn.execute(
                        "UPDATE downloads SET status='success', method=?, error=NULL "
                        "WHERE video_id=?",
                        (method, video_id),
                    )
                except Exception as e:
                    LOG.warning("FAILED %s: %s", video_id, e)
                    conn.execute(
                        "UPDATE downloads SET status='failure', error=? WHERE video_id=?",
                        (str(e), video_id),
                    )
                # No conn.commit(): DuckDB auto-commits each statement.
        except KeyboardInterrupt:
            interrupted = True
            LOG.warning(
                "Interrupted — cancelling remaining work and saving progress..."
            )
            executor.shutdown(wait=False, cancel_futures=True)

    consolidate_metadata(conn)

    # Summaries — DuckDB results convert straight to DataFrames via .df(),
    # so pd.read_sql_query is no longer needed.
    summary = conn.execute(
        "SELECT method, status, COUNT(*) AS count FROM downloads GROUP BY method, status"
    ).df()
    LOG.info("Status summary:\n%s", summary.to_string(index=False))
    error_summary = conn.execute(
        "SELECT error, COUNT(*) AS n FROM downloads WHERE status='failure' "
        "GROUP BY error ORDER BY n DESC"
    ).df()
    if not error_summary.empty:
        LOG.info("Failure breakdown:\n%s", error_summary.to_string(index=False))
    conn.close()

    if interrupted:
        LOG.info("Partial run saved. Re-run the same command to resume.")
        sys.exit(130)


if __name__ == "__main__":
    main()
