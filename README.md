# TikTok bulk video scraper

Downloads TikTok videos by ID using [pyktok](https://github.com/dfreelon/pyktok),
with a headless-Chromium (Playwright) fallback for videos whose direct download
URL is blocked by anti-bot measures. Built to run unattended on a Linux server
over a large ID list (~400k), with a resumable SQLite ledger.

## Files

| File | Purpose |
|---|---|
| `tiktok_scraper.py` | The scraper. Run this on the server. |
| `export_cookies.py` | Run **once on a machine with a logged-in TikTok session** to produce `tiktok_cookies.json`. |
| `video_ids_to_reprocess.txt` | Newline-delimited TikTok video IDs to download. |
| `pyproject.toml` / `uv.lock` | Dependency lockfile for reproducible `uv` setup. |

## How cookies work (read this first)

pyktok and Playwright both need a logged-in TikTok session to fetch most videos.
The original script read those cookies straight out of the local Chrome profile
(`browser_cookie3` / `pyk.specify_browser("chrome")`). **That cannot work on a
headless server** — there is no logged-in Chrome there.

The chosen approach decouples the two: export your session to a file once, copy
that file to the server, and the scraper loads it for both pyktok and Playwright.

1. On your Mac (logged into TikTok in Chrome, with Chrome **closed** so the
   cookie DB isn't locked):

   ```bash
   python export_cookies.py            # writes tiktok_cookies.json
   ```

2. Copy the file to the server, next to `tiktok_scraper.py`:

   ```bash
   scp tiktok_cookies.json user@tux-server:/path/to/tiktok-scrape/
   ```

`tiktok_cookies.json` **is your TikTok session** — treat it like a password. It
is git-ignored by default; do not commit or share it. Cookies also expire, so
for a multi-day run you may need to re-export and recopy partway through (the job
is resumable, so just stop, refresh the file, and restart).

> Alternative considered: a one-time interactive Playwright login *on* the server
> to create a persistent browser profile. It avoids shipping a cookie file, but
> needs a GUI/X display (or VNC) on the server and a manual login step per
> machine. The export-file approach is simpler and headless-friendly, so it's the
> default here.

## Server setup (TUX / Linux)

Requires Python ≥ 3.11. Using [`uv`](https://docs.astral.sh/uv/):

```bash
# 1. Get the code + create the venv from the lockfile
git clone <your-repo-url> tiktok-scrape && cd tiktok-scrape
uv sync

# 2. Install the Playwright browser AND its system libraries.
#    --with-deps installs the OS packages Chromium needs (needs sudo/root).
uv run playwright install --with-deps chromium
```

> The `.venv` in this repo (if present) was built on macOS and **will not run on
> Linux** — `uv sync` creates a fresh, correct one. `.venv` is git-ignored anyway.

## Running

```bash
uv run python tiktok_scraper.py \
    --video-ids video_ids_to_reprocess.txt \
    --cookies tiktok_cookies.json \
    --output-dir video_mp4s \
    --db video_downloads.db \
    --workers 16
```

For a long unattended run, detach it and keep logs:

```bash
nohup uv run python tiktok_scraper.py --workers 16 --no-progress > run.out 2>&1 &
```

### Resuming

The job is resumable. Every video's outcome is recorded in the SQLite ledger
(`video_downloads.db`). Re-running the **same command** skips anything already
marked `success` and only processes what's left — so if the process dies at
380k/423k, just start it again. Add `--retry-failures` to also re-attempt videos
that previously failed (e.g. after refreshing expired cookies).

### Key flags

| Flag | Default | Notes |
|---|---|---|
| `--workers N` | `16` | Concurrent download workers. See tuning below. |
| `--retry-failures` | off | Re-attempt rows previously marked `failure`. |
| `--limit N` | all | Only consider N IDs — for smoke tests. |
| `--random-subset` | off | With `--limit`, take a random sample (uses `--seed`). |
| `--flat` | off | One output dir instead of sharded subdirs. Not advised at 400k. |
| `--no-progress` | off | Disable the progress bar (cleaner under `nohup`). |
| `--max-retries N` | `3` | Per-video attempts before giving up. |

## What changed from the local version, and why

- **Cookies from a file, not Chrome.** Removed `browser_cookie3.chrome()` and
  `pyk.specify_browser("chrome")`; the scraper now loads `tiktok_cookies.json`
  and wires it into both pyktok (via its internal cookie jar) and Playwright.
- **Sharded output.** Videos are written into 100 subdirectories (by the last two
  ID digits) instead of one folder — 400k files in a single directory is rough on
  most filesystems and tooling. Use `--flat` to opt out.
- **Headless-Linux Chromium flags.** Added `--no-sandbox` (needed when running as
  root/in containers) and `--disable-dev-shm-usage` (avoids crashes from a small
  `/dev/shm`), plus `playwright install --with-deps` for the OS libraries.
- **Cross-mount safe moves.** `os.rename` → `shutil.move`, so an output dir on a
  different mount than the working dir doesn't raise a cross-device error.
- **Logging + `__main__` guard + clean Ctrl-C.** Runs log to `scrape.log`, the
  whole thing is wrapped in `main()`, and an interrupt cancels remaining work and
  saves progress so you can resume.

## Operational risks to plan for

- **Anti-bot / rate limiting.** ~400k requests from one server IP will likely get
  throttled or temporarily blocked, which shows up as a rising failure rate. This
  build deliberately keeps things simple (no proxies/throttle), per the brief —
  if it becomes a problem, options are: lower `--workers`, add an inter-request
  delay, rotate proxies, or split the ID list across IPs/days. The resumable
  ledger makes a stop-and-resume strategy cheap.
- **Cookie expiry.** Session cookies expire. For a multi-day run, re-export and
  recopy `tiktok_cookies.json`, then resume (optionally with `--retry-failures`).
- **Disk space.** 400k videos can run to hundreds of GB up to a few TB. Make sure
  `--output-dir` points at a volume with room, and check the cross-mount note
  above applies cleanly.
- **Single TikTok account.** Heavy automated downloading can get an account
  flagged. Consider using a throwaway account for the session you export.
