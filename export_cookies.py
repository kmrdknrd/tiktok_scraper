"""
Export your logged-in TikTok cookies to a JSON file.

Run this ONCE on a machine where you are logged into TikTok in Chrome (e.g. your
Mac). It writes tiktok_cookies.json, which tiktok_scraper.py loads on the server
so the server never needs a logged-in browser of its own.

    python export_cookies.py                 # -> tiktok_cookies.json
    python export_cookies.py --browser edge  # pull from a different browser
    python export_cookies.py -o creds/tt.json

Then copy the JSON file to the server (e.g. scp) next to tiktok_scraper.py.

SECURITY: this file IS your TikTok session. Treat it like a password — it is
git-ignored by default. Do not commit it or share it. Cookies also expire, so
you may need to re-export periodically for very long runs.
"""

import argparse
import json
import sys

import browser_cookie3


def main():
    p = argparse.ArgumentParser(description="Export TikTok cookies to JSON.")
    p.add_argument("--browser", default="chrome",
                   help="Browser to read cookies from: chrome, edge, firefox, brave, etc.")
    p.add_argument("-o", "--output", default="tiktok_cookies.json",
                   help="Output JSON path.")
    args = p.parse_args()

    try:
        loader = getattr(browser_cookie3, args.browser)
    except AttributeError:
        sys.exit(f"Unknown browser '{args.browser}'. Try: chrome, edge, firefox, brave.")

    cj = loader(domain_name="tiktok.com")

    cookies = []
    for c in cj:
        cookie = {
            "name": c.name,
            "value": c.value,
            "domain": c.domain,
            "path": c.path or "/",
            "secure": bool(c.secure),
            "httpOnly": bool(c.has_nonstandard_attr("HttpOnly"))
            if hasattr(c, "has_nonstandard_attr")
            else False,
        }
        if c.expires is not None:
            cookie["expires"] = int(c.expires)
        cookies.append(cookie)

    if not cookies:
        sys.exit(
            "No tiktok.com cookies found. Make sure you're logged into TikTok in "
            f"{args.browser} and that the browser is closed (some OSes lock the "
            "cookie DB while the browser is running)."
        )

    with open(args.output, "w") as f:
        json.dump(cookies, f, indent=2)

    print(f"Wrote {len(cookies)} cookies to {args.output}")
    print("Copy this file to the server next to tiktok_scraper.py. Keep it secret.")


if __name__ == "__main__":
    main()
