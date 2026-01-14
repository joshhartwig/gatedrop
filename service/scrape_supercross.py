#!/usr/bin/env python3
"""
SupercrossLive Results Scraper (events -> event -> race results)

What this script does:
1) GET https://results.supercrosslive.com/events/                 (discover events)
2) GET https://results.supercrosslive.com/results/?id=...&p=view_event  (discover sessions)
3) GET each https://results.supercrosslive.com/results/?id=...&p=view_race_result
   and extract finishing-order rows.

Important notes:
- The event page DOES contain links to p=view_race_result sessions.
- The race result pages are not always a <table>. We implement:
  A) Table parser if a <table> with POS header exists
  B) Fallback "text row" parser (best effort) that pulls lines starting with a position number

Usage:
  pip install requests beautifulsoup4

  python scrape_supercross.py --out event.json
  python scrape_supercross.py --event-id 487830 --out a1.json
  python scrape_supercross.py --event-id 487830 --only-main-events --out a1_mains.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


BASE = "https://results.supercrosslive.com"
EVENTS_URL = f"{BASE}/events/"
RESULTS_ROOT = f"{BASE}/results/"  # important for joining ?id=... hrefs safely

UA = "Mozilla/5.0 (compatible; supercross-fantasy-bot/1.0)"


@dataclass
class EventRef:
    event_id: str
    name: str
    url: str


@dataclass
class SessionRef:
    race_result_id: str
    session_name: str
    url: str


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def http_get(url: str, session: requests.Session, sleep_s: float) -> str:
    time.sleep(max(0.0, sleep_s))
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_query_param(url: str, key: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(url).query)
        vals = q.get(key, [])
        return vals[0] if vals else None
    except Exception:
        return None


# --------------------------
# Step 1: Discover events
# --------------------------
def discover_events(events_html: str) -> List[EventRef]:
    """
    Finds event links on the events page.
    We look for anchors that have query p=view_event and id=<event_id>.
    """
    soup = BeautifulSoup(events_html, "html.parser")
    found: List[EventRef] = []

    for a in soup.find_all("a", href=True):
        href_raw = (a["href"] or "").strip()
        if not href_raw:
            continue

        href = urljoin(EVENTS_URL, href_raw)

        q = parse_qs(urlparse(href).query)
        pval = (q.get("p", [""])[0] or "").strip().lower()
        if pval != "view_event":
            continue

        event_id = q.get("id", [None])[0]
        if not event_id:
            continue

        name = normalize_ws(a.get_text(" ", strip=True)) or f"event_{event_id}"
        found.append(EventRef(event_id=event_id, name=name, url=href))

    # de-dupe by event_id
    uniq: Dict[str, EventRef] = {}
    for e in found:
        uniq.setdefault(e.event_id, e)

    return list(uniq.values())


# --------------------------
# Step 2: Discover sessions
# --------------------------
def discover_sessions(event_html: str) -> List[SessionRef]:
    """
    From an event page, find all race result links.

    The event page includes links like:
      /results/?id=6440192&p=view_race_result
      results/?id=...&p=view_race_result
      ?id=...&p=view_race_result

    Key fix:
      If href begins with "?", we MUST join against RESULTS_ROOT, not the full event URL with query.
    """
    soup = BeautifulSoup(event_html, "html.parser")
    sessions: List[SessionRef] = []

    for a in soup.find_all("a", href=True):
        href_raw = (a["href"] or "").strip()
        if not href_raw:
            continue

        # Normalize URL safely for the three common forms
        if href_raw.startswith("?"):
            href = urljoin(RESULTS_ROOT, href_raw)
        else:
            href = urljoin(f"{BASE}/", href_raw)

        # Fast reject
        if "view_race_result" not in href:
            continue

        q = parse_qs(urlparse(href).query)
        pval = (q.get("p", [""])[0] or "").strip().lower()
        if pval != "view_race_result":
            continue

        rrid = q.get("id", [None])[0]
        if not rrid:
            continue

        session_name = normalize_ws(a.get_text(" ", strip=True)) or f"race_result_{rrid}"
        sessions.append(SessionRef(race_result_id=rrid, session_name=session_name, url=href))

    # de-dupe by race_result_id
    uniq: Dict[str, SessionRef] = {}
    for s in sessions:
        uniq.setdefault(s.race_result_id, s)

    return list(uniq.values())


# --------------------------
# Step 3: Parse results
# --------------------------
def parse_race_results_table_first(result_html: str) -> List[Dict[str, Any]]:
    """
    Prefer parsing <table> with a 'POS' header.
    If not found, fallback to text-row parsing.
    """
    soup = BeautifulSoup(result_html, "html.parser")
    tables = soup.find_all("table")

    for t in tables:
        header = []
        thead = t.find("thead")
        if thead:
            header = [normalize_ws(x.get_text(" ", strip=True)).upper() for x in thead.find_all(["th", "td"])]
        else:
            first_tr = t.find("tr")
            if first_tr:
                header = [normalize_ws(x.get_text(" ", strip=True)).upper() for x in first_tr.find_all(["th", "td"])]

        if not header or "POS" not in header:
            continue

        # Map header -> index
        col_map = {h: i for i, h in enumerate(header)}

        def get_cell(cells: List[str], *names: str) -> Optional[str]:
            for n in names:
                k = n.upper()
                if k in col_map and col_map[k] < len(cells):
                    v = cells[col_map[k]]
                    return v if v != "" else None
            return None

        results: List[Dict[str, Any]] = []
        tbody = t.find("tbody") or t

        for tr in tbody.find_all("tr"):
            cells = [normalize_ws(x.get_text(" ", strip=True)) for x in tr.find_all(["td", "th"])]
            if not cells:
                continue

            pos_raw = get_cell(cells, "POS")
            if not pos_raw or not re.fullmatch(r"\d+", pos_raw):
                continue

            results.append(
                {
                    "pos": int(pos_raw),
                    "number": get_cell(cells, "#", "NUM", "NO", "NUMBER"),
                    "rider": get_cell(cells, "RIDER", "NAME"),
                    "bike": get_cell(cells, "BIKE"),
                    "best_lap": get_cell(cells, "BEST LAP", "BEST"),
                    "time": get_cell(cells, "TIME", "TOTAL TIME"),
                    "gap": get_cell(cells, "GAP", "INTERVAL"),
                    "points": get_cell(cells, "POINTS", "PTS"),
                    "raw": cells,
                }
            )

        if results:
            return results

    # Fallback if no usable table
    return parse_race_results_text_fallback(result_html)


def parse_race_results_text_fallback(result_html: str) -> List[Dict[str, Any]]:
    """
    Best-effort fallback parser when results are not in a <table>.
    Strategy:
      - Extract visible text
      - Find a header line that contains 'POS' and 'RIDER' (or similar)
      - After that, read lines that start with a position integer
    """
    soup = BeautifulSoup(result_html, "html.parser")
    text = soup.get_text("\n", strip=True)
    if not text:
        return []

    lines = [normalize_ws(ln) for ln in text.splitlines() if normalize_ws(ln)]
    # find likely header
    header_idx = None
    for i, ln in enumerate(lines):
        up = ln.upper()
        if "POS" in up and ("RIDER" in up or "NAME" in up) and ("#" in up or "BIKE" in up):
            header_idx = i
            break

    if header_idx is None:
        return []

    results: List[Dict[str, Any]] = []

    for ln in lines[header_idx + 1 :]:
        # Stop if we hit obvious footer-ish content
        if ln.upper().startswith("GENERATED BY") or ln.upper().startswith("LIVE SUPER"):
            break

        # Typical rows begin with position, then number:
        # "1 21 Cooper Webb YAMAHA ..."
        m = re.match(r"^(\d+)\s+(\S+)\s+(.*)$", ln)
        if not m:
            continue

        pos = int(m.group(1))
        num = m.group(2)
        rest = m.group(3)

        # Try to split rider name from the rest (heuristic)
        # We'll take first 2-3 tokens as rider name if it looks like words.
        tokens = rest.split()
        rider = None
        if len(tokens) >= 2:
            # assume "First Last" at minimum
            rider = " ".join(tokens[:2])
            # if third token looks like part of last name (e.g., Deegan? still one token) we leave it
            # you can enhance this later with a rider registry.
        results.append(
            {
                "pos": pos,
                "number": num,
                "rider_guess": rider,
                "line": ln,
            }
        )

    return results


def is_main_event(session_name: str) -> bool:
    s = session_name.lower()
    return "main event" in s and "results" in s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-id", help="Specific event id to scrape (otherwise uses the first event found on events page)")
    ap.add_argument("--limit-sessions", type=int, default=0, help="Limit number of sessions fetched (0 = no limit)")
    ap.add_argument("--sleep", type=float, default=0.5, help="Sleep between requests (seconds)")
    ap.add_argument("--out", default="supercross_event.json", help="Output JSON file")
    ap.add_argument("--only-main-events", action="store_true", help="Only fetch sessions that look like main events")
    ap.add_argument("--debug", action="store_true", help="Print debug info about discovery")
    args = ap.parse_args()

    with requests.Session() as s:
        s.headers.update({"User-Agent": UA})

        # 1) events
        events_html = http_get(EVENTS_URL, s, sleep_s=args.sleep)
        events = discover_events(events_html)
        if not events:
            print("No events found. The events page structure may have changed.", file=sys.stderr)
            return 2

        chosen: Optional[EventRef] = None
        if args.event_id:
            chosen = next((e for e in events if e.event_id == args.event_id), None)
            if not chosen:
                print(f"Event id {args.event_id} not found on events page.", file=sys.stderr)
                if args.debug:
                    print("Found event_ids:", [e.event_id for e in events], file=sys.stderr)
                return 2
        else:
            chosen = events[0]

        # 2) event page -> sessions
        if args.debug:
            print(f"[debug] chosen event: id={chosen.event_id} name={chosen.name}")
            print(f"[debug] event url: {chosen.url}")

        event_html = http_get(chosen.url, s, sleep_s=args.sleep)

        if args.debug:
            print(f"[debug] event_html contains 'view_race_result'? {'view_race_result' in event_html}")

        sessions = discover_sessions(event_html)
        if not sessions:
            print("No sessions found on event page. Page structure may have changed.", file=sys.stderr)
            if args.debug:
                # dump a few hrefs to see what's there
                soup = BeautifulSoup(event_html, "html.parser")
                hrefs = []
                for a in soup.find_all("a", href=True):
                    hrefs.append(a["href"])
                    if len(hrefs) >= 30:
                        break
                print("[debug] first 30 hrefs:", hrefs, file=sys.stderr)
            return 2

        if args.only_main_events:
            sessions = [sess for sess in sessions if is_main_event(sess.session_name)]

        # stable ordering: by session_name then id
        sessions.sort(key=lambda x: (x.session_name.lower(), x.race_result_id))

        if args.limit_sessions and args.limit_sessions > 0:
            sessions = sessions[: args.limit_sessions]

        if args.debug:
            print(f"[debug] sessions found: {len(sessions)}")
            for sess in sessions[:10]:
                print(f"[debug]  - {sess.session_name} ({sess.race_result_id})")

        # 3) fetch & parse each race result page
        session_payloads: List[Dict[str, Any]] = []
        for sess in sessions:
            html = http_get(sess.url, s, sleep_s=args.sleep)
            rows = parse_race_results_table_first(html)
            session_payloads.append(
                {
                    "session_name": sess.session_name,
                    "race_result_id": sess.race_result_id,
                    "url": sess.url,
                    "results": rows,
                }
            )

        payload = {
            "event": {
                "event_id": chosen.event_id,
                "name": chosen.name,
                "url": chosen.url,
            },
            "sessions": session_payloads,
        }

        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        print(f"Wrote {args.out} with {len(session_payloads)} sessions for event_id={chosen.event_id}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())