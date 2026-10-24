#!/usr/bin/env python3
"""Builds Tick Lab's broker CFD price library from Dukascopy's public history.

Each month of each symbol becomes one small file, m1/<SYMBOL>/<YYYY-MM>.tlm1.gz,
holding every active minute's bid and ask open, high, low and close.

File format (little-endian, then gzipped):
  bytes 0-3   b'TLM1'
  byte  4     version (1)
  bytes 5-7   unused
  bytes 8-11  uint32 n, the number of minutes in the file
  bytes 12-15 int32 scale: prices are whole numbers of 1/scale
  n x uint16  minute of the month (0 = 00:00 UTC on the 1st)
  0-2 bytes   padding to a multiple of 4
  8 x n int32 bid open, bid high, bid low, bid close, ask open, ask high,
              ask low, ask close. Each column is stored as differences from
              the value before it (the first value is the price itself).

Usage:
  python build.py --symbol EURUSD --since 2021-10      # one symbol
  python build.py --manifest                           # rebuild manifest.json
"""
import argparse, concurrent.futures as cf, datetime as dt, gzip, json, lzma, os, random, struct, sys, time, urllib.error, urllib.request

SYMBOLS = {
    # Tick Lab name: (Dukascopy instrument, price scale, trades at weekends)
    "EURUSD": ("EURUSD", 100000, False), "GBPUSD": ("GBPUSD", 100000, False), "USDJPY": ("USDJPY", 1000, False),
    "AUDUSD": ("AUDUSD", 100000, False), "USDCAD": ("USDCAD", 100000, False), "USDCHF": ("USDCHF", 100000, False),
    "XAUUSD": ("XAUUSD", 1000, False), "XAGUSD": ("XAGUSD", 1000, False),
    "USOIL": ("LIGHTCMDUSD", 1000, False), "UKOIL": ("BRENTCMDUSD", 1000, False),
    "NAS100": ("USATECHIDXUSD", 1000, False), "US500": ("USA500IDXUSD", 1000, False), "US30": ("USA30IDXUSD", 1000, False),
    "GER40": ("DEUIDXEUR", 1000, False), "UK100": ("GBRIDXGBP", 1000, False), "JPN225": ("JPNIDXJPY", 1000, False),
    "BTCUSD": ("BTCUSD", 10, True), "ETHUSD": ("ETHUSD", 10, True),
}
# Dukascopy refuses GitHub's servers, so the Action fetches through Tick Lab's Cloudflare relay (TL_FEED).
FEED = os.environ.get("TL_FEED", "https://datafeed.dukascopy.com/datafeed/").rstrip("/") + "/{inst}/{y:04d}/{m:02d}/{d:02d}/{side}_candles_min_1.bi5"
BUDGET = float(os.environ.get("TL_BUDGET", "0"))   # seconds to work before stopping cleanly (0 = no limit)
STARTED = time.time()
GAP = float(os.environ.get("TL_GAP", "0"))   # seconds between requests, to stay inside the relay's free daily allowance
_next = [0.0]
import threading
_lock = threading.Lock()


def pace():
    if not GAP:
        return
    with _lock:
        now = time.time()
        at = max(now, _next[0])
        _next[0] = at + GAP
    if at > now:
        time.sleep(at - now)
HERE = os.path.dirname(os.path.abspath(__file__))


STATS = {"ok": 0, "missing": 0, "retries": 0, "failed": 0, "codes": {}}


def fetch(url, tries=8):
    """Downloads one file politely: backs off when Dukascopy says slow down.
    Returns b"" for a file that doesn't exist and None if it never came."""
    for k in range(tries):
        pace()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (TickLab library builder)"})
            with urllib.request.urlopen(req, timeout=60) as r:
                STATS["ok"] += 1
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                STATS["missing"] += 1
                return b""
            STATS["codes"][e.code] = STATS["codes"].get(e.code, 0) + 1
            wait = float(e.headers.get("Retry-After") or 0) or min(30, 1.5 * 2 ** k)
        except Exception as e:
            name = type(e).__name__
            STATS["codes"][name] = STATS["codes"].get(name, 0) + 1
            wait = min(30, 1.5 * 2 ** k)
        STATS["retries"] += 1
        time.sleep(wait + random.random())
    STATS["failed"] += 1
    return None


def candles(raw):
    """Dukascopy 1-minute candles: (second of day, open, close, low, high, volume)."""
    if not raw:
        return []
    data = lzma.decompress(raw, format=lzma.FORMAT_ALONE)
    return [struct.unpack_from(">5If", data, o) for o in range(0, len(data) - len(data) % 24, 24)]


def month_days(y, m):
    d = dt.date(y, m, 1)
    while d.month == m:
        yield d
        d += dt.timedelta(days=1)


def build_month(sym, y, m, pool):
    inst, scale, weekends = SYMBOLS[sym]
    days = [d for d in month_days(y, m) if weekends or d.weekday() != 5]   # no Saturdays for markets that close
    urls = []
    for d in days:
        for side in ("BID", "ASK"):
            urls.append(FEED.format(inst=inst, y=d.year, m=d.month - 1, d=d.day, side=side))
    files = list(pool.map(fetch, urls))
    first = dt.datetime(y, m, 1, tzinfo=dt.timezone.utc)
    rows = []
    missing = 0
    for i, d in enumerate(days):
        if files[2 * i] is None or files[2 * i + 1] is None:
            missing += 1                       # never came: leave the day out rather than stop
            continue
        bid, ask = candles(files[2 * i]), candles(files[2 * i + 1])
        base = int((dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc) - first).total_seconds() // 60)
        for b, a in zip(bid, ask):
            sec, bo, bc, bl, bh, vol = b
            if not vol > 0 and bo == bc and bl == bh:
                continue                       # market closed that minute
            _, ao, ac, al, ah, _ = a
            rows.append((base + sec // 60, bo, bh, bl, bc, ao, ah, al, ac))
    rows.sort()
    return scale, rows, missing


def encode(scale, rows):
    n = len(rows)
    out = bytearray(b"TLM1" + bytes([1, 0, 0, 0]) + struct.pack("<Ii", n, scale))
    out += struct.pack("<%dH" % n, *[r[0] for r in rows])
    out += b"\0" * ((-len(out)) % 4)
    for c in range(1, 9):
        prev, col = 0, []
        for r in rows:
            col.append(r[c] - prev)
            prev = r[c]
        out += struct.pack("<%di" % n, *col)
    return gzip.compress(bytes(out), 9, mtime=0)


def months_between(since, until):
    y, m = since
    while (y, m) <= until:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def last_full_month():
    today = dt.date.today()
    first = today.replace(day=1)
    if today.day < 3:                      # Dukascopy finishes the last days a little late
        first = (first - dt.timedelta(days=1)).replace(day=1)
    prev = first - dt.timedelta(days=1)
    return prev.year, prev.month


def build_symbol(sym, since, force):
    until = last_full_month()
    os.makedirs(os.path.join(HERE, "m1", sym), exist_ok=True)
    with cf.ThreadPoolExecutor(int(os.environ.get("TL_THREADS", "3"))) as pool:
        for y, m in reversed(list(months_between(since, until))):   # newest first: the library is useful sooner
            if BUDGET and time.time() - STARTED > BUDGET:
                print("::notice::%s: time's up for this run, the next run carries on" % sym, flush=True)
                break
            path = os.path.join(HERE, "m1", sym, "%04d-%02d.tlm1.gz" % (y, m))
            if os.path.exists(path) and not force:
                continue
            t0 = time.time()
            scale, rows, missing = build_month(sym, y, m, pool)
            note = "%s %04d-%02d: %d minutes in %.0fs, %d days missing, requests %s" % (sym, y, m, len(rows), time.time() - t0, missing, json.dumps(STATS))
            print(("::warning::" if missing else "::notice::") + note, flush=True)
            if not rows or missing > 3:
                continue                       # try this month again next run
            with open(path, "wb") as f:
                f.write(encode(scale, rows))


def manifest():
    out = {"version": 1, "updated": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "symbols": {}}
    root = os.path.join(HERE, "m1")
    for sym in sorted(SYMBOLS):
        d = os.path.join(root, sym)
        months = sorted(f[:7] for f in os.listdir(d) if f.endswith(".tlm1.gz")) if os.path.isdir(d) else []
        if months:
            out["symbols"][sym] = {"scale": SYMBOLS[sym][1], "months": months}
    with open(os.path.join(HERE, "manifest.json"), "w") as f:
        json.dump(out, f, separators=(",", ":"))
    print("manifest:", {k: len(v["months"]) for k, v in out["symbols"].items()})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol")
    ap.add_argument("--since", default="2021-10")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--manifest", action="store_true")
    a = ap.parse_args()
    if a.manifest:
        manifest()
    elif a.symbol:
        y, m = map(int, a.since.split("-"))
        build_symbol(a.symbol, (y, m), a.force)
    else:
        ap.error("give --symbol or --manifest")
