# scripts/build_derivs_json.py
# Builds data/derivs.json with funding_z and oi_delta_z (Bybit, free endpoints), for BTC/ETH.
# Start small; you can add more symbols later.

import json
import os
import math
from datetime import datetime, timezone
from collections import defaultdict
import urllib.request
import urllib.parse
import urllib.error
import time

LOOKBACK_DAYS = 90  # number of historical days for z-score context
COINS = [
    {"id": "bitcoin",  "symbol": "BTCUSDT"},
    {"id": "ethereum", "symbol": "ETHUSDT"},
]

BYBIT_BASE = "https://api.bybit.com"

def http_get(url, retry=3, sleep_ms=400):
    last_err = None
    for i in range(retry):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8")
        except Exception as e:
            last_err = e
            time.sleep((sleep_ms/1000.0) * (2**i))
    raise last_err

def to_date_ymd(ms):
    ts = int(ms) / 1000.0
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")

def z_score(series, latest):
    xs = [float(x) for x in series if x is not None]
    if len(xs) < 10:
        return None
    mean = sum(xs) / len(xs)
    var = sum((x - mean)**2 for x in xs) / len(xs)
    sd = math.sqrt(var)
    if sd == 0:
        return 0.0
    return (latest - mean) / sd

def get_bybit_funding_daily(symbol, days=LOOKBACK_DAYS):
    # Funding history is 8-hourly; we average per day. Need pagination to cover ~3*days points.
    per_page = 200  # Bybit max
    need_points = 3 * days + 6  # buffer
    cursor = None
    got = []
    attempts = 0
    while len(got) < need_points and attempts < 8:
        q = {
            "category": "linear",
            "symbol": symbol,
            "limit": str(per_page)
        }
        if cursor:
            q["cursor"] = cursor
        url = BYBIT_BASE + "/v5/market/funding/history?" + urllib.parse.urlencode(q)
        raw = http_get(url)
        data = json.loads(raw)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit funding error: {data}")
        items = data["result"].get("list", [])
        got.extend(items)
        cursor = data["result"].get("nextPageCursor")
        if not cursor or not items:
            break
        attempts += 1

    per_day = defaultdict(list)
    for it in got:
        try:
            rate = float(it["fundingRate"])
            d = to_date_ymd(it["fundingRateTimestamp"])
            per_day[d].append(rate)
        except Exception:
            continue

    days_sorted = sorted(per_day.keys())
    out = [{"date": d, "val": sum(per_day[d]) / len(per_day[d])} for d in days_sorted]
    return out[-(days + 1):]  # include latest + history

def get_bybit_oi_daily(symbol, days=LOOKBACK_DAYS):
    # 1d OI; usually 200 points are enough; paginate if needed.
    per_page = 200
    cursor = None
    got = []
    attempts = 0
    while len(got) < (days + 10) and attempts < 5:
        q = {
            "category": "linear",
            "symbol": symbol,
            "interval": "1d",
            "limit": str(per_page)
        }
        if cursor:
            q["cursor"] = cursor
        url = BYBIT_BASE + "/v5/market/open-interest?" + urllib.parse.urlencode(q)
        raw = http_get(url)
        data = json.loads(raw)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit OI error: {data}")
        items = data["result"].get("list", [])
        got.extend(items)
        cursor = data["result"].get("nextPageCursor")
        if not cursor or not items:
            break
        attempts += 1

    arr = []
    for it in got:
        try:
            d = to_date_ymd(it["timestamp"])
            oi = float(it["openInterest"])
            arr.append({"date": d, "oi": oi})
        except Exception:
            continue

    arr.sort(key=lambda x: x["date"])
    return arr[-(days + 4):]  # a few extra to compute 3D change

def compute_oi_3d_change(oi_series):
    # Returns [{"date", "oi3"}], where oi3 is 3-day percent change of OI
    out = []
    for i in range(len(oi_series)):
        d = oi_series[i]["date"]
        if i >= 3:
            prev = oi_series[i - 3]["oi"]
            now = oi_series[i]["oi"]
            oi3 = (now / prev - 1.0) if prev else None
        else:
            oi3 = None
        out.append({"date": d, "oi3": oi3})
    return out

def build_for_symbol(symbol):
    f = get_bybit_funding_daily(symbol, LOOKBACK_DAYS)
    o = get_bybit_oi_daily(symbol, LOOKBACK_DAYS)
    o3 = compute_oi_3d_change(o)

    fd = {x["date"]: x["val"] for x in f}
    o3d = {x["date"]: x["oi3"] for x in o3}

    dates = sorted(set(fd.keys()).intersection(o3d.keys()))
    if len(dates) < 15:
        return None

    funding_series = [fd[d] for d in dates[:-1]]
    funding_latest = fd[dates[-1]]
    funding_z = z_score(funding_series, funding_latest)

    oi_series = [o3d[d] for d in dates[:-1] if o3d[d] is not None]
    oi_latest = o3d[dates[-1]]
    oi_delta_z = z_score(oi_series, oi_latest) if oi_latest is not None else None

    return {
        "funding_z": funding_z,
        "oi_delta_z": oi_delta_z,
        "funding_days": len(funding_series) + 1,
        "oi_days": len([x for x in o3d.values() if x is not None])
    }

def main():
    items = []
    for c in COINS:
        try:
            res = build_for_symbol(c["symbol"])
            if res is None:
                print(f"SKIP {c['symbol']}: not enough data")
                continue
            items.append({"id": c["id"], **res})
            print(f"DONE {c['symbol']}: fz={res['funding_z']}, oiz={res['oi_delta_z']}")
        except Exception as e:
            print(f"ERROR {c['symbol']}: {e}")

    out = {
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lookback_days": LOOKBACK_DAYS,
        "source": "bybit",
        "items": items
    }

    os.makedirs("data", exist_ok=True)
    with open("data/derivs.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print("WROTE data/derivs.json")

if __name__ == "__main__":
    main()
