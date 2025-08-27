# scripts/build_metrics.py
# Automated builder for Funding_z, OI_Delta_z, and Fear & Greed.
# - Prefers OKX (works reliably from GitHub Actions).
# - If OKX OI history is blocked, falls back to daily "current OI" snapshots
#   persisted in data/oi_series_{id}.json, then computes 3D % change + z-score.
# - Writes data/derivs.json (and updates oi_series files).
#
# Requirements: Python 3.10+, requests (`pip install requests`)

import os
import json
import math
import time
from datetime import datetime, timezone

import requests

LOOKBACK_DAYS = 90
COINS = [
    {"id": "bitcoin",  "bybit": "BTCUSDT", "okx": "BTC-USDT-SWAP"},
    {"id": "ethereum", "bybit": "ETHUSDT", "okx": "ETH-USDT-SWAP"},
]

OKX = "https://www.okx.com"
BYBIT = "https://api.bybit.com"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})

def http_get_json(url, params=None, retry=3, backoff=0.6):
    last = None
    for i in range(retry):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    last = e
            else:
                last = f"HTTP {r.status_code} body={r.text[:200]}"
        except Exception as e:
            last = e
        time.sleep(backoff * (2 ** i))
    raise RuntimeError(f"GET {url} failed: {last}")

def z_score(series, latest):
    xs = [float(x) for x in series if x is not None]
    if len(xs) < 10:
        return None
    mean = sum(xs) / len(xs)
    var = sum((x - mean) ** 2 for x in xs) / len(xs)
    sd = math.sqrt(var)
    if sd == 0:
        return 0.0
    return (latest - mean) / sd

def to_date_ymd_ms(ms):
    ts = int(ms) / 1000.0
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")

# ---------- Funding (Bybit first, OKX fallback) ----------
def funding_daily_bybit(symbol):
    url = f"{BYBIT}/v5/market/funding/history"
    data = http_get_json(url, {"category":"linear", "symbol":symbol, "limit":"200"})
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit funding error: {data}")
    per_day = {}
    for it in data.get("result", {}).get("list", []):
        d = to_date_ymd_ms(it["fundingRateTimestamp"])
        r = float(it["fundingRate"])
        per_day.setdefault(d, []).append(r)
    days = sorted(per_day)
    out = [{"date": d, "val": sum(per_day[d])/len(per_day[d])} for d in days]
    return out[-(LOOKBACK_DAYS+1):]

def funding_daily_okx(inst_id):
    url = f"{OKX}/api/v5/public/funding-rate-history"
    data = http_get_json(url, {"instId": inst_id, "limit":"200"})
    if data.get("code") != "0":
        raise RuntimeError(f"OKX funding error: {data}")
    per_day = {}
    for it in data.get("data", []):
        d = to_date_ymd_ms(it.get("fundingTime") or it.get("ts"))
        r = float(it["fundingRate"])
        per_day.setdefault(d, []).append(r)
    days = sorted(per_day)
    out = [{"date": d, "val": sum(per_day[d])/len(per_day[d])} for d in days]
    return out[-(LOOKBACK_DAYS+1):]

def metric_funding_z(coin):
    # Bybit -> OKX
    try:
        f = funding_daily_bybit(coin["bybit"])
        if len(f) >= 11:
            latest = f[-1]["val"]
            hist = [x["val"] for x in f[:-1]]
            return z_score(hist, latest), len(f)
    except Exception:
        pass
    try:
        f = funding_daily_okx(coin["okx"])
        if len(f) >= 11:
            latest = f[-1]["val"]
            hist = [x["val"] for x in f[:-1]]
            return z_score(hist, latest), len(f)
    except Exception:
        pass
    return None, 0

# ---------- OI (OKX first, Bybit fallback, then snapshot series) ----------
def oi_daily_okx(inst_id):
    # Try 1D first
    url = f"{OKX}/api/v5/public/open-interest-history"
    try:
        d1 = http_get_json(url, {"instId": inst_id, "period":"1D", "limit":"200"})
        if d1.get("code") == "0" and d1.get("data"):
            arr = [{"date": to_date_ymd_ms(it["ts"]), "oi": float(it["oi"])} for it in d1["data"]]
            arr.sort(key=lambda x: x["date"])
            return arr[-(LOOKBACK_DAYS+8):]
    except Exception:
        pass
    # 8H -> daily last
    d2 = http_get_json(url, {"instId": inst_id, "period":"8H", "limit":"480"})
    if d2.get("code") != "0":
        raise RuntimeError(f"OKX OI error: {d2}")
    rows = [{"date": to_date_ymd_ms(it["ts"]), "oi": float(it["oi"])} for it in d2.get("data", [])]
    per_day = {}
    for r in rows:
        per_day[r["date"]] = r["oi"]  # last of each day
    days = sorted(per_day)
    out = [{"date": d, "oi": per_day[d]} for d in days]
    return out[-(LOOKBACK_DAYS+8):]

def oi_daily_bybit(symbol):
    url = f"{BYBIT}/v5/market/open-interest"
    data = http_get_json(url, {"category":"linear", "symbol":symbol, "intervalTime":"1d", "limit":"200"})
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit OI error: {data}")
    arr = [{"date": to_date_ymd_ms(it["timestamp"]), "oi": float(it["openInterest"])}
           for it in data.get("result", {}).get("list", [])]
    arr.sort(key=lambda x: x["date"])
    return arr[-(LOOKBACK_DAYS+8):]

def okx_current_oi(inst_id):
    url = f"{OKX}/api/v5/public/open-interest"
    data = http_get_json(url, {"instType":"SWAP", "instId":inst_id})
    if data.get("code") != "0":
        raise RuntimeError(f"OKX current OI error: {data}")
    row = (data.get("data") or [None])[0]
    if not row:
        raise RuntimeError("OKX current OI empty")
    return {"date": to_date_ymd_ms(row["ts"]), "oi": float(row["oi"])}

def load_series(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_series(path, series, cap=200):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if len(series) > cap:
        series = series[-cap:]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(series, f, ensure_ascii=False, separators=(",", ":"))

def compute_oi_delta_z_from_series(series):
    # series: list of {"date","oi"} ascending
    if len(series) < 14:
        return None, 0
    ch = []
    for i in range(len(series)):
        if i >= 3:
            prev = series[i-3]["oi"]
            now  = series[i]["oi"]
            ch.append((now/prev - 1.0) if prev else None)
        else:
            ch.append(None)
    last_val = None
    last_idx = -1
    for i in range(len(ch)-1, -1, -1):
        if ch[i] is not None:
            last_val = ch[i]; last_idx = i; break
    if last_idx < 0:
        return None, 0
    hist = [x for x in ch[:last_idx] if x is not None]
    return z_score(hist, last_val), len(hist) + 1

def metric_oi_delta_z(coin):
    # Try OKX history
    try:
        o = oi_daily_okx(coin["okx"])
        z, days = compute_oi_delta_z_from_series(o)
        if days > 0:
            return z, days, o  # include series for optional persistence
    except Exception:
        pass
    # Try Bybit history
    try:
        o = oi_daily_bybit(coin["bybit"])
        z, days = compute_oi_delta_z_from_series(o)
        if days > 0:
            return z, days, o
    except Exception:
        pass
    # Fallback: current OI snapshot + repo series
    series_path = os.path.join("data", f"oi_series_{coin['id']}.json")
    series = load_series(series_path)
    try:
        cur = okx_current_oi(coin["okx"])
        if series and series[-1]["date"] == cur["date"]:
            series[-1] = cur
        else:
            series.append(cur)
        save_series(series_path, series, cap=200)
    except Exception:
        # keep old series if current fetch fails
        pass
    z, days = compute_oi_delta_z_from_series(series)
    return z, days, series

# ---------- Sentiment (Fear & Greed) ----------
def fetch_fng():
    url = "https://api.alternative.me/fng/?limit=1&format=json"
    data = http_get_json(url)
    row = (data.get("data") or [None])[0]
    if not row:
        return None
    return {
        "value": int(row["value"]),
        "classification": str(row.get("value_classification") or ""),
        "timestamp": int(row["timestamp"])
    }

def main():
    items = []
    for c in COINS:
        fz, f_days = metric_funding_z(c)
        oz, o_days, _ = metric_oi_delta_z(c)
        items.append({
            "id": c["id"],
            "funding_z": fz,
            "oi_delta_z": oz,
            "funding_days": f_days,
            "oi_days": o_days
        })
        time.sleep(0.2)

    fng = fetch_fng()

    out = {
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lookback_days": LOOKBACK_DAYS,
        "source": "okx/bybit + fallback snapshot",
        "items": items,
        "sentiment": fng
    }

    os.makedirs("data", exist_ok=True)
    with open("data/derivs.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print("Wrote data/derivs.json")

if __name__ == "__main__":
    main()