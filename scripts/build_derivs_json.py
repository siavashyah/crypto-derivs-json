# scripts/build_derivs_json.py
# Builds data/derivs.json with funding_z and oi_delta_z (Bybit, free endpoints) for BTC/ETH.
# Safe-write (only overwrite when items >= MIN_ITEMS_TO_REPLACE) and base fallback support.

import json
import os
import math
import time
from datetime import datetime, timezone
from collections import defaultdict
import urllib.request
import urllib.parse
import urllib.error

LOOKBACK_DAYS = 90
COINS = [
  {"id": "bitcoin",  "symbol": "BTCUSDT"},
  {"id": "ethereum", "symbol": "ETHUSDT"},
]
MIN_ITEMS_TO_REPLACE = 1

# Bases: primary and optional fallback (set in Actions env)
PRIMARY_BASE = os.environ.get("BYBIT_BASE", "https://api.bybit.com")
FALLBACK_BASE = os.environ.get("BYBIT_BASE_FALLBACK", "").strip()
BASES = [PRIMARY_BASE] + ([FALLBACK_BASE] if FALLBACK_BASE else [])

def http_get(url, retry=3, sleep_ms=400):
  last_err = None
  for i in range(retry):
    try:
      req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "accept":"application/json"})
      with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")
    except Exception as e:
      last_err = e
      time.sleep((sleep_ms / 1000.0) * (2 ** i))
  raise last_err

def to_date_ymd(ms):
  ts = int(ms) / 1000.0
  return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")

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

def request_json(path, params):
  q = urllib.parse.urlencode(params)
  last_err = None
  for base in BASES:
    try:
      url = base.rstrip("/") + path + "?" + q
      raw = http_get(url)
      return json.loads(raw)
    except Exception as e:
      last_err = e
      continue
  raise last_err

def get_bybit_funding_daily(symbol, days=LOOKBACK_DAYS):
  per_page = 200
  need_points = 3 * days + 6
  cursor = None
  got = []
  attempts = 0

  while len(got) < need_points and attempts < 8:
    params = { "category":"linear", "symbol":symbol, "limit":str(per_page) }
    if cursor:
      params["cursor"] = cursor
    data = request_json("/v5/market/funding/history", params)
    if data.get("retCode") != 0:
      raise RuntimeError(f"Bybit funding error: {data}")
    items = data["result"].get("list", [])
    got.extend(items)
    cursor = data["result"].get("nextPageCursor")
    if not cursor or not items:
      break
    attempts += 1
    time.sleep(0.2)

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
  print(f"FUNDING {symbol}: points={len(got)} days={len(out)}")
  return out[-(days + 1):]

def get_bybit_oi_daily(symbol, days=LOOKBACK_DAYS):
  per_page = 200
  cursor = None
  got = []
  attempts = 0

  while len(got) < (days + 10) and attempts < 5:
    params = { "category":"linear", "symbol":symbol, "intervalTime":"1d", "limit":str(per_page) }
    if cursor:
      params["cursor"] = cursor
    data = request_json("/v5/market/open-interest", params)
    if data.get("retCode") != 0:
      raise RuntimeError(f"Bybit OI error: {data}")
    items = data["result"].get("list", [])
    got.extend(items)
    cursor = data["result"].get("nextPageCursor")
    if not cursor or not items:
      break
    attempts += 1
    time.sleep(0.2)

  arr = []
  for it in got:
    try:
      d = to_date_ymd(it["timestamp"])
      oi = float(it["openInterest"])
      arr.append({"date": d, "oi": oi})
    except Exception:
      continue

  arr.sort(key=lambda x: x["date"])
  print(f"OI {symbol}: rows={len(arr)}")
  return arr[-(days + 4):]

def compute_oi_3d_change(oi_series):
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
    print(f"SKIP {symbol}: insufficient aligned days ({len(dates)})")
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

def write_json_atomic(path, data, min_items=1):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  tmp = path + ".tmp"
  with open(tmp, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
  items_len = len(data.get("items", []))
  if items_len >= min_items:
    os.replace(tmp, path)
    print(f"WROTE {path} (items={items_len})")
  else:
    print(f"SKIP overwrite: items={items_len} < min_items={min_items}. Keeping previous file.")
    try:
      os.remove(tmp)
    except Exception:
      pass

def main():
  items = []
  for c in COINS:
    try:
      res = build_for_symbol(c["symbol"])
      if res is None:
        continue
      items.append({"id": c["id"], **res})
      print(f"DONE {c['symbol']}: fz={res['funding_z']}, oiz={res['oi_delta_z']}")
      time.sleep(0.25)
    except Exception as e:
      print(f"ERROR {c['symbol']}: {e}")

  out = {
    "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "lookback_days": LOOKBACK_DAYS,
    "source": "bybit",
    "items": items
  }
  write_json_atomic("data/derivs.json", out, min_items=MIN_ITEMS_TO_REPLACE)

if __name__ == "__main__":
  main()