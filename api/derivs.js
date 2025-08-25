// api/derivs.js
// Returns funding_z and oi_delta_z for BTC/ETH (Bybit first, OKX fallback).
// Designed to avoid empty `items` by computing each metric independently.

export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 90;

    const COINS = [
      { id: 'bitcoin',  bybit: 'BTCUSDT',     okx: 'BTC-USDT-SWAP' },
      { id: 'ethereum', bybit: 'ETHUSDT',     okx: 'ETH-USDT-SWAP' }
      // Add more: { id: 'solana', bybit: 'SOLUSDT', okx: 'SOL-USDT-SWAP' }
    ];

    const BYBIT = 'https://api.bybit.com';
    const OKX   = 'https://www.okx.com';

    async function httpJSON(url) {
      const r = await fetch(url, {
        headers: { 'user-agent': 'Mozilla/5.0', 'accept': 'application/json' },
        cache: 'no-store'
      });
      if (!r.ok) throw new Error('HTTP ' + r.status + ' ' + url);
      return r.json();
    }

    function toDateYMDms(ms) {
      const n = Number(ms);
      return new Date(n).toISOString().slice(0, 10);
    }

    function zScore(series, latest) {
      const xs = series.filter(x => x !== null && !Number.isNaN(Number(x))).map(Number);
      if (xs.length < 10) return null;
      const mean = xs.reduce((a, b) => a + b, 0) / xs.length;
      const sd = Math.sqrt(xs.reduce((a, b) => a + (b - mean) * (b - mean), 0) / xs.length) || 0;
      if (sd === 0) return 0;
      return (Number(latest) - mean) / sd;
    }

    // ---------- Bybit providers ----------
    async function fundingDailyBybit(symbol) {
      const url = `${BYBIT}/v5/market/funding/history?category=linear&symbol=${symbol}&limit=200`;
      const data = await httpJSON(url);
      if (data.retCode !== 0) throw new Error('Bybit funding error ' + JSON.stringify(data));
      const perDay = {};
      (data.result?.list || []).forEach(it => {
        const d = toDateYMDms(it.fundingRateTimestamp);
        const r = Number(it.fundingRate);
        if (!perDay[d]) perDay[d] = [];
        perDay[d].push(r);
      });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, val: perDay[d].reduce((a, b) => a + b, 0) / perDay[d].length }));
      return out.slice(-(LOOKBACK_DAYS + 1));
    }

    async function oiDailyBybit(symbol) {
      const url = `${BYBIT}/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=1d&limit=200`;
      const data = await httpJSON(url);
      if (data.retCode !== 0) throw new Error('Bybit OI error ' + JSON.stringify(data));
      const arr = (data.result?.list || []).map(it => ({
        date: toDateYMDms(it.timestamp),
        oi: Number(it.openInterest)
      }));
      arr.sort((a, b) => a.date.localeCompare(b.date));
      return arr.slice(-(LOOKBACK_DAYS + 4));
    }

    // ---------- OKX providers ----------
    async function fundingDailyOKX(instId) {
      const url = `${OKX}/api/v5/public/funding-rate-history?instId=${instId}&limit=200`;
      const data = await httpJSON(url);
      if (data.code !== '0') throw new Error('OKX funding error ' + JSON.stringify(data));
      const perDay = {};
      (data.data || []).forEach(it => {
        // fundingTime (ms) or fallback ts
        const d = it.fundingTime ? toDateYMDms(it.fundingTime) : toDateYMDms(it.ts);
        const r = Number(it.fundingRate);
        if (!perDay[d]) perDay[d] = [];
        perDay[d].push(r);
      });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, val: perDay[d].reduce((a, b) => a + b, 0) / perDay[d].length }));
      return out.slice(-(LOOKBACK_DAYS + 1));
    }

    async function oiDailyOKX(instId) {
      // Use 8H granularity and take end-of-day as daily
      const url = `${OKX}/api/v5/public/open-interest-history?instId=${instId}&period=8H&limit=300`;
      const data = await httpJSON(url);
      if (data.code !== '0') throw new Error('OKX OI error ' + JSON.stringify(data));
      const rows = (data.data || []).map(it => ({
        date: toDateYMDms(it.ts),
        oi: Number(it.oi)
      }));
      const perDay = {};
      rows.forEach(r => { perDay[r.date] = r.oi; }); // last value per day
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, oi: perDay[d] }));
      return out.slice(-(LOOKBACK_DAYS + 4));
    }

    function oi3d(oiArr) {
      return oiArr.map((x, i) => ({
        date: x.date,
        oi3: i >= 3 ? (oiArr[i].oi / oiArr[i - 3].oi - 1) : null
      }));
    }

    async function metricFundingZ(coin) {
      // Try Bybit, then OKX
      try {
        const f = await fundingDailyBybit(coin.bybit);
        if (f.length >= 11) {
          const latest = f[f.length - 1].val;
          const hist = f.slice(0, -1).map(x => x.val);
          return { z: zScore(hist, latest), days: f.length };
        }
      } catch (_) {}
      try {
        const f = await fundingDailyOKX(coin.okx);
        if (f.length >= 11) {
          const latest = f[f.length - 1].val;
          const hist = f.slice(0, -1).map(x => x.val);
          return { z: zScore(hist, latest), days: f.length };
        }
      } catch (_) {}
      return { z: null, days: 0 };
    }

    async function metricOIDeltaZ(coin) {
      // Try Bybit, then OKX
      try {
        const o = await oiDailyBybit(coin.bybit);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const latest = o3[o3.length - 1].oi3;
          const hist = o3.slice(0, -1).map(x => x.oi3).filter(v => v !== null);
          return { z: latest === null ? null : zScore(hist, latest), days: hist.length + 1 };
        }
      } catch (_) {}
      try {
        const o = await oiDailyOKX(coin.okx);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const latest = o3[o3.length - 1].oi3;
          const hist = o3.slice(0, -1).map(x => x.oi3).filter(v => v !== null);
          return { z: latest === null ? null : zScore(hist, latest), days: hist.length + 1 };
        }
      } catch (_) {}
      return { z: null, days: 0 };
    }

    const items = [];
    for (const c of COINS) {
      const [fz, oz] = await Promise.all([metricFundingZ(c), metricOIDeltaZ(c)]);
      // Include the coin even if one metric is null (Sheet will ingest whichever exists)
      if (fz.z !== null || oz.z !== null) {
        items.push({
          id: c.id,
          funding_z: fz.z,
          oi_delta_z: oz.z,
          funding_days: fz.days,
          oi_days: oz.days
        });
      } else {
        // As last resort, still include with nulls so you can see the coin in JSON
        items.push({
          id: c.id,
          funding_z: null,
          oi_delta_z: null,
          funding_days: fz.days,
          oi_days: oz.days
        });
      }
      // polite backoff between coins
      await new Promise(r => setTimeout(r, 150));
    }

    const out = {
      as_of: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
      lookback_days: LOOKBACK_DAYS,
      source: 'bybit_or_okx',
      items
    };

    res.setHeader('content-type', 'application/json; charset=utf-8');
    res.setHeader('cache-control', 's-maxage=300, stale-while-revalidate=60');
    res.status(200).send(JSON.stringify(out));
  } catch (e) {
    res.status(500).send(JSON.stringify({ error: String(e) }));
  }
}