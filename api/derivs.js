// api/derivs.js
// Robust derivs endpoint: Funding_z and OI_Delta_z with OKX-first OI,
// fallbacks to Bybit and r.jina.ai mirrors, and last-non-null 3D OI change.

export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 90;

    // Expand the list later if you wish
    const COINS = [
      { id: 'bitcoin',  bybit: 'BTCUSDT',     okx: 'BTC-USDT-SWAP' },
      { id: 'ethereum', bybit: 'ETHUSDT',     okx: 'ETH-USDT-SWAP' }
    ];

    // Multi-base fallbacks help when a CDN blocks Vercel’s region
    const BYBIT_BASES = [
      'https://api.bybit.com',
      'https://r.jina.ai/http://api.bybit.com'
    ];
    const OKX_BASES = [
      'https://www.okx.com',
      'https://r.jina.ai/http://www.okx.com'
    ];

    // Small helper to try multiple bases for one path+query
    async function httpJSONMulti(bases, pathWithQuery) {
      let lastErr = null;
      for (const base of bases) {
        const url = base.replace(/\/+$/, '') + pathWithQuery;
        try {
          const r = await fetch(url, {
            headers: { 'user-agent': 'Mozilla/5.0', 'accept': 'application/json' },
            cache: 'no-store'
          });
          if (!r.ok) { lastErr = new Error('HTTP ' + r.status + ' ' + url); continue; }
          return await r.json();
        } catch (e) {
          lastErr = e;
        }
      }
      throw lastErr || new Error('All bases failed for ' + pathWithQuery);
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

    function oi3d(oiArr) {
      return oiArr.map((x, i) => ({
        date: x.date,
        oi3: i >= 3 ? (oiArr[i].oi / oiArr[i - 3].oi - 1) : null
      }));
    }

    function lastNonNull(arr, key) {
      for (let i = arr.length - 1; i >= 0; i--) {
        const v = arr[i][key];
        if (v !== null && !Number.isNaN(Number(v))) return { idx: i, val: Number(v) };
      }
      return { idx: -1, val: null };
    }

    // ---------- Providers (with multi-base fallbacks) ----------

    // Bybit funding: 8h -> daily average
    async function fundingDailyBybit(symbol) {
      const path = `/v5/market/funding/history?category=linear&symbol=${encodeURIComponent(symbol)}&limit=200`;
      const data = await httpJSONMulti(BYBIT_BASES, path);
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

    // Bybit OI: 1d interval
    async function oiDailyBybit(symbol) {
      const path = `/v5/market/open-interest?category=linear&symbol=${encodeURIComponent(symbol)}&intervalTime=1d&limit=200`;
      const data = await httpJSONMulti(BYBIT_BASES, path);
      if (data.retCode !== 0) throw new Error('Bybit OI error ' + JSON.stringify(data));
      const arr = (data.result?.list || []).map(it => ({
        date: toDateYMDms(it.timestamp),
        oi: Number(it.openInterest)
      }));
      arr.sort((a, b) => a.date.localeCompare(b.date));
      return arr.slice(-(LOOKBACK_DAYS + 8));
    }

    // OKX funding: events -> daily average
    async function fundingDailyOKX(instId) {
      const path = `/api/v5/public/funding-rate-history?instId=${encodeURIComponent(instId)}&limit=200`;
      const data = await httpJSONMulti(OKX_BASES, path);
      if (data.code !== '0') throw new Error('OKX funding error ' + JSON.stringify(data));
      const perDay = {};
      (data.data || []).forEach(it => {
        const d = it.fundingTime ? toDateYMDms(it.fundingTime) : toDateYMDms(it.ts);
        const r = Number(it.fundingRate);
        if (!perDay[d]) perDay[d] = [];
        perDay[d].push(r);
      });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, val: perDay[d].reduce((a, b) => a + b, 0) / perDay[d].length }));
      return out.slice(-(LOOKBACK_DAYS + 1));
    }

    // OKX OI history, try 1D first, then 8H aggregated to daily
    async function oiDailyOKX(instId) {
      // 1) Try daily directly
      try {
        const path1 = `/api/v5/public/open-interest-history?instId=${encodeURIComponent(instId)}&period=1D&limit=200`;
        const d1 = await httpJSONMulti(OKX_BASES, path1);
        if (d1.code === '0' && Array.isArray(d1.data) && d1.data.length) {
          const arr = d1.data.map(it => ({
            date: toDateYMDms(it.ts),
            oi: Number(it.oi)
          }));
          arr.sort((a, b) => a.date.localeCompare(b.date));
          return arr.slice(-(LOOKBACK_DAYS + 8));
        }
      } catch (_) { /* fall through to 8H */ }

      // 2) Fallback to 8H -> collapse to daily (last of each day)
      const path2 = `/api/v5/public/open-interest-history?instId=${encodeURIComponent(instId)}&period=8H&limit=480`;
      const d2 = await httpJSONMulti(OKX_BASES, path2);
      if (d2.code !== '0') throw new Error('OKX OI error ' + JSON.stringify(d2));
      const rows = (d2.data || []).map(it => ({
        date: toDateYMDms(it.ts),
        oi: Number(it.oi)
      }));
      const perDay = {};
      rows.forEach(r => { perDay[r.date] = r.oi; });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, oi: perDay[d] }));
      return out.slice(-(LOOKBACK_DAYS + 8));
    }

    // ---------- Metrics per coin (OKX-first for OI) ----------

    async function metricFundingZ(coin) {
      // Prefer Bybit for funding; fallback to OKX
      try {
        const f = await fundingDailyBybit(coin.bybit);
        if (f.length >= 11) {
          const latest = f[f.length - 1].val;
          const hist = f.slice(0, -1).map(x => x.val);
          return { z: zScore(hist, latest), days: f.length, provider: 'bybit' };
        }
      } catch (_) {}
      try {
        const f = await fundingDailyOKX(coin.okx);
        if (f.length >= 11) {
          const latest = f[f.length - 1].val;
          const hist = f.slice(0, -1).map(x => x.val);
          return { z: zScore(hist, latest), days: f.length, provider: 'okx' };
        }
      } catch (_) {}
      return { z: null, days: 0, provider: null };
    }

    async function metricOIDeltaZ(coin) {
      // Prefer OKX for OI; fallback to Bybit
      try {
        const o = await oiDailyOKX(coin.okx);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const last = lastNonNull(o3, 'oi3');
          const hist = last.idx > 0 ? o3.slice(0, last.idx).map(x => x.oi3).filter(v => v !== null) : [];
          return { z: last.val === null ? null : zScore(hist, last.val), days: hist.length + (last.val !== null ? 1 : 0), provider: 'okx' };
        }
      } catch (_) {}
      try {
        const o = await oiDailyBybit(coin.bybit);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const last = lastNonNull(o3, 'oi3');
          const hist = last.idx > 0 ? o3.slice(0, last.idx).map(x => x.oi3).filter(v => v !== null) : [];
          return { z: last.val === null ? null : zScore(hist, last.val), days: hist.length + (last.val !== null ? 1 : 0), provider: 'bybit' };
        }
      } catch (_) {}
      return { z: null, days: 0, provider: null };
    }

    const debug = String(new URL(req.url, 'http://x')).searchParams.get('debug') === '1';
    const items = [];
    const diag = [];

    for (const c of COINS) {
      const [fz, oz] = await Promise.all([metricFundingZ(c), metricOIDeltaZ(c)]);
      items.push({
        id: c.id,
        funding_z: fz.z,
        oi_delta_z: oz.z,
        funding_days: fz.days,
        oi_days: oz.days
      });
      if (debug) {
        diag.push({ id: c.id, funding_provider: fz.provider, oi_provider: oz.provider, f_days: fz.days, oi_days: oz.days });
      }
      // polite backoff
      await new Promise(r => setTimeout(r, 120));
    }

    const out = {
      as_of: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
      lookback_days: LOOKBACK_DAYS,
      source: 'bybit_or_okx',
      items
    };
    if (debug) out.debug = diag;

    res.setHeader('content-type', 'application/json; charset=utf-8');
    res.setHeader('cache-control', 's-maxage=300, stale-while-revalidate=60');
    res.status(200).send(JSON.stringify(out));
  } catch (e) {
    res.status(500).send(JSON.stringify({ error: String(e) }));
  }
}