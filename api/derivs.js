// api/derivs.js
// Funding_z (Bybit->OKX) and OI_Delta_z (OKX->Bybit->Binance) with fallbacks and debug info.

export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 90;

    const COINS = [
      { id: 'bitcoin',  bybit: 'BTCUSDT',     okx: 'BTC-USDT-SWAP', binance: 'BTCUSDT' },
      { id: 'ethereum', bybit: 'ETHUSDT',     okx: 'ETH-USDT-SWAP', binance: 'ETHUSDT' }
    ];

    const BYBIT_BASES = [
      'https://api.bybit.com',
      'https://r.jina.ai/http://api.bybit.com'
    ];
    const OKX_BASES = [
      'https://www.okx.com',
      'https://r.jina.ai/http://www.okx.com'
    ];
    const BINANCE_BASES = [
      'https://fapi.binance.com',
      'https://r.jina.ai/http://fapi.binance.com'
    ];

    function getDebugFlag(r) {
      try {
        if (r && r.query && (r.query.debug === '1' || r.query.debug === 'true' || r.query.debug === 1 || r.query.debug === true)) {
          return true;
        }
        if (r && typeof r.url === 'string') {
          const qIndex = r.url.indexOf('?');
          if (qIndex !== -1) {
            const qs = r.url.substring(qIndex + 1);
            const params = new URLSearchParams(qs);
            const d = params.get('debug');
            return d === '1' || d === 'true';
          }
        }
      } catch (_) {}
      return false;
    }
    const debug = getDebugFlag(req);

    async function httpJSONMulti(bases, pathWithQuery) {
      let lastErr = null;
      for (const base of bases) {
        const url = base.replace(/\/+$/, '') + pathWithQuery;
        try {
          const r = await fetch(url, {
            headers: { 'user-agent': 'curl/8.0', 'accept': 'application/json' },
            cache: 'no-store'
          });
          if (!r.ok) { lastErr = new Error('HTTP ' + r.status + ' ' + url); continue; }
          // tolerate non-JSON content-type if body is JSON text
          const text = await r.text();
          try { return JSON.parse(text); } catch (e) { lastErr = new Error('Parse error ' + url + ' ' + e); continue; }
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

    // ---------- Providers ----------

    // Bybit funding (8h -> daily avg)
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

    // OKX funding (events -> daily avg)
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

    // Bybit OI (1d)
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

    // OKX OI history: try 1D then 8H collapsed to daily
    async function oiDailyOKX(instId) {
      try {
        const path1 = `/api/v5/public/open-interest-history?instId=${encodeURIComponent(instId)}&period=1D&limit=200`;
        const d1 = await httpJSONMulti(OKX_BASES, path1);
        if (d1.code === '0' && Array.isArray(d1.data) && d1.data.length) {
          const arr = d1.data.map(it => ({ date: toDateYMDms(it.ts), oi: Number(it.oi) }));
          arr.sort((a, b) => a.date.localeCompare(b.date));
          return arr.slice(-(LOOKBACK_DAYS + 8));
        }
      } catch (_) { /* fall through */ }

      const path2 = `/api/v5/public/open-interest-history?instId=${encodeURIComponent(instId)}&period=8H&limit=480`;
      const d2 = await httpJSONMulti(OKX_BASES, path2);
      if (d2.code !== '0') throw new Error('OKX OI error ' + JSON.stringify(d2));
      const rows = (d2.data || []).map(it => ({ date: toDateYMDms(it.ts), oi: Number(it.oi) }));
      const perDay = {};
      rows.forEach(r => { perDay[r.date] = r.oi; });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, oi: perDay[d] }));
      return out.slice(-(LOOKBACK_DAYS + 8));
    }

    // Binance OI history (daily)
    async function oiDailyBinance(symbol) {
      const path = `/futures/data/openInterestHist?symbol=${encodeURIComponent(symbol)}&period=1d&limit=200`;
      const data = await httpJSONMulti(BINANCE_BASES, path);
      if (!Array.isArray(data) || data.length === 0) throw new Error('Binance OI error ' + JSON.stringify(data));
      const arr = data.map(it => ({
        date: toDateYMDms(it.timestamp),
        oi: Number(it.sumOpenInterest)
      }));
      arr.sort((a, b) => a.date.localeCompare(b.date));
      return arr.slice(-(LOOKBACK_DAYS + 8));
    }

    // ---------- Metrics builders ----------

    async function metricFundingZ(coin) {
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
      const errs = [];
      // OKX first
      try {
        const o = await oiDailyOKX(coin.okx);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const last = lastNonNull(o3, 'oi3');
          const hist = last.idx > 0 ? o3.slice(0, last.idx).map(x => x.oi3).filter(v => v !== null) : [];
          return { z: last.val === null ? null : zScore(hist, last.val), days: hist.length + (last.val !== null ? 1 : 0), provider: 'okx', err: null };
        }
        errs.push('okx:no-data');
      } catch (e1) { errs.push('okx:' + String(e1)); }

      // Bybit second
      try {
        const o = await oiDailyBybit(coin.bybit);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const last = lastNonNull(o3, 'oi3');
          const hist = last.idx > 0 ? o3.slice(0, last.idx).map(x => x.oi3).filter(v => v !== null) : [];
          return { z: last.val === null ? null : zScore(hist, last.val), days: hist.length + (last.val !== null ? 1 : 0), provider: 'bybit', err: null };
        }
        errs.push('bybit:no-data');
      } catch (e2) { errs.push('bybit:' + String(e2)); }

      // Binance last
      try {
        const o = await oiDailyBinance(coin.binance);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const last = lastNonNull(o3, 'oi3');
          const hist = last.idx > 0 ? o3.slice(0, last.idx).map(x => x.oi3).filter(v => v !== null) : [];
          return { z: last.val === null ? null : zScore(hist, last.val), days: hist.length + (last.val !== null ? 1 : 0), provider: 'binance', err: null };
        }
        errs.push('binance:no-data');
      } catch (e3) { errs.push('binance:' + String(e3)); }

      return { z: null, days: 0, provider: null, err: errs.join(' | ') };
    }

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
        diag.push({
          id: c.id,
          funding_provider: fz.provider,
          oi_provider: oz.provider,
          f_days: fz.days,
          oi_days: oz.days,
          oi_err: oz.err || null
        });
      }
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