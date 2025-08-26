// api/derivs.js (replace your current file with this)
export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 90;

    const COINS = [
      { id: 'bitcoin',  bybit: 'BTCUSDT',     okx: 'BTC-USDT-SWAP' },
      { id: 'ethereum', bybit: 'ETHUSDT',     okx: 'ETH-USDT-SWAP' }
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

    // Bybit funding (8h -> daily avg)
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

    // Bybit OI (daily)
    async function oiDailyBybit(symbol) {
      const url = `${BYBIT}/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=1d&limit=200`;
      const data = await httpJSON(url);
      if (data.retCode !== 0) throw new Error('Bybit OI error ' + JSON.stringify(data));
      const arr = (data.result?.list || []).map(it => ({
        date: toDateYMDms(it.timestamp),
        oi: Number(it.openInterest)
      }));
      arr.sort((a, b) => a.date.localeCompare(b.date));
      return arr.slice(-(LOOKBACK_DAYS + 8)); // a little extra
    }

    // OKX funding (events -> daily avg)
    async function fundingDailyOKX(instId) {
      const url = `${OKX}/api/v5/public/funding-rate-history?instId=${instId}&limit=200`;
      const data = await httpJSON(url);
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

    // OKX OI (8H -> daily last)
    async function oiDailyOKX(instId) {
      const url = `${OKX}/api/v5/public/open-interest-history?instId=${instId}&period=8H&limit=480`; // ~160 days
      const data = await httpJSON(url);
      if (data.code !== '0') throw new Error('OKX OI error ' + JSON.stringify(data));
      const rows = (data.data || []).map(it => ({
        date: toDateYMDms(it.ts),
        oi: Number(it.oi)
      }));
      const perDay = {};
      rows.forEach(r => { perDay[r.date] = r.oi; });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, oi: perDay[d] }));
      return out.slice(-(LOOKBACK_DAYS + 8));
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

    async function metricFundingZ(coin) {
      try {
        const f = await fundingDailyBybit(coin.bybit);
        if (f.length >= 11) return { z: zScore(f.slice(0, -1).map(x => x.val), f[f.length - 1].val), days: f.length };
      } catch (_) {}
      try {
        const f = await fundingDailyOKX(coin.okx);
        if (f.length >= 11) return { z: zScore(f.slice(0, -1).map(x => x.val), f[f.length - 1].val), days: f.length };
      } catch (_) {}
      return { z: null, days: 0 };
    }

    async function metricOIDeltaZ(coin) {
      try {
        const o = await oiDailyBybit(coin.bybit);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const last = lastNonNull(o3, 'oi3'); // use last non-null change
          const hist = o3.slice(0, last.idx).map(x => x.oi3).filter(v => v !== null);
          return { z: last.idx === -1 ? null : zScore(hist, last.val), days: hist.length + 1 };
        }
      } catch (_) {}
      try {
        const o = await oiDailyOKX(coin.okx);
        if (o.length >= 14) {
          const o3 = oi3d(o);
          const last = lastNonNull(o3, 'oi3');
          const hist = o3.slice(0, last.idx).map(x => x.oi3).filter(v => v !== null);
          return { z: last.idx === -1 ? null : zScore(hist, last.val), days: hist.length + 1 };
        }
      } catch (_) {}
      return { z: null, days: 0 };
    }

    const items = [];
    for (const c of COINS) {
      const [fz, oz] = await Promise.all([metricFundingZ(c), metricOIDeltaZ(c)]);
      items.push({
        id: c.id,
        funding_z: fz.z,
        oi_delta_z: oz.z,
        funding_days: fz.days,
        oi_days: oz.days
      });
      await new Promise(r => setTimeout(r, 120));
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