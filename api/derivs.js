// api/derivs.js
// Vercel Serverless function that returns funding_z and oi_delta_z for BTC/ETH.
// Primary: Bybit (free). Fallback: OKX (free).

export default async function handler(req, res) {
  try {
    const LOOKBACK_DAYS = 90;

    const COINS = [
      { id: 'bitcoin',  bybit: 'BTCUSDT',     okx: 'BTC-USDT-SWAP' },
      { id: 'ethereum', bybit: 'ETHUSDT',     okx: 'ETH-USDT-SWAP' }
      // Add more: { id: 'solana', bybit: 'SOLUSDT', okx: 'SOL-USDT-SWAP' },
    ];

    const BYBIT = 'https://api.bybit.com';
    const OKX   = 'https://www.okx.com';

    async function httpJSON(url) {
      const r = await fetch(url, {
        headers: { 'user-agent': 'Mozilla/5.0', 'accept': 'application/json' }
      });
      if (!r.ok) throw new Error('HTTP ' + r.status + ' ' + url);
      return r.json();
    }

    function toDateYMDms(ms) {
      const ts = Number(ms);
      return new Date(ts).toISOString().slice(0, 10); // YYYY-MM-DD
    }
    function toDateYMDsec(secEpoch) {
      const ms = Number(secEpoch) * 1000;
      return new Date(ms).toISOString().slice(0, 10);
    }

    function zScore(series, latest) {
      const xs = series.filter(x => x !== null && !Number.isNaN(Number(x))).map(Number);
      if (xs.length < 10) return null;
      const mean = xs.reduce((a,b)=>a+b,0) / xs.length;
      const sd = Math.sqrt(xs.reduce((a,b)=>a+(b-mean)*(b-mean),0) / xs.length) || 0;
      if (sd === 0) return 0;
      return (Number(latest) - mean) / sd;
    }

    // Bybit: funding 8h → daily average
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
      const out = days.map(d => ({ date: d, val: perDay[d].reduce((a,b)=>a+b,0)/perDay[d].length }));
      return out.slice(-(LOOKBACK_DAYS + 1));
    }

    // Bybit: daily open interest
    async function oiDailyBybit(symbol) {
      const url = `${BYBIT}/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=1d&limit=200`;
      const data = await httpJSON(url);
      if (data.retCode !== 0) throw new Error('Bybit OI error ' + JSON.stringify(data));
      const arr = (data.result?.list || []).map(it => ({
        date: toDateYMDms(it.timestamp),
        oi: Number(it.openInterest)
      }));
      arr.sort((a,b)=> a.date.localeCompare(b.date));
      return arr.slice(-(LOOKBACK_DAYS + 4));
    }

    // OKX: funding rate history (already per funding event)
    async function fundingDailyOKX(instId) {
      const url = `${OKX}/api/v5/public/funding-rate-history?instId=${instId}&limit=200`;
      const data = await httpJSON(url);
      if (data.code !== '0') throw new Error('OKX funding error ' + JSON.stringify(data));
      const perDay = {};
      (data.data || []).forEach(it => {
        // OKX fields: fundingRate, fundingTime (ms), or ts
        const d = it.fundingTime ? toDateYMDms(it.fundingTime) : toDateYMDms(it.ts);
        const r = Number(it.fundingRate);
        if (!perDay[d]) perDay[d] = [];
        perDay[d].push(r);
      });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, val: perDay[d].reduce((a,b)=>a+b,0)/perDay[d].length }));
      return out.slice(-(LOOKBACK_DAYS + 1));
    }

    // OKX: open interest history (use 8H and take end-of-day)
    async function oiDailyOKX(instId) {
      const url = `${OKX}/api/v5/public/open-interest-history?instId=${instId}&period=8H&limit=300`;
      const data = await httpJSON(url);
      if (data.code !== '0') throw new Error('OKX OI error ' + JSON.stringify(data));
      const rows = (data.data || []).map(it => ({
        date: toDateYMDms(it.ts),
        oi: Number(it.oi)
      }));
      // collapse 8H to one reading per day (use last reading of each day)
      const perDay = {};
      rows.forEach(r => { perDay[r.date] = r.oi; });
      const days = Object.keys(perDay).sort();
      const out = days.map(d => ({ date: d, oi: perDay[d] }));
      return out.slice(-(LOOKBACK_DAYS + 4));
    }

    function oi3d(oiArr) {
      return oiArr.map((x,i) => ({
        date: x.date,
        oi3: i >= 3 ? (oiArr[i].oi / oiArr[i-3].oi - 1) : null
      }));
    }

    async function buildOne(coin) {
      // Try Bybit → fallback to OKX
      let f = null, o = null;
      try {
        f = await fundingDailyBybit(coin.bybit);
        o = await oiDailyBybit(coin.bybit);
      } catch (e) {
        // Fallback to OKX
        try {
          f = await fundingDailyOKX(coin.okx);
          o = await oiDailyOKX(coin.okx);
        } catch (e2) {
          throw new Error('Both providers failed for ' + coin.id);
        }
      }
      const o3 = oi3d(o);
      const fd = Object.fromEntries(f.map(x => [x.date, x.val]));
      const o3d = Object.fromEntries(o3.map(x => [x.date, x.oi3]));
      const dates = Object.keys(fd).filter(d => d in o3d).sort();
      if (dates.length < 15) return null;

      const fundingSeries = dates.slice(0, -1).map(d => fd[d]);
      const fundingLatest = fd[dates[dates.length-1]];
      const oiSeries = dates.slice(0, -1).map(d => o3d[d]).filter(x => x !== null);
      const oiLatest = o3d[dates[dates.length-1]];

      const funding_z = zScore(fundingSeries, fundingLatest);
      const oi_delta_z = (oiLatest === null ? null : zScore(oiSeries, oiLatest));

      return {
        id: coin.id,
        funding_z,
        oi_delta_z,
        funding_days: fundingSeries.length + 1,
        oi_days: oiSeries.length + 1
      };
    }

    const items = [];
    for (const c of COINS) {
      try {
        const res1 = await buildOne(c);
        if (res1) items.push(res1);
      } catch (e) {
        // skip this coin
      }
      // polite backoff between coins
      await new Promise(r => setTimeout(r, 200));
    }

    const out = {
      as_of: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
      lookback_days: LOOKBACK_DAYS,
      source: 'bybit_or_okx',
      items
    };

    res.setHeader('content-type', 'application/json; charset=utf-8');
    // Optional caching (5 minutes at the edge)
    res.setHeader('cache-control', 's-maxage=300, stale-while-revalidate=60');
    res.status(200).send(JSON.stringify(out));
  } catch (e) {
    res.status(500).send(JSON.stringify({ error: String(e) }));
  }
}
