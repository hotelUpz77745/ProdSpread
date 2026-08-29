# ============================================================
# FILE: API/BITGET/symbol.py
# ROLE: BITGET USDT-M Futures symbols & 24h volumes via REST.
# ============================================================

from __future__ import annotations

import asyncio
from typing import Dict, Optional, Any

import aiohttp
from utils import SessionManager


class BitgetSymbols:
    """BITGET USDT-M Futures: symbols + 24h volumes via REST."""

    BASE_URL = "https://api.bitget.com"

    def __init__(self, timeout_sec: float = 20.0, retries: int = 3):
        self._retries = int(retries)

    async def _get_session(self) -> aiohttp.ClientSession:
        return await SessionManager().get_session()

    async def aclose(self) -> None:
        pass  # Managed by SessionManager

    async def _get_json(self, path: str, params: Optional[dict] = None) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(1, self._retries + 1):
            try:
                session = await self._get_session()
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"HTTP {resp.status}: {text}")
                    data = await resp.json()
                    if not isinstance(data, dict):
                        raise RuntimeError(f"Bad JSON root: {type(data)}")
                    return data
            except Exception as e:
                last_err = e
                if attempt < self._retries:
                    await asyncio.sleep(0.35 * attempt)
                else:
                    break

        raise RuntimeError(f"BITGET REST failed: {path} err={last_err}")

    async def get_volumes(self, quote: str = "USDT") -> Dict[str, float]:
        """Returns dict: coin -> 24h volume USD."""
        payload = await self._get_json("/api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
        arr = payload.get("data", [])
        
        q = quote.upper()
        out: Dict[str, float] = {}
        
        if isinstance(arr, list):
            for it in arr:
                if not isinstance(it, dict):
                    continue
                sym = str(it.get("symbol", "")).upper()
                if not sym.endswith(q):
                    continue
                
                coin = sym[:-len(q)]
                vol = float(it.get("usdtVolume", 0.0) or it.get("quoteVolume", 0.0))
                out[coin] = vol
                
        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = BitgetSymbols()
    vols = await api.get_volumes("USDT")
    print(f"BITGET active USDT-M symbols: {len(vols)}")
    print("Top 10 by vol:", sorted(vols.items(), key=lambda x: x[1], reverse=True)[:10])
    sm = SessionManager()
    await sm.close()

if __name__ == "__main__":
    asyncio.run(_main())
