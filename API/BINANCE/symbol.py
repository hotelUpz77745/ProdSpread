# ============================================================
# FILE: API/BINANCE/symbol.py
# ROLE: Binance USDT-M Futures symbols & 24h volumes via REST.
# ENDPOINTS:
#   GET /fapi/v1/exchangeInfo  -> filters: PERPETUAL + TRADING + quoteAsset=USDT
#   GET /fapi/v1/ticker/24hr   -> 24h volumes
# NOTE: Single responsibility: ONLY fetch + filter symbols + volumes.
# ============================================================

from __future__ import annotations

import asyncio
from typing import Dict, Optional, Set

import aiohttp


class BinanceSymbols:
    """Binance USDT-M Futures: symbols + 24h volumes via REST.

    Filters applied:
        - contractType == 'PERPETUAL'
        - status == 'TRADING'
        - quoteAsset == quote (default: 'USDT')

    Returns:
        dict[coin: str, vol_usd: float]
        coin = base asset only, e.g. 'BTC' (without quote suffix)
    """

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, timeout_sec: float = 20.0, retries: int = 3):
        self._retries = int(retries)

    async def _get_session(self) -> aiohttp.ClientSession:
        from utils import SessionManager
        return await SessionManager().get_session()

    async def aclose(self) -> None:
        pass  # Managed by SessionManager

    async def _get_json(self, path: str):
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(1, self._retries + 1):
            try:
                session = await self._get_session()
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    return await resp.json()
            except Exception as e:
                last_err = e
                s = (str(e) or "").lower()
                if "session is closed" in s or "connector is closed" in s:
                    self._session = None
                if attempt < self._retries:
                    await asyncio.sleep(0.4 * attempt)
        raise RuntimeError(f"Binance REST failed: {path} err={last_err}")

    async def _get_active_perp_set(self, quote: str = "USDT") -> Set[str]:
        """Returns set of base coins that are PERPETUAL + TRADING + quote."""
        data = await self._get_json("/fapi/v1/exchangeInfo")
        q = quote.upper()
        active: Set[str] = set()
        for s in data.get("symbols", []) or []:
            if not isinstance(s, dict):
                continue
            if s.get("contractType") != "PERPETUAL":
                continue
            if s.get("status") != "TRADING":
                continue
            if (s.get("quoteAsset") or "").upper() != q:
                continue
            coin = s.get("baseAsset", "")
            if coin:
                active.add(coin.upper())
        return active

    async def get_volumes(self, quote: str = "USDT") -> Dict[str, float]:
        """Returns dict: coin -> 24h volume USD. Only active PERPETUAL USDT contracts."""
        active_set, ticker_data = await asyncio.gather(
            self._get_active_perp_set(quote),
            self._get_json("/fapi/v1/ticker/24hr"),
        )
        q = quote.upper()
        out: Dict[str, float] = {}
        for item in ticker_data:
            sym = item.get("symbol", "")
            if not sym.endswith(q):
                continue
            coin = sym[:-len(q)]
            if coin not in active_set:
                continue
            out[coin] = float(item.get("quoteVolume", 0))
        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = BinanceSymbols()
    vols = await api.get_volumes("USDT")
    print(f"BINANCE active PERP USDT symbols: {len(vols)}")
    print("Top 10 by vol:", sorted(vols.items(), key=lambda x: x[1], reverse=True)[:10])
    await api.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
