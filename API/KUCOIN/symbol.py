# ============================================================
# FILE: API/KUCOIN/symbol.py
# ROLE: KuCoin Futures symbols & 24h volumes via REST.
# ENDPOINT: GET https://api-futures.kucoin.com/api/v1/contracts/active
# NOTE: Single responsibility: ONLY fetch + filter symbols + volumes.
#       Filters: status=Open, quoteAsset=USDT, contract suffix USDTM.
#       XBT alias: KuCoin calls BTC as 'XBT' -> normalized to 'BTC'.
# ============================================================

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import aiohttp


# KuCoin uses XBT as alias for BTC
_BASE_ALIASES: Dict[str, str] = {"XBT": "BTC"}


def _normalize_base(coin: str) -> str:
    return _BASE_ALIASES.get(coin.upper(), coin.upper())


class KucoinSymbols:
    """KuCoin Futures: symbols + 24h volumes via REST.

    Filters applied:
        - status == 'Open'
        - quoteAsset == quote (default: 'USDT')
        - symbol suffix == {quote}M (e.g. USDTM)

    Returns:
        dict[coin: str, vol_usd: float]
        coin = normalized base asset (XBT -> BTC alias applied)
    """

    BASE_URL = "https://api-futures.kucoin.com"

    def __init__(self, timeout_sec: float = 20.0, retries: int = 3):
        self._retries = int(retries)

    async def _get_session(self) -> aiohttp.ClientSession:
        from utils import SessionManager
        return await SessionManager().get_session()

    async def aclose(self) -> None:
        pass  # Managed by SessionManager

    async def _get_json(self, path: str) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(1, self._retries + 1):
            try:
                session = await self._get_session()
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    data = await resp.json()
                    if not isinstance(data, dict):
                        raise RuntimeError(f"Bad JSON root: {type(data)}")
                    return data
            except Exception as e:
                last_err = e
                s = (str(e) or "").lower()
                if "session is closed" in s or "connector is closed" in s:
                    self._session = None
                if attempt < self._retries:
                    await asyncio.sleep(0.4 * attempt)
        raise RuntimeError(f"KuCoin REST failed: {path} err={last_err}")

    @staticmethod
    def _match_quote(item: Dict[str, Any], quote: str) -> bool:
        q = quote.upper()
        for k in ("quoteCurrency", "rootSymbol", "settleCurrency"):
            v = item.get(k)
            if v and str(v).upper() == q:
                return True
        return False

    async def get_volumes(self, quote: str = "USDT") -> Dict[str, float]:
        """Returns dict: coin -> 24h volume USD. Only Open USDT-M contracts."""
        js = await self._get_json("/api/v1/contracts/active")
        items = js.get("data") or []
        if isinstance(items, dict):
            items = [items]

        q = quote.upper()
        suffix = q + "M"
        out: Dict[str, float] = {}

        for it in items:
            if not isinstance(it, dict):
                continue
            # Explicit status check
            if str(it.get("status", "")).strip() != "Open":
                continue
            # Quote filter
            if not self._match_quote(it, quote):
                continue
            sym = str(it.get("symbol", "")).upper()
            if not sym.endswith(suffix):
                continue
            coin_raw = sym[: -len(suffix)]
            coin = _normalize_base(coin_raw)
            out[coin] = float(it.get("turnoverOf24h", 0))

        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = KucoinSymbols()
    vols = await api.get_volumes("USDT")
    print(f"KUCOIN active USDT-M symbols: {len(vols)}")
    print("Top 10 by vol:", sorted(vols.items(), key=lambda x: x[1], reverse=True)[:10])
    print("BTC vol:", vols.get("BTC"))  # confirm XBT->BTC alias
    await api.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
