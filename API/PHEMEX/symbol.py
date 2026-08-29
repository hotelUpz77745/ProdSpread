# ============================================================
# FILE: API/PHEMEX/symbol.py
# ROLE: PHEMEX USDT-M Futures symbols & 24h volumes via REST.
# ============================================================

from __future__ import annotations

import asyncio
from typing import Dict, Optional, Any

import aiohttp
from utils import SessionManager


class PhemexSymbols:
    """PHEMEX USDT-M Futures: symbols + 24h volumes via REST."""

    BASE_URL = "https://api.phemex.com"

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

        raise RuntimeError(f"PHEMEX REST failed: {path} err={last_err}")

    async def get_volumes(self, quote: str = "USDT") -> Dict[str, float]:
        """Returns dict: coin -> 24h volume USD."""
        payload = await self._get_json("/md/v2/ticker/24hr/all")
        arr = payload.get("result", [])
        
        q = quote.upper()
        out: Dict[str, float] = {}
        
        if isinstance(arr, list):
            for it in arr:
                if not isinstance(it, dict):
                    continue
                sym_raw = str(it.get("symbol", ""))
                # Phemex spot pairs start with lowercase 's', e.g. sBTCUSDT.
                if sym_raw.startswith("s"):
                    continue
                    
                sym = sym_raw.upper()
                if not sym.endswith(q):
                    continue
                
                coin = sym[:-len(q)]
                # If there's an index like 1000PEPE, we leave it as is or normalize it? 
                # We'll leave normalization for later if needed, base logic is just slicing.
                
                vol = float(it.get("turnoverRv", 0.0))
                out[coin] = vol
                
        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = PhemexSymbols()
    vols = await api.get_volumes("USDT")
    print(f"PHEMEX active USDT-M symbols: {len(vols)}")
    print("Top 10 by vol:", sorted(vols.items(), key=lambda x: x[1], reverse=True)[:10])
    sm = SessionManager()
    await sm.close()

if __name__ == "__main__":
    asyncio.run(_main())
