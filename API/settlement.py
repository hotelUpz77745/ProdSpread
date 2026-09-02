# ============================================================
# FILE: API/settlement.py
# ROLE: Сбор фактического биржевого PnL и комиссий после закрытия позиций.
# ============================================================

import asyncio
import time
import hmac
import hashlib
import base64
from typing import Dict, Any, Optional
import aiohttp
from c_log import log

class ExchangeSettlement:
    def __init__(self, 
                 binance_key: str, binance_secret: str,
                 kucoin_key: str, kucoin_secret: str, kucoin_passphrase: str,
                 bitget_key: str = "", bitget_secret: str = "", bitget_passphrase: str = "",
                 session: Optional[aiohttp.ClientSession] = None):
        self.binance_key = binance_key
        self.binance_secret = binance_secret
        self.kucoin_key = kucoin_key
        self.kucoin_secret = kucoin_secret
        self.kucoin_passphrase = kucoin_passphrase
        self.bitget_key = bitget_key
        self.bitget_secret = bitget_secret
        self.bitget_passphrase = bitget_passphrase
        self._session = session
        self._bnb_price_cache = {"price": 0.0, "ts": 0.0}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        from utils import SessionManager
        return await SessionManager().get_session()

    async def get_bnb_price(self) -> float:
        now = time.time()
        if self._bnb_price_cache["price"] > 0 and (now - self._bnb_price_cache["ts"] < 300):
            return self._bnb_price_cache["price"]
        try:
            session = await self._get_session()
            async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=BNBUSDT") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = float(data.get("price", 0.0))
                    self._bnb_price_cache = {"price": price, "ts": now}
                    return price
        except Exception as e:
            log(f"[Settlement] Error getting BNB price: {e}", level="WARNING")
        return self._bnb_price_cache.get("price", 600.0)

    async def get_binance_trade_pnl(self, symbol: str, start_time_ms: int, position_side: str = None) -> Dict[str, Any]:
        """
        Запрашивает фактические трейды по символу на Binance начиная с start_time_ms.
        Возвращает:
          - realized_pnl: чистый PnL по трейдам закрытия (USDT)
          - commission: суммарная списанная комиссия (USDT)
          - net_pnl: realized_pnl - commission (USDT)
        """
        if not self.binance_key or not self.binance_secret:
            return {"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}

        try:
            session = await self._get_session()
            timestamp = int(time.time() * 1000)
            # Запрашиваем с небольшим запасом -500мс
            query_start = max(0, start_time_ms - 500)
            query_string = f"symbol={symbol}&startTime={query_start}&recvWindow=10000&timestamp={timestamp}"
            signature = hmac.new(self.binance_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
            url = f"https://fapi.binance.com/fapi/v1/userTrades?{query_string}&signature={signature}"
            headers = {"X-MBX-APIKEY": self.binance_key}

            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    trades = await resp.json()
                    if isinstance(trades, list):
                        realized_pnl = 0.0
                        total_commission_usd = 0.0
                        bnb_price = await self.get_bnb_price()

                        for t in trades:
                            if position_side and t.get("positionSide", "").upper() != position_side.upper():
                                continue
                            
                            realized_pnl += float(t.get("realizedPnl", 0.0))
                            comm = float(t.get("commission", 0.0))
                            asset = str(t.get("commissionAsset", "")).upper()
                            
                            if asset == "BNB":
                                total_commission_usd += comm * bnb_price
                            else:
                                total_commission_usd += comm

                        net_pnl = realized_pnl - total_commission_usd
                        return {
                            "realized_pnl": realized_pnl,
                            "commission": total_commission_usd,
                            "net_pnl": net_pnl,
                            "trades_count": len(trades)
                        }
                else:
                    err = await resp.text()
                    log(f"[Settlement] Binance userTrades error ({resp.status}): {err}", level="WARNING")
        except Exception as e:
            log(f"[Settlement] Binance userTrades exception: {e}", level="ERROR")

        return {"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}

    async def get_kucoin_position_pnl(self, symbol: str, start_time_ms: int) -> Dict[str, Any]:
        """
        Запрашивает историю закрытых позиций на KuCoin (/api/v1/history-positions).
        На KuCoin поле `pnl` УЖЕ включает в себя все торговые комиссии.
        """
        if not self.kucoin_key or not self.kucoin_secret:
            return {"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}

        try:
            session = await self._get_session()
            endpoint = "/api/v1/history-positions"
            query_str = "?pageSize=10"
            now = str(int(time.time() * 1000))
            str_to_sign = now + "GET" + endpoint + query_str
            sig = base64.b64encode(hmac.new(self.kucoin_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
            passphrase_hmac = hmac.new(self.kucoin_secret.encode('utf-8'), self.kucoin_passphrase.encode('utf-8'), hashlib.sha256)
            encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')

            headers = {
                'KC-API-KEY': self.kucoin_key,
                'KC-API-SIGN': sig,
                'KC-API-TIMESTAMP': now,
                'KC-API-PASSPHRASE': encrypted_passphrase,
                'KC-API-KEY-VERSION': '2'
            }
            url = f"https://api-futures.kucoin.com{endpoint}{query_str}"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == "200000" and data.get("data"):
                        items = data["data"].get("items", [])
                        for item in items:
                            if item.get("symbol") == symbol:
                                close_time = int(item.get("closeTime", 0))
                                open_time = int(item.get("openTime", 0))
                                # Проверяем, что позиция относится к нашей сделке
                                if close_time >= (start_time_ms - 2000) or open_time >= (start_time_ms - 2000):
                                    net_pnl = float(item.get("pnl", 0.0))
                                    trade_fee = float(item.get("tradeFee", 0.0))
                                    gross_pnl = float(item.get("realisedGrossCost", 0.0))
                                    return {
                                        "realized_pnl": gross_pnl,
                                        "commission": trade_fee,
                                        "net_pnl": net_pnl,
                                        "close_price": float(item.get("closePrice", 0.0)),
                                        "open_price": float(item.get("openPrice", 0.0))
                                    }
                else:
                    err = await resp.text()
                    log(f"[Settlement] Kucoin history-positions error ({resp.status}): {err}", level="WARNING")
        except Exception as e:
            log(f"[Settlement] Kucoin history-positions exception: {e}", level="ERROR")

        return {"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}

    def _generate_bitget_signature(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(bytes(self.bitget_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod=hashlib.sha256)
        return base64.b64encode(mac.digest()).decode('utf-8')

    async def get_bitget_position_pnl(self, symbol: str, start_time_ms: int) -> Dict[str, Any]:
        """
        Запрашивает историю закрытых позиций на Bitget (/api/v2/mix/position/history-position).
        Поле `netProfit` на Bitget УЖЕ включает в себя комиссии и фандинг.
        """
        if not self.bitget_key or not self.bitget_secret:
            return {"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}

        try:
            session = await self._get_session()
            now = str(int(time.time() * 1000))
            clean_symbol = symbol.replace("_UMCBL", "").strip().upper()
            endpoint = f"/api/v2/mix/position/history-position?productType=USDT-FUTURES&symbol={clean_symbol}&pageSize=5"
            sig = self._generate_bitget_signature(now, "GET", endpoint)

            headers = {
                'ACCESS-KEY': self.bitget_key,
                'ACCESS-SIGN': sig,
                'ACCESS-TIMESTAMP': now,
                'ACCESS-PASSPHRASE': self.bitget_passphrase,
                'Content-Type': 'application/json'
            }
            url = f"https://api.bitget.com{endpoint}"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == "00000" and data.get("data"):
                        items = data["data"].get("list", [])
                        for item in items:
                            if item.get("symbol") == clean_symbol:
                                u_time = int(item.get("uTime") or item.get("cTime") or 0)
                                if u_time >= (start_time_ms - 3000):
                                    net_pnl = float(item.get("netProfit", 0.0))
                                    open_fee = float(item.get("openFeeTotal", 0.0))
                                    close_fee = float(item.get("closeFeeTotal", 0.0))
                                    gross_pnl = float(item.get("cumRealisedPnl", 0.0))
                                    return {
                                        "realized_pnl": gross_pnl,
                                        "commission": open_fee + close_fee,
                                        "net_pnl": net_pnl,
                                        "close_price": float(item.get("closePriceAvg", 0.0)),
                                        "open_price": float(item.get("openPriceAvg", 0.0))
                                    }
                else:
                    err = await resp.text()
                    log(f"[Settlement] Bitget history-position error ({resp.status}): {err}", level="WARNING")
        except Exception as e:
            log(f"[Settlement] Bitget history-position exception: {e}", level="ERROR")

        return {"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}

    async def settle_trade(self, 
                           sym: str, 
                           native_long: str, 
                           native_short: str, 
                           long_ex: str, 
                           short_ex: str, 
                           open_time_ms: int, 
                           total_investment_usd: float,
                           delay_sec: float = 1.8) -> Dict[str, Any]:
        """
        Главный метод клиринга: ждет delay_sec, запрашивает биржи и возвращает
        точный суммарный биржевой Net PnL и Net Yield в процентах.
        """
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

        tasks = []
        if long_ex.upper() == "BINANCE":
            tasks.append(self.get_binance_trade_pnl(native_long, open_time_ms, position_side="LONG"))
        elif long_ex.upper() == "KUCOIN":
            tasks.append(self.get_kucoin_position_pnl(native_long, open_time_ms))
        elif long_ex.upper() == "BITGET":
            tasks.append(self.get_bitget_position_pnl(native_long, open_time_ms))
        else:
            tasks.append(asyncio.sleep(0, result={"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}))

        if short_ex.upper() == "BINANCE":
            tasks.append(self.get_binance_trade_pnl(native_short, open_time_ms, position_side="SHORT"))
        elif short_ex.upper() == "KUCOIN":
            tasks.append(self.get_kucoin_position_pnl(native_short, open_time_ms))
        elif short_ex.upper() == "BITGET":
            tasks.append(self.get_bitget_position_pnl(native_short, open_time_ms))
        else:
            tasks.append(asyncio.sleep(0, result={"realized_pnl": 0.0, "commission": 0.0, "net_pnl": 0.0}))

        long_res, short_res = await asyncio.gather(*tasks)

        long_net_usd = float(long_res.get("net_pnl", 0.0))
        short_net_usd = float(short_res.get("net_pnl", 0.0))
        total_comm_usd = float(long_res.get("commission", 0.0)) + float(short_res.get("commission", 0.0))
        total_net_pnl_usd = long_net_usd + short_net_usd

        net_yield_pct = (total_net_pnl_usd / total_investment_usd) if total_investment_usd > 0 else 0.0

        res = {
            "symbol": sym,
            "long_ex": long_ex,
            "short_ex": short_ex,
            "long_net_pnl_usd": long_net_usd,
            "short_net_pnl_usd": short_net_usd,
            "total_commission_usd": total_comm_usd,
            "total_net_pnl_usd": total_net_pnl_usd,
            "net_yield_pct": net_yield_pct,
            "is_profit": total_net_pnl_usd >= 0.0
        }

        log(f"[{sym}] 🏛️ Биржевой клиринг: {long_ex} Net: {long_net_usd:+.4f}$ | {short_ex} Net: {short_net_usd:+.4f}$ | Total: {total_net_pnl_usd:+.4f}$ ({net_yield_pct*100:+.3f}%) | Fees: {total_comm_usd:.4f}$", level="INFO")
        return res
