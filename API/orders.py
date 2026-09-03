# ============================================================
# FILE: API/orders.py
# ROLE: Управление ордерами (создание, отмена, отслеживание).
# ============================================================
import aiohttp
from c_log import log
import asyncio
from typing import Optional
import time
import hmac
import hashlib
import base64
import uuid
import json
from decimal import Decimal, ROUND_HALF_UP

def round_by_step(value: float, step_str) -> str:
    try:
        step = Decimal(str(step_str)).normalize()
        val = Decimal(str(value))
        precision = max(0, -step.as_tuple().exponent)
        steps = (val / step).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
        quantized_val = steps * step
        return f"{quantized_val:.{precision}f}"
    except Exception:
        return str(value)

class InsufficientMarginError(Exception):
    pass

class BinanceOrder:
    def __init__(self, api_key: str, api_secret: str, session: aiohttp.ClientSession, position_stream=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = session
        self.position_stream = position_stream
        self.symbol_info = None
        self._bg_task = None
        self._keepalive_task = None

    def start(self):
        """Запускает фоновые таски. Вызывать ПОСЛЕ старта event loop."""
        self._bg_task = asyncio.create_task(self._fetch_exchange_info_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def _keepalive_loop(self):
        while True:
            try:
                await asyncio.sleep(120)
                await self.warmup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"[BinanceOrder] Keepalive error: {e}", level="WARNING")

    async def _fetch_exchange_info_loop(self):
        while True:
            try:
                async with self.session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.symbol_info = data.get("symbols", [])
                        log("[BinanceOrder] Exchange specs updated.", level="INFO")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"[BinanceOrder] Error fetching specs: {e}", level="ERROR")
                
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break

    @staticmethod
    def get_spec_precisions(symbol_info, symbol):
        if not symbol_info:
            return None
        symbol_data = next((item for item in symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            return None

        lot_size_filter = next((f for f in symbol_data.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None)
        price_filter = next((f for f in symbol_data.get("filters", []) if f.get("filterType") == "PRICE_FILTER"), None)

        if not lot_size_filter or not price_filter:
            return None

        def count_decimal_places(number):
            try:
                d = Decimal(str(number)).normalize()
                return max(0, -d.as_tuple().exponent)
            except Exception:
                return 0

        qty_precission = count_decimal_places(lot_size_filter['stepSize'])
        price_precision = count_decimal_places(price_filter['tickSize'])

        return qty_precission, price_precision

    def _generate_signature(self, query_string: str) -> str:
        return hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    def check_order_size(self, symbol: str, size_usd: float, price: float):
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required to calculate quantity")
            
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Exchange specs not loaded yet")
            
        symbol_data = next((item for item in self.symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            raise ValueError(f"[{symbol}] Precision rules not found in specs")
            
        lot_size_filter = next((f for f in symbol_data.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None)
        step_size = lot_size_filter.get('stepSize', '1') if lot_size_filter else '1'
        
        raw_qty = size_usd / price
        qty_str = round_by_step(raw_qty, step_size)
        
        if float(qty_str) <= 0:
            raise ValueError(f"[{symbol}] Calculated order size is 0 after rounding (raw_qty={raw_qty}, step_size={step_size}).")


    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None, time_in_force: str = "IOC"):
        self.last_activity_ts = __import__("time").time()
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required to calculate quantity")
            
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Exchange specs not loaded yet")
            
        symbol_data = next((item for item in self.symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            raise ValueError(f"[{symbol}] Precision rules not found in specs")
            
        lot_size_filter = next((f for f in symbol_data.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None)
        price_filter = next((f for f in symbol_data.get("filters", []) if f.get("filterType") == "PRICE_FILTER"), None)
        
        step_size = lot_size_filter.get('stepSize', '1') if lot_size_filter else '1'
        tick_size = price_filter.get('tickSize', '0.01') if price_filter else '0.01'
        
        raw_qty = size_usd / price
        qty_str = round_by_step(raw_qty, step_size)
        price_str = round_by_step(price, tick_size)
        
        if float(qty_str) <= 0:
            raise ValueError(f"[{symbol}] Calculated order size is 0 after rounding (raw_qty={raw_qty}, step_size={step_size}).")


        timestamp = int(time.time() * 1000)
        
        pos_side_str = f"&positionSide={position_side.upper()}" if position_side else ""
        
        if order_type.upper() == "LIMIT":
            tif = (time_in_force or "IOC").upper()
            query_string = f"symbol={symbol}&side={side.upper()}{pos_side_str}&type=LIMIT&timeInForce={tif}&quantity={qty_str}&price={price_str}&timestamp={timestamp}"
        else:
            query_string = f"symbol={symbol}&side={side.upper()}{pos_side_str}&type=MARKET&quantity={qty_str}&timestamp={timestamp}"
            
        signature = self._generate_signature(query_string)
        
        url = f"https://fapi.binance.com/fapi/v1/order?{query_string}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        
        log(f"[BinanceOrder] POST {url}", level="DEBUG")
        async with self.session.post(url, headers=headers) as resp:
            data = await resp.json()
            if resp.status != 200:
                msg = data.get("msg", "")
                log(f"[BinanceOrder] API Error: {msg}", level="ERROR")
                if "Margin is insufficient" in msg or "Balance insufficient" in msg:
                    raise InsufficientMarginError(msg)
                raise Exception(f"Binance API Error: {msg}")
            log(f"[BinanceOrder] Ордер исполнен: {data}", level="INFO")
            return data

    async def cancel_all_orders(self, symbol: str):
        self.last_activity_ts = __import__("time").time()
        timestamp = int(time.time() * 1000)
        query_string = f"symbol={symbol}&timestamp={timestamp}"
        signature = self._generate_signature(query_string)
        
        url = f"https://fapi.binance.com/fapi/v1/allOpenOrders?{query_string}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        
        try:
            async with self.session.delete(url, headers=headers) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("code") == 200:
                    log(f"[BinanceOrder] Отменены все ордера по {symbol}", level="INFO")
        except Exception as e:
            log(f"[BinanceOrder] Ошибка отмены ордеров {symbol}: {e}", level="ERROR")

    async def set_margin_type(self, symbol: str, margin_type: str, **kwargs) -> bool:
        for attempt in range(3):
            try:
                timestamp = int(time.time() * 1000)
                query_string = f"symbol={symbol}&marginType={margin_type.upper()}&timestamp={timestamp}"
                signature = self._generate_signature(query_string)
                url = f"https://fapi.binance.com/fapi/v1/marginType?{query_string}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}
                
                async with self.session.post(url, headers=headers) as resp:
                    data = await resp.json()
                    if resp.status == 200:
                        return True
                    if resp.status == 429 or data.get("code") == 429 or data.get("code") == -1003:
                        await asyncio.sleep(1.0 + attempt * 0.5)
                        continue
                    if data.get("code") == -4046: # No need to change
                        return True
                    log(f"[BinanceOrder] Ошибка set_margin_type {symbol}: {data}", level="WARNING")
                    return False
            except Exception as e:
                log(f"[BinanceOrder] Исключение set_margin_type {symbol}: {e}", level="ERROR")
                return False
        return False

    async def set_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        for attempt in range(3):
            try:
                timestamp = int(time.time() * 1000)
                query_string = f"symbol={symbol}&leverage={leverage}&timestamp={timestamp}"
                signature = self._generate_signature(query_string)
                url = f"https://fapi.binance.com/fapi/v1/leverage?{query_string}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}
                
                async with self.session.post(url, headers=headers) as resp:
                    data = await resp.json()
                    if resp.status == 200:
                        return True
                    if resp.status == 429 or data.get("code") == 429 or data.get("code") == -1003:
                        await asyncio.sleep(1.0 + attempt * 0.5)
                        continue
                    msg = str(data.get("msg", "")).lower()
                    if "no need to change" in msg:
                        return True
                    log(f"[BinanceOrder] Ошибка set_leverage {symbol}: {data}", level="WARNING")
                    return False
            except Exception as e:
                log(f"[BinanceOrder] Исключение set_leverage {symbol}: {e}", level="ERROR")
                return False
        return False

    async def warmup(self):
        if __import__("time").time() - getattr(self, "last_activity_ts", 0.0) < 30.0:
            return
        """Отправляет фейковый POST запрос для прогрева WAF/Gateway и TLS туннеля"""
        try:
            timestamp = int(time.time() * 1000)
            query_string = f"symbol=INVALID_PAIR_WARMUP&side=BUY&type=LIMIT&timeInForce=IOC&quantity=1&price=1&timestamp={timestamp}"
            signature = self._generate_signature(query_string)
            url = f"https://fapi.binance.com/fapi/v1/order?{query_string}&signature={signature}"
            headers = {"X-MBX-APIKEY": self.api_key}
            
            async with self.session.post(url, headers=headers) as resp:
                await resp.read()
                # Убираем лог, чтобы не спамить каждые 2 минуты
        except Exception:
            pass

    def get_executed_position(self, symbol: str, side: str):
        if self.position_stream:
            return self.position_stream.get_position(symbol, side)
        return {"size": 0.0, "price": 0.0}

    async def get_position_rest(self, symbol: str, side: str = None) -> dict:
        if not self.api_key:
            return {"size": 0.0, "price": 0.0, "status": "ok"}
        try:
            timestamp = int(time.time() * 1000)
            query_string = f"symbol={symbol}&timestamp={timestamp}"
            sig = self._generate_signature(query_string)
            url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query_string}&signature={sig}"
            async with self.session.get(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for p in data:
                        pos_side = (p.get("positionSide") or "BOTH").upper()
                        if side and pos_side != "BOTH" and pos_side != side.upper():
                            continue
                        amt = abs(float(p.get("positionAmt", 0)))
                        price = float(p.get("entryPrice", 0))
                        if amt > 0:
                            if self.position_stream:
                                self.position_stream.positions.setdefault(symbol, {})[pos_side] = {"size": amt, "price": price}
                            return {"size": amt, "price": price, "status": "ok"}
                    return {"size": 0.0, "price": 0.0, "status": "ok"}
                else:
                    log(f"[BinanceOrder] get_position_rest HTTP {resp.status}", level="WARNING")
                    return {"size": 0.0, "price": 0.0, "status": "error"}
        except Exception as e:
            log(f"[BinanceOrder] get_position_rest network error: {e}", level="WARNING")
            return {"size": 0.0, "price": 0.0, "status": "error"}

    async def get_exact_position(self, symbol: str, side: str) -> dict:
        return await self.get_exact_position_guarded(symbol, side)

    async def get_exact_position_guarded(self, symbol: str, side: str, max_retries: int = 3, retry_delay: float = 0.015) -> dict:
        self.last_activity_ts = __import__("time").time()
        for attempt in range(max_retries):
            pos = await self.get_position_rest(symbol, side)
            if pos.get("status") == "ok":
                if pos.get("size", 0.0) > 0:
                    return pos
                # Если биржа вернула честный 0.0
                if self.position_stream and symbol in self.position_stream.positions:
                    self.position_stream.positions[symbol][side.upper()] = {"size": 0.0, "price": 0.0}
                return {"size": 0.0, "price": 0.0, "status": "ok"}
            # Сетевой сбой - ждем и ретраим
            await asyncio.sleep(retry_delay * (attempt + 1))

        # Если все ретраи упали - берем последнее известное значение из WS
        ws_pos = self.get_executed_position(symbol, side)
        log(f"[BinanceOrder] REST моргнул после {max_retries} ретраев, страховка WS: {ws_pos}", level="WARNING")
        return {"size": ws_pos.get("size", 0.0), "price": ws_pos.get("price", 0.0), "status": "fallback_ws"}

    async def get_active_positions(self) -> list:
        if not self.api_key:
            return []
        timestamp = int(time.time() * 1000)
        query_string = f"timestamp={timestamp}"
        sig = self._generate_signature(query_string)
        url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query_string}&signature={sig}"
        async with self.session.get(url, headers={"X-MBX-APIKEY": self.api_key}) as resp:
            if resp.status == 200:
                data = await resp.json()
                active = []
                for p in data:
                    amt = float(p.get("positionAmt", 0))
                    if amt != 0:
                        active.append({"symbol": p["symbol"], "size": amt})
                return active
        return []

class KucoinOrder:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, session: aiohttp.ClientSession, position_stream, margin_settings: dict):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.session = session
        self.position_stream = position_stream
        self.margin_settings = margin_settings
        self.symbol_info = None
        self._bg_task = None
        self._keepalive_task = None

    def start(self):
        """Запускает фоновые таски. Вызывать ПОСЛЕ старта event loop."""
        self._bg_task = asyncio.create_task(self._fetch_exchange_info_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def _keepalive_loop(self):
        while True:
            try:
                await asyncio.sleep(120)
                await self.warmup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"[KucoinOrder] Keepalive error: {e}", level="WARNING")

    async def _fetch_exchange_info_loop(self):
        while True:
            try:
                async with self.session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("code") == "200000":
                            self.symbol_info = data.get("data", [])
                            log("[KucoinOrder] Exchange specs updated.", level="INFO")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"[KucoinOrder] Error fetching specs: {e}", level="ERROR")
                
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break

    @staticmethod
    def get_spec_precisions(symbol_info, symbol):
        if not symbol_info:
            return None
        symbol_data = next((item for item in symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            return None
            
        def count_decimal_places(number):
            try:
                d = Decimal(str(number))
                return max(0, -d.as_tuple().exponent)
            except Exception:
                return 0
            
        qty_precision = count_decimal_places(symbol_data.get('lotSize', 1))
        price_precision = count_decimal_places(symbol_data.get('tickSize', 0.1))
        multiplier = float(symbol_data.get('multiplier', 1.0))
        
        return qty_precision, price_precision, multiplier

    def _generate_signature(self, str_to_sign: str) -> str:
        h = hmac.new(self.api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(h.digest()).decode('utf-8')

    def check_order_size(self, symbol: str, size_usd: float, price: float):
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required to calculate quantity")
            
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Exchange specs not loaded yet")
            
        symbol_data = next((item for item in self.symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            raise ValueError(f"[{symbol}] Precision rules not found in specs")
            
        lot_size = symbol_data.get('lotSize', 1)
        multiplier = float(symbol_data.get('multiplier', 1.0))
        
        raw_lots = (size_usd / price) / multiplier
        qty_str = round_by_step(raw_lots, lot_size)
        
        if float(qty_str) <= 0:
            raise ValueError(f"[{symbol}] Calculated order size is 0 after rounding (raw_lots={raw_lots}, lot_size={lot_size}, multiplier={multiplier}).")


    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None, time_in_force: str = "IOC"):
        self.last_activity_ts = __import__("time").time()
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required to calculate quantity")
            
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Exchange specs not loaded yet")
            
        symbol_data = next((item for item in self.symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            raise ValueError(f"[{symbol}] Precision rules not found in specs")
            
        lot_size = symbol_data.get('lotSize', 1)
        tick_size = symbol_data.get('tickSize', 0.1)
        multiplier = float(symbol_data.get('multiplier', 1.0))
        
        raw_lots = (size_usd / price) / multiplier
        
        qty_str = round_by_step(raw_lots, lot_size)
        price_str = round_by_step(price, tick_size)
        
        if float(qty_str) <= 0:
            raise ValueError(f"[{symbol}] Calculated order size is 0 after rounding (raw_lots={raw_lots}, lot_size={lot_size}, multiplier={multiplier}).")
        
        endpoint = "/api/v1/orders"
        now = str(int(time.time() * 1000))
        
        leverage = str(self.margin_settings["leverage"])
        marginMode = str(self.margin_settings["margin_type"]).upper()

        body = {
            "clientOid": str(uuid.uuid4()),
            "symbol": symbol,
            "side": side.lower(),
            "size": qty_str,
            "leverage": leverage,
            "marginMode": marginMode
        }
        
        if order_type.upper() == "LIMIT":
            body["type"] = "limit"
            body["timeInForce"] = (time_in_force or "IOC").upper()
            body["price"] = price_str
        else:
            body["type"] = "market"
            
        if position_side:
            body["positionSide"] = position_side.upper()
            if (side.lower() == "buy" and position_side.upper() == "SHORT") or \
               (side.lower() == "sell" and position_side.upper() == "LONG"):
                body["reduceOnly"] = True
        
        body_str = json.dumps(body)
        str_to_sign = now + "POST" + endpoint + body_str
        signature = self._generate_signature(str_to_sign)
        
        passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
        encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
        
        headers = {
            'KC-API-KEY': self.api_key,
            'KC-API-SIGN': signature,
            'KC-API-TIMESTAMP': now,
            'KC-API-PASSPHRASE': encrypted_passphrase,
            'KC-API-KEY-VERSION': '2',
            'Content-Type': 'application/json'
        }
        
        url = f"https://api-futures.kucoin.com{endpoint}"
        log(f"[KucoinOrder] POST {url}", level="DEBUG")
        async with self.session.post(url, headers=headers, data=body_str) as resp:
            data = await resp.json()
            if data.get('code') != '200000':
                msg = data.get("msg", str(data))
                log(f"[KucoinOrder] API Error: {msg}", level="ERROR")
                if "Balance" in msg or "Margin" in msg:
                    raise InsufficientMarginError(msg)
                raise Exception(f"Kucoin API Error: {msg}")
            log(f"[KucoinOrder] Ордер исполнен: {data}", level="INFO")
            return data

    async def cancel_all_orders(self, symbol: str):
        self.last_activity_ts = __import__("time").time()
        endpoint = f"/api/v1/orders?symbol={symbol}"
        now = str(int(time.time() * 1000))
        str_to_sign = now + "DELETE" + endpoint
        signature = self._generate_signature(str_to_sign)
        
        passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
        encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
        
        headers = {
            'KC-API-KEY': self.api_key,
            'KC-API-SIGN': signature,
            'KC-API-TIMESTAMP': now,
            'KC-API-PASSPHRASE': encrypted_passphrase,
            'KC-API-KEY-VERSION': '2'
        }
        
        url = f"https://api-futures.kucoin.com{endpoint}"
        try:
            async with self.session.delete(url, headers=headers) as resp:
                data = await resp.json()
                if data.get('code') == '200000':
                    log(f"[KucoinOrder] Отменены все ордера по {symbol}", level="INFO")
        except Exception as e:
            log(f"[KucoinOrder] Ошибка отмены ордеров {symbol}: {e}", level="ERROR")

    async def warmup(self):
        if __import__("time").time() - getattr(self, "last_activity_ts", 0.0) < 30.0:
            return
        """Отправляет фейковый POST запрос для прогрева WAF/Gateway и TLS туннеля"""
        try:
            endpoint = "/api/v1/orders"
            now = str(int(time.time() * 1000))
            
            body = {
                "clientOid": str(uuid.uuid4()),
                "symbol": "INVALID_PAIR_WARMUP",
                "side": "buy",
                "type": "limit",
                "timeInForce": "IOC",
                "size": "1",
                "price": "1"
            }
            body_str = json.dumps(body)
            str_to_sign = now + "POST" + endpoint + body_str
            signature = self._generate_signature(str_to_sign)
            
            passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
            encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
            
            headers = {
                'KC-API-KEY': self.api_key,
                'KC-API-SIGN': signature,
                'KC-API-TIMESTAMP': now,
                'KC-API-PASSPHRASE': encrypted_passphrase,
                'KC-API-KEY-VERSION': '2',
                'Content-Type': 'application/json'
            }
            
            url = f"https://api-futures.kucoin.com{endpoint}"
            async with self.session.post(url, headers=headers, data=body_str) as resp:
                await resp.read()
                # Убираем лог, чтобы не спамить
        except Exception:
            pass

    async def get_active_positions(self) -> list:
        if not self.api_key:
            return []
        endpoint = "/api/v1/positions"
        now = str(int(time.time() * 1000))
        str_to_sign = now + "GET" + endpoint
        sig = self._generate_signature(str_to_sign)
        
        passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
        encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
        
        headers = {
            'KC-API-KEY': self.api_key,
            'KC-API-SIGN': sig,
            'KC-API-TIMESTAMP': now,
            'KC-API-PASSPHRASE': encrypted_passphrase,
            'KC-API-KEY-VERSION': '2'
        }
        
        url = f"https://api-futures.kucoin.com{endpoint}"
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                active = []
                if data.get('code') == '200000':
                    for p in data.get('data', []):
                        amt = float(p.get("currentQty", 0))
                        if amt != 0:
                            active.append({"symbol": p["symbol"], "size": amt})
                return active
        return []

    async def set_margin_type(self, symbol: str, margin_type: str, leverage: int = None) -> bool:
        """
        Переключает режим маржи для символа.
        Для ISOLATED: leverage передается в том же запросе (биржа обновляет его сразу).
        Для CROSS: leverage не передается здесь, он ставится отдельно через changeCrossUserLeverage.
        """
        for attempt in range(3):
            try:
                now = str(int(time.time() * 1000))
                body = {"symbol": symbol, "marginMode": margin_type.upper()}
                if margin_type.upper() == "ISOLATED" and leverage is not None:
                    body["leverage"] = str(leverage)
                body_str = json.dumps(body)
                endpoint = "/api/v2/position/changeMarginMode"
                str_to_sign = now + "POST" + endpoint + body_str
                signature = self._generate_signature(str_to_sign)
                
                passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
                encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
                
                headers = {
                    'KC-API-KEY': self.api_key,
                    'KC-API-SIGN': signature,
                    'KC-API-TIMESTAMP': now,
                    'KC-API-PASSPHRASE': encrypted_passphrase,
                    'KC-API-KEY-VERSION': '2',
                    'Content-Type': 'application/json'
                }
                
                url = f"https://api-futures.kucoin.com{endpoint}"
                async with self.session.post(url, headers=headers, data=body_str) as resp:
                    data = await resp.json()
                    if data.get('code') == '200000':
                        return True
                    if resp.status == 429 or data.get("code") == "429000" or data.get("code") == "400014":
                        await asyncio.sleep(1.0 + attempt * 0.5)
                        continue
                    log(f"[KucoinOrder] Ошибка set_margin_type {symbol}: {data}", level="WARNING")
                    return False
            except Exception as e:
                log(f"[KucoinOrder] Исключение set_margin_type {symbol}: {e}", level="ERROR")
                return False
        return False
        
    async def set_leverage(self, symbol: str, leverage: int, margin_type: str = "CROSS") -> bool:
        """
        Устанавливает плечо для символа.
        - CROSS: POST /api/v2/changeCrossUserLeverage
        - ISOLATED: плечо уже передано в set_margin_type через changeMarginMode.
                    Здесь ничего не делаем — возвращаем True.
        """
        if margin_type.upper() == "ISOLATED":
            # Для изолированной маржи плечо уже выставлено в set_margin_type
            return True
        for attempt in range(3):
            try:
                now = str(int(time.time() * 1000))
                body = {"symbol": symbol, "leverage": str(leverage)}
                body_str = json.dumps(body)
                endpoint = "/api/v2/changeCrossUserLeverage"
                str_to_sign = now + "POST" + endpoint + body_str
                signature = self._generate_signature(str_to_sign)
                
                passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
                encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
                
                headers = {
                    'KC-API-KEY': self.api_key,
                    'KC-API-SIGN': signature,
                    'KC-API-TIMESTAMP': now,
                    'KC-API-PASSPHRASE': encrypted_passphrase,
                    'KC-API-KEY-VERSION': '2',
                    'Content-Type': 'application/json'
                }
                
                url = f"https://api-futures.kucoin.com{endpoint}"
                async with self.session.post(url, headers=headers, data=body_str) as resp:
                    data = await resp.json()
                    if data.get('code') == '200000':
                        return True
                    if resp.status == 429 or data.get("code") == "429000" or data.get("code") == "400014":
                        await asyncio.sleep(1.0 + attempt * 0.5)
                        continue
                    log(f"[KucoinOrder] Ошибка set_leverage {symbol}: {data}", level="WARNING")
                    return False
            except Exception as e:
                log(f"[KucoinOrder] Исключение set_leverage {symbol}: {e}", level="ERROR")
                return False
        return False

    def get_multiplier(self, symbol: str) -> float:
        if self.symbol_info:
            item = next((x for x in self.symbol_info if x.get('symbol') == symbol), None)
            if item:
                return float(item.get('multiplier', 1.0))
        return 1.0

    async def get_position_rest(self, symbol: str, side: str = None) -> dict:
        if not self.api_key:
            return {"size": 0.0, "price": 0.0, "status": "ok"}
        try:
            endpoint = f"/api/v1/position?symbol={symbol}"
            if side:
                endpoint += f"&positionSide={side.upper()}"
            now = str(int(time.time() * 1000))
            str_to_sign = now + "GET" + endpoint
            sig = self._generate_signature(str_to_sign)
            passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
            encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
            headers = {
                'KC-API-KEY': self.api_key,
                'KC-API-SIGN': sig,
                'KC-API-TIMESTAMP': now,
                'KC-API-PASSPHRASE': encrypted_passphrase,
                'KC-API-KEY-VERSION': '2'
            }
            url = f"https://api-futures.kucoin.com{endpoint}"
            async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == "200000" and data.get("data"):
                        pos_data = data["data"]
                        multiplier = self.get_multiplier(symbol)
                        raw_lots = float(pos_data.get("currentQty", 0))
                        amt = raw_lots * multiplier
                        price = float(pos_data.get("avgEntryPrice", 0))
                        pos_side = (pos_data.get("positionSide") or ("LONG" if raw_lots > 0 else "SHORT")).upper()
                        if amt != 0:
                            res_pos = {"size": abs(amt), "price": price, "status": "ok"}
                            if self.position_stream:
                                self.position_stream.positions.setdefault(symbol, {})[pos_side] = {"size": abs(raw_lots), "price": price}
                            if not side or side.upper() == pos_side:
                                return res_pos
                    return {"size": 0.0, "price": 0.0, "status": "ok"}
                else:
                    log(f"[KucoinOrder] get_position_rest HTTP {resp.status}", level="WARNING")
                    return {"size": 0.0, "price": 0.0, "status": "error"}
        except Exception as e:
            log(f"[KucoinOrder] get_position_rest network error: {e}", level="WARNING")
            return {"size": 0.0, "price": 0.0, "status": "error"}

    def get_executed_position(self, symbol: str, side: str):
        if self.position_stream:
            pos = self.position_stream.get_position(symbol, side)
            multiplier = self.get_multiplier(symbol)
            return {
                "size": pos.get("size", 0.0) * multiplier,
                "price": pos.get("price", 0.0)
            }
        return {"size": 0.0, "price": 0.0}

    async def get_exact_position(self, symbol: str, side: str) -> dict:
        return await self.get_exact_position_guarded(symbol, side)

    async def get_exact_position_guarded(self, symbol: str, side: str, max_retries: int = 3, retry_delay: float = 0.015) -> dict:
        self.last_activity_ts = __import__("time").time()
        for attempt in range(max_retries):
            pos = await self.get_position_rest(symbol, side)
            if pos.get("status") == "ok":
                if pos.get("size", 0.0) > 0:
                    return pos
                if self.position_stream and symbol in self.position_stream.positions:
                    self.position_stream.positions[symbol][side.upper()] = {"size": 0.0, "price": 0.0}
                return {"size": 0.0, "price": 0.0, "status": "ok"}
            await asyncio.sleep(retry_delay * (attempt + 1))

        ws_pos = self.get_executed_position(symbol, side)
        log(f"[KucoinOrder] REST моргнул после {max_retries} ретраев, страховка WS: {ws_pos}", level="WARNING")
        return {"size": ws_pos.get("size", 0.0), "price": ws_pos.get("price", 0.0), "status": "fallback_ws"}



class OkxOrder:

    async def _keepalive_loop(self):
        while True:
            try:
                import asyncio
                await asyncio.sleep(120)
                await self.warmup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                pass

    async def warmup(self):
        import time
        if time.time() - getattr(self, "last_activity_ts", 0.0) < 30.0:
            return
        try:
            timestamp = str(int(time.time() * 1000))
            body = '{"instId":"INVALID-SWAP","tdMode":"cross","side":"buy","ordType":"limit","sz":"1","px":"1"}'
            signature = self._generate_signature(timestamp, "POST", "/api/v5/trade/order", body)
            headers = {
                "OK-ACCESS-KEY": self.api_key,
                "OK-ACCESS-SIGN": signature,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.api_passphrase,
                "Content-Type": "application/json"
            }
            async with self.session.post("https://www.okx.com/api/v5/trade/order", headers=headers, data=body) as resp:
                await resp.read()
        except Exception:
            pass

    def check_order_size(self, symbol: str, size_usd: float, price: float):
        pass

    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None):
        self.last_activity_ts = __import__("time").time()
        log(f"[OkxOrder] Виртуальный ордер {side} создан для {symbol}, объем {size_usd} USD", level="DEBUG")
        await asyncio.sleep(0.005)
        
    async def cancel_all_orders(self, symbol: str):
        self.last_activity_ts = __import__("time").time()
        pass
        
    async def set_margin_type(self, symbol: str, margin_type: str, **kwargs) -> bool:
        return True
        
    async def set_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        return True
        
    def get_executed_position(self, symbol: str, side: str):
        return {"size": 0.0, "price": 0.0}

    async def get_exact_position(self, symbol: str, side: str):
        return {"size": 0.0, "price": 0.0}

class BitgetOrder:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, margin_settings: dict, session=None, position_stream=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.margin_settings = margin_settings
        self.session = session
        self.position_stream = position_stream
        self.symbol_info = []

    def start(self):
        self._bg_task = asyncio.create_task(self.update_symbol_info())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        return self._bg_task

    
    async def _keepalive_loop(self):
        while True:
            try:
                import asyncio
                await asyncio.sleep(120)
                await self.warmup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                pass

    async def warmup(self):
        import time, json
        if time.time() - getattr(self, "last_activity_ts", 0.0) < 30.0:
            return
        try:
            timestamp = str(int(time.time() * 1000))
            body_dict = {
                "symbol": "INVALIDUSDT",
                "productType": "usdt-futures",
                "marginMode": "crossed",
                "marginCoin": "USDT",
                "size": "1",
                "price": "1",
                "side": "buy",
                "tradeSide": "open",
                "orderType": "limit",
                "force": "ioc"
            }
            body = json.dumps(body_dict)
            signature = self._generate_signature(timestamp, "POST", "/api/v2/mix/order/place-order", body)
            headers = {
                "ACCESS-KEY": self.api_key,
                "ACCESS-SIGN": signature,
                "ACCESS-TIMESTAMP": timestamp,
                "ACCESS-PASSPHRASE": self.api_passphrase,
                "Content-Type": "application/json"
            }
            async with self.session.post("https://api.bitget.com/api/v2/mix/order/place-order", headers=headers, data=body) as resp:
                await resp.read()
        except Exception:
            pass

    def _generate_signature(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(bytes(self.api_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        return base64.b64encode(mac.digest()).decode('utf-8')

    async def update_symbol_info(self):
        if not self.session:
            from utils import SessionManager
            self.session = await SessionManager().get_session()
        url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
        try:
            async with self.session.get(url) as resp:
                data = await resp.json()
                if data.get("code") == "00000":
                    self.symbol_info = data.get("data", [])
        except Exception as e:
            log(f"[BitgetOrder] Failed to update symbol info: {e}", level="ERROR")

    def check_order_size(self, symbol: str, size_usd: float, price: float):
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required to calculate quantity")
            
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Exchange specs not loaded yet")
            
        symbol_data = next((item for item in self.symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            raise ValueError(f"[{symbol}] Precision rules not found in specs")
            
        sizeMultiplier = float(symbol_data.get('sizeMultiplier', 1.0))
        volumePlace = int(symbol_data.get('volumePlace', 0))
        
        raw_qty = (size_usd / price) / sizeMultiplier
        qty_str = f"{raw_qty:.{volumePlace}f}"
        
        if float(qty_str) <= 0:
            raise ValueError(f"[{symbol}] Calculated order size is 0 after rounding (raw_qty={raw_qty}).")

    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None, time_in_force: str = "IOC"):
        self.last_activity_ts = __import__("time").time()
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required")
        if not self.symbol_info:
            await self.update_symbol_info()
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Specs not loaded")
            
        symbol_data = next((item for item in self.symbol_info if item.get('symbol') == symbol), None)
        if not symbol_data:
            raise ValueError(f"[{symbol}] Precision rules not found")
            
        sizeMultiplier = float(symbol_data.get('sizeMultiplier', 1.0))
        volumePlace = int(symbol_data.get('volumePlace', 0))
        pricePlace = int(symbol_data.get('pricePlace', 2))
        
        raw_qty = (size_usd / price) / sizeMultiplier
        qty_str = f"{raw_qty:.{volumePlace}f}"
        price_str = f"{price:.{pricePlace}f}"
        
        if float(qty_str) <= 0:
            raise ValueError(f"[{symbol}] Calculated order size is 0")
        
        endpoint = "/api/v2/mix/order/place-order"
        now = str(int(time.time() * 1000))
        
        is_close = False
        if position_side:
            p_side = position_side.upper()
            if p_side in ("REDUCE", "CLOSE"):
                is_close = True
            elif p_side == "LONG" and side.upper() == "SELL":
                is_close = True
            elif p_side == "SHORT" and side.upper() == "BUY":
                is_close = True

        # Bitget v2 API: place-order с tradeSide="close" возвращает 22002.
        # Для закрытия используем специальный эндпоинт close-positions.
        if is_close and position_side:
            hold = position_side.upper()
            if hold in ("LONG", "SHORT"):
                return await self._close_position(symbol, hold.lower())

        trade_side = "open"

        mm = self.margin_settings["margin_type"].lower()
        if mm == "cross":
            mm = "crossed"

        body = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginMode": mm,
            "marginCoin": "USDT",
            "size": qty_str,
            "side": side.lower(),
            "tradeSide": trade_side,
            "orderType": order_type.lower(),
            "clientOid": str(uuid.uuid4())
        }
        if order_type.upper() == "LIMIT":
            body["price"] = price_str
            body["force"] = (time_in_force or "IOC").lower()
            
        body_str = json.dumps(body)
        signature = self._generate_signature(now, "POST", endpoint, body_str)
        
        headers = {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': now,
            'ACCESS-PASSPHRASE': self.api_passphrase,
            'Content-Type': 'application/json'
        }
        
        url = f"https://api.bitget.com{endpoint}"
        async with self.session.post(url, headers=headers, data=body_str) as resp:
            data = await resp.json()
            if data.get('code') != '00000':
                msg = data.get('msg', str(data))
                code_str = str(data.get('code'))
                if "margin" in msg.lower() or "balance" in msg.lower() or code_str in ("40754", "43012", "40762"):
                    raise InsufficientMarginError(f"Bitget API Error: {data}")
                raise Exception(f"Bitget API Error: {data}")
            return data

    async def _close_position(self, symbol: str, hold_side: str):
        """Закрытие позиции через специальный эндпоинт Bitget close-positions."""
        endpoint = "/api/v2/mix/order/close-positions"
        now = str(int(time.time() * 1000))
        body = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginCoin": "USDT",
            "holdSide": hold_side
        }
        body_str = json.dumps(body)
        signature = self._generate_signature(now, "POST", endpoint, body_str)
        headers = {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': now,
            'ACCESS-PASSPHRASE': self.api_passphrase,
            'Content-Type': 'application/json'
        }
        url = f"https://api.bitget.com{endpoint}"
        async with self.session.post(url, headers=headers, data=body_str) as resp:
            data = await resp.json()
            if data.get('code') != '00000':
                msg = data.get('msg', str(data))
                raise Exception(f"Bitget API Error: {data}")
            log(f"[BitgetOrder] Позиция {symbol} ({hold_side}) закрыта: {data}", level="INFO")
            return data

    async def cancel_all_orders(self, symbol: str):
        self.last_activity_ts = __import__("time").time()
        if not self.api_key:
            return
        try:
            if not self.session:
                from utils import SessionManager
                self.session = await SessionManager().get_session()
                
            endpoint_get = f"/api/v2/mix/order/orders-pending?symbol={symbol}&productType=USDT-FUTURES"
            now = str(int(time.time() * 1000))
            sig = self._generate_signature(now, "GET", endpoint_get)
            headers = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-SIGN': sig,
                'ACCESS-TIMESTAMP': now,
                'ACCESS-PASSPHRASE': self.api_passphrase
            }
            async with self.session.get(f"https://api.bitget.com{endpoint_get}", headers=headers) as resp:
                data = await resp.json()
                orders = data.get('data', {}).get('entrustedList', []) or []
                
            if not orders:
                return
                
            order_ids = [str(o['orderId']) for o in orders if 'orderId' in o]
            if not order_ids:
                return
                
            if len(order_ids) == 1:
                endpoint_cancel = "/api/v2/mix/order/cancel-order"
                body = {
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "marginCoin": "USDT",
                    "orderId": order_ids[0]
                }
            else:
                endpoint_cancel = "/api/v2/mix/order/cancel-batch-orders"
                body = {
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "marginCoin": "USDT",
                    "orderIdList": order_ids
                }
                
            now_c = str(int(time.time() * 1000))
            body_str = json.dumps(body)
            sig_c = self._generate_signature(now_c, "POST", endpoint_cancel, body_str)
            headers_c = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-SIGN': sig_c,
                'ACCESS-TIMESTAMP': now_c,
                'ACCESS-PASSPHRASE': self.api_passphrase,
                'Content-Type': 'application/json'
            }
            async with self.session.post(f"https://api.bitget.com{endpoint_cancel}", headers=headers_c, data=body_str) as resp_c:
                res_c = await resp_c.json()
                if res_c.get('code') == '00000':
                    log(f"[BitgetOrder] Отменены ордера по {symbol}: {order_ids}", level="INFO")
                else:
                    log(f"[BitgetOrder] Ошибка отмены ордеров {symbol}: {res_c}", level="WARNING")
        except Exception as e:
            log(f"[BitgetOrder] Ошибка отмены ордеров {symbol}: {e}", level="ERROR")

    async def set_margin_type(self, symbol: str, margin_type: str, leverage: int = None) -> bool:
        if not self.api_key:
            return False
        for attempt in range(3):
            try:
                if not self.session:
                    from utils import SessionManager
                    self.session = await SessionManager().get_session()
                    
                mm = margin_type.lower()
                if mm == "cross":
                    mm = "crossed"
                    
                endpoint = "/api/v2/mix/account/set-margin-mode"
                body = {
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "marginCoin": "USDT",
                    "marginMode": mm
                }
                body_str = json.dumps(body)
                now = str(int(time.time() * 1000))
                sig = self._generate_signature(now, "POST", endpoint, body_str)
                headers = {
                    'ACCESS-KEY': self.api_key,
                    'ACCESS-SIGN': sig,
                    'ACCESS-TIMESTAMP': now,
                    'ACCESS-PASSPHRASE': self.api_passphrase,
                    'Content-Type': 'application/json'
                }
                url = f"https://api.bitget.com{endpoint}"
                async with self.session.post(url, headers=headers, data=body_str) as resp:
                    data = await resp.json()
                    if data.get('code') == '00000':
                        return True
                    if str(data.get('code')) == '429':
                        await asyncio.sleep(1.0 + attempt * 0.5)
                        continue
                    msg = str(data.get('msg', '')).lower()
                    if "no need to change" in msg or "not changed" in msg or "same" in msg:
                        return True
                    log(f"[BitgetOrder] Ошибка set_margin_type {symbol}: {data}", level="WARNING")
                    return False
            except Exception as e:
                log(f"[BitgetOrder] Исключение set_margin_type {symbol}: {e}", level="ERROR")
                return False
        return False

    async def set_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        if not self.api_key:
            return False
        for attempt in range(3):
            try:
                if not self.session:
                    from utils import SessionManager
                    self.session = await SessionManager().get_session()
                    
                endpoint = "/api/v2/mix/account/set-leverage"
                body = {
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "marginCoin": "USDT",
                    "leverage": str(leverage)
                }
                body_str = json.dumps(body)
                now = str(int(time.time() * 1000))
                sig = self._generate_signature(now, "POST", endpoint, body_str)
                headers = {
                    'ACCESS-KEY': self.api_key,
                    'ACCESS-SIGN': sig,
                    'ACCESS-TIMESTAMP': now,
                    'ACCESS-PASSPHRASE': self.api_passphrase,
                    'Content-Type': 'application/json'
                }
                url = f"https://api.bitget.com{endpoint}"
                async with self.session.post(url, headers=headers, data=body_str) as resp:
                    data = await resp.json()
                    if data.get('code') == '00000':
                        return True
                    if str(data.get('code')) == '429':
                        await asyncio.sleep(1.0 + attempt * 0.5)
                        continue
                    msg = str(data.get('msg', '')).lower()
                    if "no need to change" in msg or "not changed" in msg or "same" in msg:
                        return True
                    log(f"[BitgetOrder] Ошибка set_leverage {symbol}: {data}", level="WARNING")
                    return False
            except Exception as e:
                log(f"[BitgetOrder] Исключение set_leverage {symbol}: {e}", level="ERROR")
                return False
        return False

    def get_executed_position(self, symbol: str, side: str):
        if self.position_stream:
            return self.position_stream.get_position(symbol, side)
        return {"size": 0.0, "price": 0.0}

    async def get_position_rest(self, symbol: str, side: str = None) -> dict:
        if not self.api_key:
            return {"size": 0.0, "price": 0.0, "status": "ok"}
        try:
            if not self.session:
                from utils import SessionManager
                self.session = await SessionManager().get_session()
            now = str(int(time.time() * 1000))
            endpoint = f"/api/v2/mix/position/single-position?productType=USDT-FUTURES&symbol={symbol}&marginCoin=USDT"
            signature = self._generate_signature(now, "GET", endpoint)
            headers = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-SIGN': signature,
                'ACCESS-TIMESTAMP': now,
                'ACCESS-PASSPHRASE': self.api_passphrase,
                'Content-Type': 'application/json'
            }
            url = f"https://api.bitget.com{endpoint}"
            async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == "00000" and data.get("data"):
                        for p in data["data"]:
                            pos_side = (p.get("holdSide") or p.get("posSide") or "").upper()
                            if side and pos_side != side.upper():
                                continue
                            amt = abs(float(p.get("total", 0.0)))
                            price = float(p.get("openPriceAvg") or p.get("averageOpenPrice") or p.get("breakEvenPrice") or 0.0)
                            if amt > 0:
                                if self.position_stream:
                                    if symbol not in self.position_stream.positions:
                                        self.position_stream.positions[symbol] = {
                                            "LONG": {"size": 0.0, "price": 0.0},
                                            "SHORT": {"size": 0.0, "price": 0.0}
                                        }
                                    self.position_stream.positions[symbol][pos_side] = {"size": amt, "price": price}
                                return {"size": amt, "price": price, "status": "ok"}
                    return {"size": 0.0, "price": 0.0, "status": "ok"}
                else:
                    log(f"[BitgetOrder] get_position_rest HTTP {resp.status}", level="WARNING")
                    return {"size": 0.0, "price": 0.0, "status": "error"}
        except Exception as e:
            log(f"[BitgetOrder] get_position_rest network error: {e}", level="WARNING")
            return {"size": 0.0, "price": 0.0, "status": "error"}

    async def get_exact_position(self, symbol: str, side: str) -> dict:
        return await self.get_exact_position_guarded(symbol, side)

    async def get_exact_position_guarded(self, symbol: str, side: str, max_retries: int = 3, retry_delay: float = 0.015) -> dict:
        self.last_activity_ts = __import__("time").time()
        for attempt in range(max_retries):
            pos = await self.get_position_rest(symbol, side)
            if pos.get("status") == "ok":
                if pos.get("size", 0.0) > 0:
                    return pos
                if self.position_stream and symbol in self.position_stream.positions:
                    self.position_stream.positions[symbol][side.upper()] = {"size": 0.0, "price": 0.0}
                return {"size": 0.0, "price": 0.0, "status": "ok"}
            await asyncio.sleep(retry_delay * (attempt + 1))

        ws_pos = self.get_executed_position(symbol, side)
        log(f"[BitgetOrder] REST моргнул после {max_retries} ретраев, страховка WS: {ws_pos}", level="WARNING")
        return {"size": ws_pos.get("size", 0.0), "price": ws_pos.get("price", 0.0), "status": "fallback_ws"}


