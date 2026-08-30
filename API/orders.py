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
                        self.symbol_info = await resp.json()
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
        if not symbol_info or not isinstance(symbol_info, dict) or "symbols" not in symbol_info:
            return None
            
        symbol_data = next((item for item in symbol_info.get("symbols", []) if item.get('symbol') == symbol), None)
        if not symbol_data:
            return None

        lot_size_filter = next((f for f in symbol_data.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None)
        price_filter = next((f for f in symbol_data.get("filters", []) if f.get("filterType") == "PRICE_FILTER"), None)

        if not lot_size_filter or not price_filter:
            return None

        def count_decimal_places(number):
            number_str = f"{float(number):.10f}".rstrip('0')
            if '.' in number_str:
                return len(number_str.split('.')[-1])
            return 0

        qty_precission = count_decimal_places(lot_size_filter['stepSize'])
        price_precision = count_decimal_places(price_filter['tickSize'])

        return qty_precission, price_precision

    def _generate_signature(self, query_string: str) -> str:
        return hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None):
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required to calculate quantity")
            
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Exchange specs not loaded yet")
            
        precisions = self.get_spec_precisions(self.symbol_info, symbol)
        if not precisions:
            raise ValueError(f"[{symbol}] Precision rules not found in specs")
            
        qty_prec, price_prec = precisions
        raw_qty = size_usd / price
        quantity = round(raw_qty, qty_prec) if qty_prec > 0 else int(raw_qty)
        price = round(price, price_prec) if price_prec > 0 else int(price)
            
        timestamp = int(time.time() * 1000)
        
        pos_side_str = f"&positionSide={position_side.upper()}" if position_side else ""
        
        if order_type.upper() == "LIMIT":
            query_string = f"symbol={symbol}&side={side.upper()}{pos_side_str}&type=LIMIT&timeInForce=IOC&quantity={quantity}&price={price}&timestamp={timestamp}"
        else:
            query_string = f"symbol={symbol}&side={side.upper()}{pos_side_str}&type=MARKET&quantity={quantity}&timestamp={timestamp}"
            
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
                if data.get("code") == -4046: # No need to change
                    return True
                raise Exception(f"{data}")
        except Exception as e:
            raise Exception(f"MarginType Error: {e}")

    async def set_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
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
                raise Exception(f"{data}")
        except Exception as e:
            raise Exception(f"Leverage Error: {e}")

    async def warmup(self):
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
            number_str = f"{float(number):.10f}".rstrip('0')
            if '.' in number_str:
                return len(number_str.split('.')[-1])
            return 0
            
        qty_precision = count_decimal_places(symbol_data.get('lotSize', 1))
        price_precision = count_decimal_places(symbol_data.get('tickSize', 0.1))
        multiplier = float(symbol_data.get('multiplier', 1.0))
        
        return qty_precision, price_precision, multiplier

    def _generate_signature(self, str_to_sign: str) -> str:
        h = hmac.new(self.api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(h.digest()).decode('utf-8')

    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None):
        if not price or price <= 0:
            raise ValueError(f"[{symbol}] Reference price is required to calculate quantity")
            
        if not self.symbol_info:
            raise ValueError(f"[{symbol}] Exchange specs not loaded yet")
            
        precisions = self.get_spec_precisions(self.symbol_info, symbol)
        if not precisions:
            raise ValueError(f"[{symbol}] Precision rules not found in specs")
            
        qty_prec, price_prec, multiplier = precisions
        raw_lots = (size_usd / price) / multiplier
        quantity = round(raw_lots, qty_prec) if qty_prec > 0 else int(raw_lots)
        quantity = max(1, quantity)
        price = round(price, price_prec) if price_prec > 0 else int(price)
        
        endpoint = "/api/v1/orders"
        now = str(int(time.time() * 1000))
        
        leverage = str(self.margin_settings["leverage"])
        marginMode = str(self.margin_settings["margin_type"]).upper()

        body = {
            "clientOid": str(uuid.uuid4()),
            "symbol": symbol,
            "side": side.lower(),
            "size": str(quantity),
            "leverage": leverage,
            "marginMode": marginMode
        }
        
        if order_type.upper() == "LIMIT":
            body["type"] = "limit"
            body["timeInForce"] = "IOC"
            body["price"] = str(price)
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
        endpoint = "/api/v1/position"
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
                raise Exception(f"{data}")
        except Exception as e:
            raise Exception(f"Kucoin MarginType Error: {e}")
        
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
                raise Exception(f"{data}")
        except Exception as e:
            raise Exception(f"Kucoin Leverage Error: {e}")

    def get_executed_position(self, symbol: str, side: str):
        if self.position_stream:
            return self.position_stream.get_position(symbol, side)
        return {"size": 0.0, "price": 0.0}

class OkxOrder:
    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None):
        log(f"[OkxOrder] Виртуальный ордер {side} создан для {symbol}, объем {size_usd} USD", level="DEBUG")
        await asyncio.sleep(0.005)
        
    async def cancel_all_orders(self, symbol: str):
        pass
        
    async def set_margin_type(self, symbol: str, margin_type: str, **kwargs) -> bool:
        return True
        
    async def set_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        return True
        
    def get_executed_position(self, symbol: str, side: str):
        return {"size": 0.0, "price": 0.0}

class BitgetOrder:
    async def place_order(self, symbol: str, side: str, size_usd: float, price: float, order_type: str = "LIMIT", position_side: str = None):
        log(f"[BitgetOrder] Виртуальный ордер {side} создан для {symbol}, объем {size_usd} USD", level="DEBUG")
        await asyncio.sleep(0.005)
        
    async def cancel_all_orders(self, symbol: str):
        pass
        
    async def set_margin_type(self, symbol: str, margin_type: str, **kwargs) -> bool:
        return True
        
    async def set_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        return True
        
    def get_executed_position(self, symbol: str, side: str):
        return {"size": 0.0, "price": 0.0}


