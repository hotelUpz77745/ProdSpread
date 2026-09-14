# ============================================================
# FILE: live_tests/test_orders.py
# ROLE: Comprehensive unit and live tests for orders adapters.
# ============================================================
import unittest
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from API.orders import BinanceOrder, BitgetOrder, KucoinOrder, round_by_step

class TestOrders(unittest.IsolatedAsyncioTestCase):
    
    def test_round_by_step(self):
        self.assertEqual(round_by_step(123.4567, 0.01), "123.46")
        self.assertEqual(round_by_step(123.4567, 0.1), "123.5")
        self.assertEqual(round_by_step(123.000, 1.0), "123")
        self.assertEqual(round_by_step(0.00012345, 0.0001), "0.0001")
        self.assertEqual(round_by_step(10.5, 1.0), "11")

    @patch('API.orders.aiohttp.ClientSession')
    async def test_bitget_cancel_batch_orders_margin_coin(self, mock_session_class):
        mock_session = MagicMock()
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"code": "00000", "msg": "success"})
        
        # properly mock async with
        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__.return_value = mock_response
        mock_session.post.return_value = mock_post_ctx
        
        order_api = BitgetOrder("dummy", "dummy", "dummy", session=mock_session, position_stream=None, margin_settings={})
        
        mock_response_get = MagicMock()
        mock_response_get.status = 200
        mock_response_get.json = AsyncMock(return_value={
            "code": "00000",
            "data": {
                "entrustedList": [
                    {"orderId": "111"},
                    {"orderId": "222"}
                ]
            }
        })
        
        mock_get_ctx = AsyncMock()
        mock_get_ctx.__aenter__.return_value = mock_response_get
        mock_session.get.return_value = mock_get_ctx
        
        await order_api.cancel_all_orders("BTCUSDT")
        
        post_calls = mock_session.post.call_args_list
        self.assertGreater(len(post_calls), 0)
        
        cancel_call = post_calls[0]
        kwargs = cancel_call[1]
        data_str = kwargs.get('data')
        self.assertIsNotNone(data_str)
        
        payload = json.loads(data_str)
        self.assertEqual(payload["symbol"], "BTCUSDT")
        self.assertEqual(payload["productType"], "USDT-FUTURES")
        self.assertEqual(payload["marginCoin"], "USDT")
        self.assertEqual(payload["orderIdList"], ["111", "222"])

    @patch('API.orders.aiohttp.ClientSession')
    async def test_kucoin_place_order_market_no_price(self, mock_session_class):
        mock_session = MagicMock()
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"code": "200000", "msg": "success"})
        
        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__.return_value = mock_response
        mock_session.post.return_value = mock_post_ctx
        
        order_api = KucoinOrder("dummy", "dummy", "dummy", session=mock_session, position_stream=None, margin_settings={"leverage": 10, "margin_type": "ISOLATED"})
        order_api.symbol_info = [{"symbol": "BTCUSDTM", "lotSize": 1.0, "tickSize": 0.1, "multiplier": 0.001}]
        
        await order_api.place_order("BTCUSDTM", "SELL", 1000.0, 50000.0, order_type="MARKET", position_side="LONG")
        
        post_calls = mock_session.post.call_args_list
        self.assertGreater(len(post_calls), 0)
        
        kwargs = post_calls[0][1]
        data_str = kwargs.get('data')
        payload = json.loads(data_str)
        
        self.assertEqual(payload["type"], "market")
        self.assertNotIn("price", payload)
        self.assertEqual(payload["positionSide"], "LONG")

    @patch('API.orders.aiohttp.ClientSession')
    async def test_order_price_rounding(self, mock_session_class):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"code": "00000", "msg": "success", "data": {}})
        mock_post_ctx = AsyncMock()
        mock_post_ctx.__aenter__.return_value = mock_response
        mock_session.post.return_value = mock_post_ctx
        
        # 1. BinanceOrder
        binance = BinanceOrder("k", "s", session=mock_session)
        binance.symbol_info = [{
            "symbol": "BTCUSDT",
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                {"filterType": "PRICE_FILTER", "tickSize": "0.1"}
            ]
        }]
        
        # BUY at 50000.01 with tick 0.1 -> ceil -> 50000.1
        await binance.place_order("BTCUSDT", "BUY", 100.0, 50000.01, order_type="LIMIT", position_side="LONG")
        query = mock_session.post.call_args[0][0]
        self.assertIn("price=50000.1", query)
        
        # SELL at 50000.09 with tick 0.1 -> floor -> 50000.0
        await binance.place_order("BTCUSDT", "SELL", 100.0, 50000.09, order_type="LIMIT", position_side="SHORT")
        query = mock_session.post.call_args[0][0]
        self.assertIn("price=50000.0", query)

        # 2. BitgetOrder
        bitget = BitgetOrder("k", "s", "p", session=mock_session, margin_settings={"margin_type": "crossed", "leverage": 20})
        bitget.symbol_info = [{
            "symbol": "BTCUSDT",
            "volumePlace": 3,
            "pricePlace": 1
        }]
        
        # BUY at 50000.01 with pricePlace 1 (step 0.1) -> ceil -> 50000.1
        await bitget.place_order("BTCUSDT", "BUY", 100.0, 50000.01, order_type="LIMIT", position_side="LONG")
        body = json.loads(mock_session.post.call_args[1]["data"])
        self.assertEqual(body["price"], "50000.1")
        
        # SELL at 50000.09 with pricePlace 1 (step 0.1) -> floor -> 50000.0
        await bitget.place_order("BTCUSDT", "SELL", 100.0, 50000.09, order_type="LIMIT", position_side="SHORT")
        body = json.loads(mock_session.post.call_args[1]["data"])
        self.assertEqual(body["price"], "50000.0")

        # 3. KucoinOrder
        mock_response.json = AsyncMock(return_value={"code": "200000", "msg": "success", "data": {}})
        kucoin = KucoinOrder("k", "s", "p", mock_session, None, margin_settings={"leverage": 10, "margin_type": "ISOLATED"})
        kucoin.symbol_info = [{
            "symbol": "BTCUSDTM",
            "lotSize": 1.0,
            "tickSize": 0.1,
            "multiplier": 0.001
        }]
        
        # BUY at 50000.01 with tick 0.1 -> ceil -> 50000.1
        await kucoin.place_order("BTCUSDTM", "BUY", 100.0, 50000.01, order_type="LIMIT", position_side="LONG")
        body = json.loads(mock_session.post.call_args[1]["data"])
        self.assertEqual(body["price"], "50000.1")
        
        # SELL at 50000.09 with tick 0.1 -> floor -> 50000.0
        await kucoin.place_order("BTCUSDTM", "SELL", 100.0, 50000.09, order_type="LIMIT", position_side="SHORT")
        body = json.loads(mock_session.post.call_args[1]["data"])
        self.assertEqual(body["price"], "50000.0")

if __name__ == '__main__':
    unittest.main()
