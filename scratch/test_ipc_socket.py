import unittest
import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.ipc_socket import async_write_msg, async_read_msg

class TestIPCSocket(unittest.IsolatedAsyncioTestCase):
    
    async def test_async_write_msg(self):
        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        
        payload = {"route": "BINANCE_BITGET", "sym": "BTCUSDT"}
        
        await async_write_msg(writer, "CMD_OPEN", payload)
        
        # Check what was written
        import struct
        import pickle
        expected_data = pickle.dumps(("CMD_OPEN", payload))
        expected_header = struct.pack("!I", len(expected_data))
        
        writer.write.assert_called_once_with(expected_header + expected_data)
        writer.drain.assert_called_once()
        
    async def test_async_read_msg(self):
        reader = AsyncMock()
        
        import struct
        import pickle
        msg = ("POS_OPENED", {"sym": "BTCUSDT"})
        data = pickle.dumps(msg)
        header = struct.pack("!I", len(data))
        
        # We need to mock readexactly to return header then data
        reader.readexactly.side_effect = [header, data]
        
        msg_type, payload = await async_read_msg(reader)
            
        self.assertEqual(msg_type, "POS_OPENED")
        self.assertEqual(payload["sym"], "BTCUSDT")

if __name__ == '__main__':
    unittest.main()
