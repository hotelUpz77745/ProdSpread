# ============================================================
# FILE: CORE/ipc_socket.py
# ROLE: Utilities for async inter-process communication via TCP.
# ============================================================

import asyncio
import pickle
import struct

async def async_write_msg(writer: asyncio.StreamWriter, msg_type: str, payload: dict):
    """Serializes and sends message with 4-byte length prefix."""
    try:
        data = pickle.dumps((msg_type, payload))
        header = struct.pack("!I", len(data))
        writer.write(header + data)
        await writer.drain()
    except Exception as e:
        import traceback
        from c_log import log
        log(f"[IPC] Error sending message {msg_type}: {e}", level="ERROR")


async def async_read_msg(reader: asyncio.StreamReader):
    """Reads 4-byte header, then payload and deserializes it."""
    try:
        header = await reader.readexactly(4)
        length = struct.unpack("!I", header)[0]
        data = await reader.readexactly(length)
        msg_type, payload = pickle.loads(data)
        return msg_type, payload
    except asyncio.IncompleteReadError:
        raise EOFError("Socket closed unexpectedly.")
    except Exception as e:
        raise EOFError(f"Socket error: {e}")
