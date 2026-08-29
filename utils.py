# ============================================================
# FILE: utils.py
# ROLE: SessionManager and generic utils
# ============================================================
import aiohttp
from c_log import UnifiedLogger

class SessionManager:
    """Singleton for managing a shared aiohttp.ClientSession."""
    _instance = None
    _session = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SessionManager, cls).__new__(cls)
        return cls._instance

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close_all(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            
    async def close(self):
        await self.close_all()
