import asyncio
from typing import List

from fastapi import WebSocket


class ConnectionManager:
    """Manages websocket connections to the UI."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._loop = None

    def set_loop(self, loop):
        self._loop = loop

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    def broadcast(self, message: str):
        if not self._loop:
            return
        # This can be called from a non-async thread, so we use run_coroutine_threadsafe
        asyncio.run_coroutine_threadsafe(self._broadcast_async(message), self._loop)

    async def _broadcast_async(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)
