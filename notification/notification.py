from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
import uvicorn
import websockets
import asyncio
import json
import logging
import requests

app = FastAPI()

# تنظیمات لاگینگ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("NotificationService")

# آدرس سرویس Aggregator
AGGREGATOR_URL = "http://aggregator-service:8001/aggregate"

class NotificationData(BaseModel):
    stock: str
    action: str
    reason: str
    timestamp: float

connected_clients = set()

async def websocket_handler(websocket, path):
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            pass
    finally:
        connected_clients.remove(websocket)

@app.post("/notify")
async def notify(data: NotificationData):
    try:
        message = json.dumps(data.dict())
        for client in connected_clients:
            await client.send(message)
        logger.info(f"WebSocket notification sent: {data.stock}")

        # ارسال به Aggregator
        aggregator_response = requests.post(
            AGGREGATOR_URL,
            json=data.dict(),
            timeout=3
        )
        if aggregator_response.status_code != 200:
            logger.error(f"Aggregator failed: {aggregator_response.text}")

        return {"status": "success"}

    except Exception as e:
        logger.error(f"Notification failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":

    async def start_websocket():
        server = websockets.serve(websocket_handler, "0.0.0.0", 8765)
        await server

    def run_websocket():
        asyncio.run(start_websocket())

    import threading
    threading.Thread(target=run_websocket, daemon=True).start()

    # راه‌اندازی FastAPI
    uvicorn.run(app, host="0.0.0.0", port=8000)
