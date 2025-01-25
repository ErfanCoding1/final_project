from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
import uvicorn
import requests
import json
import websockets
import asyncio

app = FastAPI()

# مدل داده‌های دریافتی از اسپارک
class IndicatorData(BaseModel):
    stock_symbol: str
    opening_price: float
    closing_price: float
    high: float
    low: float
    volume: int
    timestamp: float
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    order_type: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    sentiment_score: Optional[float] = None
    sentiment_magnitude: Optional[float] = None
    indicator_name: Optional[str] = None
    value: Optional[float] = None
    moving_avg: float
    ema: float
    rsi: float

# ذخیره‌سازی سیگنال‌های اخیر برای وب‌ساکت
live_signals: Dict[str, dict] = {}

@app.post("/process_indicators")
async def process_indicators(data: IndicatorData):
    # منطق تولید سیگنال
    signal = {
        "stock": data.stock_symbol,
        "action": "HOLD",
        "reason": "No signal",
        "timestamp": data.timestamp
    }

    # تحلیل RSI
    if data.rsi > 70:
        signal.update({"action": "SELL", "reason": "Overbought (RSI > 70)"})
    elif data.rsi < 30:
        signal.update({"action": "BUY", "reason": "Oversold (RSI < 30)"})

    # تحلیل EMA و Moving Average
    elif data.closing_price > data.ema * 1.05:
        signal.update({"action": "SELL", "reason": "Price 5% above EMA"})
    elif data.closing_price < data.ema * 0.95:
        signal.update({"action": "BUY", "reason": "Price 5% below EMA"})

    # تحلیل احساسات (اگر موجود باشد)
    if data.sentiment_score is not None:
        if data.sentiment_score < -0.5:
            signal.update({"action": "SELL", "reason": "Negative Sentiment"})
        elif data.sentiment_score > 0.5:
            signal.update({"action": "BUY", "reason": "Positive Sentiment"})

    # ذخیره سیگنال
    live_signals[data.stock_symbol] = signal

    # ارسال به Notification Service (اختیاری)
    try:
        response = requests.post(
            "http://notification-service:8000/notify",
            json=signal
        )
    except Exception as e:
        print(f"Failed to send notification: {str(e)}")

    return {"status": "signal processed"}

# وب‌ساکت برای نمایش بلادرنگ
async def websocket_handler(websocket, path):
    while True:
        await websocket.send(json.dumps(live_signals))
        await asyncio.sleep(1)

if __name__ == "__main__":
    import threading

    # راه‌اندازی وب‌ساکت
    async def start_websocket():
        start_server = websockets.serve(websocket_handler, "0.0.0.0", 8765)
        await start_server  # WebSocket server must run in an asyncio event loop

    # اجرای وب‌ساکت در یک thread جداگانه
    def run_websocket():
        asyncio.run(start_websocket())

    websocket_thread = threading.Thread(target=run_websocket, daemon=True)
    websocket_thread.start()

    # راه‌اندازی FastAPI
    uvicorn.run(app, host="0.0.0.0", port=5000)
