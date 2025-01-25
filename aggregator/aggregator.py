from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Dict, List
import uvicorn
import logging
from collections import defaultdict

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AggregatorService")

class AggregatorData(BaseModel):
    stock: str
    action: str
    reason: str
    timestamp: float

# ذخیره‌سازی داده‌ها با ساختار پیشرفته
aggregated_data = defaultdict(lambda: {
    "buy_count": 0,
    "sell_count": 0,
    "history": []
})

@app.post("/aggregate")
async def aggregate(data: AggregatorData):
    try:
        # به‌روزرسانی آمار
        if data.action == "BUY":
            aggregated_data[data.stock]["buy_count"] += 1
        elif data.action == "SELL":
            aggregated_data[data.stock]["sell_count"] += 1
        
        # ذخیره تاریخچه
        aggregated_data[data.stock]["history"].append({
            "action": data.action,
            "reason": data.reason,
            "timestamp": data.timestamp
        })
        
        logger.info(f"Aggregated data for {data.stock}")
        return {"status": "success"}
    
    except Exception as e:
        logger.error(f"Aggregation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats(hours: int = 1):
    try:
        cutoff = datetime.now() - timedelta(hours=hours)
        result = {}
        
        for stock, data in aggregated_data.items():
            # فیلتر داده‌های تاریخچه
            filtered_history = [
                entry for entry in data["history"]
                if datetime.fromtimestamp(entry["timestamp"]) >= cutoff
            ]
            
            result[stock] = {
                "total_signals": len(filtered_history),
                "buy_signals": sum(1 for e in filtered_history if e["action"] == "BUY"),
                "sell_signals": sum(1 for e in filtered_history if e["action"] == "SELL"),
                "latest_reason": filtered_history[-1]["reason"] if filtered_history else None
            }
        
        return result
    
    except Exception as e:
        logger.error(f"Stats retrieval failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
