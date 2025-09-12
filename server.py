#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
T9 Live Streaming Server - FastAPI 後端服務
提供 SSE 即時推送、數據攝取和監控 API
"""

import asyncio
import json
import time
import logging
import os
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# 導入核心模塊
from core.event_system import get_global_publisher, EventType
from core.metrics import get_global_metrics, get_global_watchdog
from streaming.sse_server import get_global_sse_server

# 配置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("t9-server")

# 創建 FastAPI 應用
app = FastAPI(
    title="T9 Live Streaming API",
    description="T9 Live 即時數據流推送服務",
    version="2.0.0"
)

# 添加 CORS 中間件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局變量
latest_records = []
metrics = get_global_metrics()
watchdog = get_global_watchdog()
event_publisher = get_global_publisher()
sse_server = get_global_sse_server()

# 數據模型
class IngestRequest(BaseModel):
    records: List[Dict[str, Any]]

class IngestResponse(BaseModel):
    success: bool
    message: str
    processed_count: int

# 根路由
@app.get("/")
async def root():
    """根路由 - 健康檢查"""
    return {
        "service": "T9 Live Streaming API",
        "version": "2.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

# 健康檢查
@app.get("/health")
async def health_check():
    """健康檢查端點"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "metrics": metrics.get_comprehensive_stats()
    }

# SSE 流式推送端點
@app.get("/api/stream")
async def stream_events(
    request: Request,
    event_types: str = Query("result,error,heartbeat", description="事件類型過濾"),
    job_id: Optional[str] = Query(None, description="任務ID過濾")
):
    """SSE 事件流端點"""
    # 解析事件類型
    parsed_event_types = event_types.split(',') if event_types else None
    
    # 使用 SSEServer 的 create_event_stream 方法
    return await sse_server.create_event_stream(
        request=request,
        event_types=parsed_event_types,
        job_id=job_id
    )

# WebSocket 端點
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 端點"""
    await websocket.accept()
    try:
        while True:
            # 發送心跳
            await websocket.send_text(json.dumps({
                "type": "heartbeat",
                "timestamp": datetime.now().isoformat()
            }))
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")

# 數據攝取端點
@app.post("/ingest", response_model=IngestResponse)
async def ingest_data(request: IngestRequest, x_ingest_key: Optional[str] = Header(None, alias="x-ingest-key")):
    """數據攝取端點"""
    # 驗證 API 密鑰 - 先驗證，讓 401 直接拋出
    expected_key = os.getenv("INGEST_KEY", "baccaratt9webapi")
    logger.info(f"Received x_ingest_key: '{x_ingest_key}', expected: '{expected_key}'")
    if x_ingest_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid ingest key")
    
    try:
        # 處理記錄
        processed_count = 0
        for record in request.records:
            # 添加時間戳
            record["ingested_at"] = datetime.now().isoformat()
            
            # 更新最新記錄
            global latest_records
            latest_records.append(record)
            if len(latest_records) > 1000:  # 限制緩存大小
                latest_records = latest_records[-1000:]
            
            # 發送事件
            await sse_server.publish_result(
                data={"record": record, "source": "ingest"},
                jwt_version="unknown",
                latency_ms=0
            )
            
            processed_count += 1
        
        # 更新指標
        metrics.increment_counter("records_ingested", processed_count)
        
        return IngestResponse(
            success=True,
            message=f"Successfully processed {processed_count} records",
            processed_count=processed_count
        )
        
    except HTTPException:
        # 重新拋出 HTTP 異常
        raise
    except Exception as e:
        logger.error(f"Ingest error: {e}")
        metrics.record_error("ingest_error", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# 獲取最新記錄
@app.get("/api/latest")
async def get_latest_records(limit: int = Query(50, ge=1, le=1000)):
    """獲取最新記錄"""
    return {
        "records": latest_records[-limit:],
        "total": len(latest_records),
        "timestamp": datetime.now().isoformat()
    }

# 獲取系統指標
@app.get("/api/metrics")
async def get_metrics():
    """獲取系統指標"""
    return {
        "metrics": metrics.get_comprehensive_stats(),
        "watchdog": watchdog.get_stats(),
        "timestamp": datetime.now().isoformat()
    }

# 獲取事件統計
@app.get("/api/events/stats")
async def get_event_stats():
    """獲取事件統計"""
    return {
        "event_stats": sse_server.get_stats(),
        "timestamp": datetime.now().isoformat()
    }

# 啟動事件
@app.on_event("startup")
async def startup_event():
    """應用啟動事件"""
    logger.info("T9 Live Streaming Server starting up...")
    
    # SSE 服務器已經通過 get_global_sse_server() 初始化
    logger.info("SSE server initialized")
    
    logger.info("Server startup completed")

# 關閉事件
@app.on_event("shutdown")
async def shutdown_event():
    """應用關閉事件"""
    logger.info("T9 Live Streaming Server shutting down...")
    
    # 關閉 SSE 服務器
    await sse_server.shutdown()
    
    logger.info("Server shutdown completed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)