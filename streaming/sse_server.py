#!/usr/bin/env python3
"""
Server-Sent Events (SSE) 服務器 - 實現超快速即時推送
"""

import asyncio
import time
from typing import Optional, Dict, Any
from fastapi import Request, HTTPException
from fastapi.responses import StreamingResponse
import logging

from core.event_system import EventPublisher, EventType, EventFilter, get_global_publisher
from core.utils import format_duration


class SSEConnection:
    """SSE 連接管理器"""
    
    def __init__(self, request: Request, publisher: EventPublisher):
        self.request = request
        self.publisher = publisher
        self.logger = logging.getLogger(f"{__name__}.SSEConnection")
        self.connected_at = time.time()
        self.last_event_id: Optional[str] = None
        self.event_count = 0
        
        # 從請求頭獲取 Last-Event-ID
        self.last_event_id = request.headers.get("Last-Event-ID")
        
    async def stream_events(self, event_filter: Optional[EventFilter] = None) -> str:
        """
        流式發送事件
        
        Args:
            event_filter: 事件過濾器
            
        Yields:
            str: SSE 格式的事件數據
        """
        try:
            # 首先發送歷史事件（如果有 Last-Event-ID）
            if self.last_event_id:
                historical_events = self.publisher.get_event_history(self.last_event_id, limit=50)
                for event in historical_events:
                    if not event_filter or event_filter.should_include(event):
                        yield event.to_sse_format()
                        self.event_count += 1
            
            # 發送連接成功事件
            yield f"id: connection-{int(time.time())}\nevent: connected\ndata: {{\"message\": \"SSE connection established\", \"timestamp\": {time.time()}}}\n\n"
            
            # 訂閱新事件
            async for event in self.publisher.subscribe():
                # 檢查客戶端是否斷開連接
                if await self.request.is_disconnected():
                    self.logger.info("Client disconnected")
                    break
                
                # 應用過濾器
                if event_filter and not event_filter.should_include(event):
                    continue
                
                # 發送事件
                sse_data = event.to_sse_format()
                yield sse_data
                self.event_count += 1
                
                # 定期發送心跳（每30秒）
                if self.event_count % 100 == 0:  # 每100個事件檢查一次
                    current_time = time.time()
                    if (current_time - self.connected_at) % 30 < 1:  # 大約每30秒
                        heartbeat_data = f"id: heartbeat-{int(current_time)}\nevent: heartbeat\ndata: {{\"timestamp\": {current_time}, \"events_sent\": {self.event_count}}}\n\n"
                        yield heartbeat_data
        
        except asyncio.CancelledError:
            self.logger.info("SSE stream cancelled")
        except Exception as e:
            self.logger.error(f"SSE stream error: {e}")
            error_data = f"event: error\ndata: {{\"error\": \"{str(e)}\", \"timestamp\": {time.time()}}}\n\n"
            yield error_data
        finally:
            duration = time.time() - self.connected_at
            self.logger.info(f"SSE connection closed after {format_duration(duration)}, sent {self.event_count} events")


class SSEServer:
    """SSE 服務器主類"""
    
    def __init__(self, publisher: Optional[EventPublisher] = None):
        self.publisher = publisher or get_global_publisher()
        self.logger = logging.getLogger(f"{__name__}.SSEServer")
        self.connection_count = 0
        self.total_connections = 0
        
        # 統計信息
        self.stats = {
            "active_connections": 0,
            "total_connections": 0,
            "events_streamed": 0,
            "avg_connection_duration": 0.0,
            "start_time": time.time()
        }
    
    async def create_event_stream(self, request: Request, 
                                 event_types: Optional[list] = None,
                                 job_id: Optional[str] = None) -> StreamingResponse:
        """
        創建 SSE 事件流
        
        Args:
            request: FastAPI 請求對象
            event_types: 過濾的事件類型列表
            job_id: 過濾的任務ID
            
        Returns:
            StreamingResponse: SSE 流響應
        """
        # 驗證請求
        if request.headers.get("Accept") and "text/event-stream" not in request.headers.get("Accept", ""):
            raise HTTPException(status_code=406, detail="Must accept text/event-stream")
        
        # 創建事件過濾器
        event_filter = None
        if event_types or job_id:
            parsed_event_types = None
            if event_types:
                try:
                    parsed_event_types = [EventType(et) for et in event_types]
                except ValueError as e:
                    raise HTTPException(status_code=400, detail=f"Invalid event type: {e}")
            
            event_filter = EventFilter(
                event_types=parsed_event_types,
                job_id_pattern=job_id
            )
        
        # 創建 SSE 連接
        connection = SSEConnection(request, self.publisher)
        
        # 更新統計
        self.connection_count += 1
        self.total_connections += 1
        self.stats["active_connections"] = self.connection_count
        self.stats["total_connections"] = self.total_connections
        
        self.logger.info(f"New SSE connection established (total: {self.connection_count})")
        
        async def cleanup():
            self.connection_count -= 1
            self.stats["active_connections"] = self.connection_count
        
        # 返回流響應
        def event_generator():
            async def async_generator():
                try:
                    async for data in connection.stream_events(event_filter):
                        yield data
                finally:
                    await cleanup()
            
            return async_generator()
        
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Last-Event-ID",
                "Access-Control-Expose-Headers": "Last-Event-ID",
                "X-Accel-Buffering": "no",  # Nginx 無緩衝
            }
        )
    
    async def publish_result(self, data: Dict[str, Any], jwt_version: str = "unknown", 
                           latency_ms: Optional[int] = None) -> str:
        """
        快速發佈結果事件
        
        Args:
            data: 結果數據
            jwt_version: JWT 版本
            latency_ms: 延遲時間
            
        Returns:
            str: 事件ID
        """
        return await self.publisher.publish(
            EventType.RESULT,
            data,
            jwt_version=jwt_version,
            latency_ms=latency_ms
        )
    
    async def publish_batch(self, records: list, jwt_version: str = "unknown") -> list:
        """
        批量發佈結果
        
        Args:
            records: 記錄列表
            jwt_version: JWT 版本
            
        Returns:
            list: 事件ID列表
        """
        event_ids = []
        start_time = time.time()
        
        # 發佈每個記錄
        for i, record in enumerate(records):
            event_id = await self.publisher.publish(
                EventType.RESULT,
                {
                    "record": record,
                    "batch_index": i,
                    "batch_size": len(records)
                },
                jwt_version=jwt_version,
                latency_ms=int((time.time() - start_time) * 1000)
            )
            event_ids.append(event_id)
        
        # 發佈批次結束事件
        batch_end_id = await self.publisher.publish(
            EventType.BATCH_END,
            {
                "batch_size": len(records),
                "event_ids": event_ids,
                "total_latency_ms": int((time.time() - start_time) * 1000)
            },
            jwt_version=jwt_version
        )
        event_ids.append(batch_end_id)
        
        return event_ids
    
    async def publish_error(self, error: str, details: Dict[str, Any] = None, 
                          jwt_version: str = "unknown") -> str:
        """發佈錯誤事件"""
        return await self.publisher.publish(
            EventType.ERROR,
            {
                "error": error,
                "details": details or {},
                "timestamp": time.time()
            },
            jwt_version=jwt_version
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取服務器統計信息"""
        uptime = time.time() - self.stats["start_time"]
        return {
            **self.stats,
            "uptime_seconds": uptime,
            "publisher_stats": self.publisher.get_stats()
        }
    
    async def shutdown(self):
        """關閉服務器"""
        self.logger.info("Shutting down SSE server...")
        await self.publisher.shutdown()


# 全局 SSE 服務器實例
_global_sse_server: Optional[SSEServer] = None

def get_global_sse_server() -> SSEServer:
    """獲取全局 SSE 服務器"""
    global _global_sse_server
    if _global_sse_server is None:
        _global_sse_server = SSEServer()
    return _global_sse_server