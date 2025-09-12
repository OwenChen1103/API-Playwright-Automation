#!/usr/bin/env python3
"""
事件系統 - 實現即時推送的事件模型
"""

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator
from enum import Enum
from dataclasses import dataclass, asdict
from core.utils import safe_json_dumps


class EventType(Enum):
    """事件類型枚舉"""
    STARTED = "started"
    PARTIAL = "partial" 
    RESULT = "result"
    BATCH_END = "batchEnd"
    ERROR = "error"
    FINISHED = "finished"
    HEARTBEAT = "heartbeat"
    JWT_UPDATED = "jwt_updated"
    METRICS = "metrics"


@dataclass
class StreamEvent:
    """流事件數據結構"""
    event_id: str
    event_type: EventType
    timestamp: float
    jwt_version: str
    payload: Dict[str, Any]
    latency_ms: Optional[int] = None
    sequence: Optional[int] = None
    
    def to_sse_format(self) -> str:
        """轉換為 SSE 格式"""
        data = asdict(self)
        data['event_type'] = self.event_type.value
        return f"id: {self.event_id}\nevent: {self.event_type.value}\ndata: {safe_json_dumps(data)}\n\n"
    
    def to_websocket_format(self) -> str:
        """轉換為 WebSocket 格式"""
        data = asdict(self)
        data['event_type'] = self.event_type.value
        return safe_json_dumps(data)


class EventPublisher:
    """事件發佈器 - 管理事件流和訂閱者"""
    
    def __init__(self, max_subscribers: int = 100):
        self.max_subscribers = max_subscribers
        self._subscribers: List[asyncio.Queue] = []
        self._job_counter = 0
        self._sequence_counter = 0
        self._lock = asyncio.Lock()
        self._event_history: List[StreamEvent] = []
        self._max_history = 1000
        
        # 統計信息
        self._stats = {
            "events_published": 0,
            "subscribers_count": 0,
            "dropped_events": 0,
            "avg_latency_ms": 0.0
        }
    
    async def subscribe(self, queue_size: int = 1000) -> AsyncGenerator[StreamEvent, None]:
        """
        訂閱事件流
        
        Args:
            queue_size: 訂閱者隊列大小
            
        Yields:
            StreamEvent: 流事件
        """
        if len(self._subscribers) >= self.max_subscribers:
            raise RuntimeError(f"Maximum subscribers ({self.max_subscribers}) reached")
        
        subscriber_queue = asyncio.Queue(maxsize=queue_size)
        
        async with self._lock:
            self._subscribers.append(subscriber_queue)
            self._stats["subscribers_count"] = len(self._subscribers)
        
        try:
            while True:
                try:
                    event = await subscriber_queue.get()
                    if event is None:  # 終止信號
                        break
                    yield event
                except asyncio.CancelledError:
                    break
        finally:
            async with self._lock:
                if subscriber_queue in self._subscribers:
                    self._subscribers.remove(subscriber_queue)
                self._stats["subscribers_count"] = len(self._subscribers)
    
    async def publish(self, event_type: EventType, payload: Dict[str, Any], 
                     jwt_version: str = "unknown", latency_ms: Optional[int] = None) -> str:
        """
        發佈事件
        
        Args:
            event_type: 事件類型
            payload: 事件負載
            jwt_version: JWT 版本
            latency_ms: 延遲時間（毫秒）
            
        Returns:
            str: 事件ID
        """
        self._sequence_counter += 1
        event_id = f"job-{self._job_counter}-{self._sequence_counter}"
        
        event = StreamEvent(
            event_id=event_id,
            event_type=event_type,
            timestamp=time.time(),
            jwt_version=jwt_version,
            payload=payload,
            latency_ms=latency_ms,
            sequence=self._sequence_counter
        )
        
        # 添加到歷史記錄
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)
        
        # 廣播給所有訂閱者
        dead_subscribers = []
        published_count = 0
        
        async with self._lock:
            for subscriber_queue in self._subscribers:
                try:
                    # 非阻塞發送，如果隊列滿則丟棄
                    subscriber_queue.put_nowait(event)
                    published_count += 1
                except asyncio.QueueFull:
                    # 隊列滿時的背壓處理
                    if event_type in {EventType.HEARTBEAT}:
                        # 心跳事件可以丟棄
                        self._stats["dropped_events"] += 1
                    else:
                        # 重要事件嘗試丟棄舊事件
                        try:
                            # 移除最舊的事件
                            try:
                                subscriber_queue.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                            subscriber_queue.put_nowait(event)
                            published_count += 1
                        except asyncio.QueueFull:
                            # 仍然滿，標記為死亡訂閱者
                            dead_subscribers.append(subscriber_queue)
                except Exception:
                    dead_subscribers.append(subscriber_queue)
            
            # 清理死亡訂閱者
            for dead_sub in dead_subscribers:
                if dead_sub in self._subscribers:
                    self._subscribers.remove(dead_sub)
            
            self._stats["subscribers_count"] = len(self._subscribers)
            self._stats["events_published"] += published_count
        
        return event_id
    
    async def start_job(self, job_name: str = "") -> str:
        """開始一個新的任務"""
        self._job_counter += 1
        self._sequence_counter = 0
        
        job_id = f"job-{self._job_counter}"
        await self.publish(
            EventType.STARTED,
            {
                "job_id": job_id,
                "job_name": job_name,
                "started_at": time.time()
            }
        )
        return job_id
    
    async def end_job(self, job_id: str, success: bool = True, summary: Dict[str, Any] = None):
        """結束任務"""
        await self.publish(
            EventType.FINISHED,
            {
                "job_id": job_id,
                "success": success,
                "finished_at": time.time(),
                "summary": summary or {}
            }
        )
    
    async def send_heartbeat(self, stats: Dict[str, Any] = None):
        """發送心跳事件"""
        await self.publish(
            EventType.HEARTBEAT,
            {
                "timestamp": time.time(),
                "stats": stats or {},
                "publisher_stats": self._stats.copy()
            }
        )
    
    def get_event_history(self, last_event_id: Optional[str] = None, 
                         limit: int = 100) -> List[StreamEvent]:
        """
        獲取事件歷史（用於 Last-Event-ID 續傳）
        
        Args:
            last_event_id: 最後接收的事件ID
            limit: 返回事件數限制
            
        Returns:
            List[StreamEvent]: 事件列表
        """
        if not last_event_id:
            return self._event_history[-limit:] if self._event_history else []
        
        # 找到最後事件的位置
        start_index = 0
        for i, event in enumerate(self._event_history):
            if event.event_id == last_event_id:
                start_index = i + 1
                break
        
        return self._event_history[start_index:start_index + limit]
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取發佈器統計信息"""
        return {
            **self._stats,
            "job_counter": self._job_counter,
            "sequence_counter": self._sequence_counter,
            "history_size": len(self._event_history)
        }
    
    async def shutdown(self):
        """關閉發佈器"""
        async with self._lock:
            # 向所有訂閱者發送終止信號
            for subscriber_queue in self._subscribers:
                try:
                    await subscriber_queue.put(None)
                except Exception:
                    pass
            
            self._subscribers.clear()
            self._stats["subscribers_count"] = 0


class EventFilter:
    """事件過濾器"""
    
    def __init__(self, event_types: Optional[List[EventType]] = None,
                 job_id_pattern: Optional[str] = None,
                 min_timestamp: Optional[float] = None):
        self.event_types = set(event_types) if event_types else None
        self.job_id_pattern = job_id_pattern
        self.min_timestamp = min_timestamp
    
    def should_include(self, event: StreamEvent) -> bool:
        """判斷事件是否應該包含"""
        if self.event_types and event.event_type not in self.event_types:
            return False
        
        if self.job_id_pattern and self.job_id_pattern not in event.event_id:
            return False
        
        if self.min_timestamp and event.timestamp < self.min_timestamp:
            return False
        
        return True


# 全局事件發佈器實例
_global_publisher: Optional[EventPublisher] = None

def get_global_publisher() -> EventPublisher:
    """獲取全局事件發佈器"""
    global _global_publisher
    if _global_publisher is None:
        _global_publisher = EventPublisher()
    return _global_publisher