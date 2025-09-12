#!/usr/bin/env python3
"""
指標收集系統 - 實現可觀測性和性能監控
"""

import time
import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from collections import defaultdict, deque
import statistics
import logging


@dataclass
class LatencyMetric:
    """延遲指標"""
    name: str
    values: deque = field(default_factory=lambda: deque(maxlen=1000))
    last_updated: float = field(default_factory=time.time)
    
    def record(self, value: float):
        """記錄延遲值"""
        self.values.append(value)
        self.last_updated = time.time()
    
    def get_percentiles(self) -> Dict[str, float]:
        """獲取百分位數"""
        if not self.values:
            return {"p50": 0, "p95": 0, "p99": 0, "avg": 0}
        
        sorted_values = sorted(self.values)
        length = len(sorted_values)
        
        return {
            "p50": sorted_values[int(length * 0.5)],
            "p95": sorted_values[int(length * 0.95)],
            "p99": sorted_values[int(length * 0.99)],
            "avg": statistics.mean(sorted_values),
            "count": length
        }


@dataclass
class CounterMetric:
    """計數器指標"""
    name: str
    value: int = 0
    last_updated: float = field(default_factory=time.time)
    
    def increment(self, delta: int = 1):
        """增加計數"""
        self.value += delta
        self.last_updated = time.time()
    
    def reset(self):
        """重置計數"""
        self.value = 0
        self.last_updated = time.time()


@dataclass 
class RateMetric:
    """速率指標"""
    name: str
    events: deque = field(default_factory=lambda: deque(maxlen=300))  # 5分鐘窗口
    last_updated: float = field(default_factory=time.time)
    
    def record_event(self):
        """記錄事件"""
        now = time.time()
        self.events.append(now)
        self.last_updated = now
    
    def get_rate(self, window_seconds: int = 60) -> float:
        """獲取指定時間窗口內的速率（事件/秒）"""
        now = time.time()
        cutoff = now - window_seconds
        
        recent_events = [t for t in self.events if t >= cutoff]
        return len(recent_events) / window_seconds if window_seconds > 0 else 0


class MetricsCollector:
    """指標收集器"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.MetricsCollector")
        
        # 延遲指標
        self.latency_metrics: Dict[str, LatencyMetric] = {}
        
        # 計數器指標  
        self.counter_metrics: Dict[str, CounterMetric] = {}
        
        # 速率指標
        self.rate_metrics: Dict[str, RateMetric] = {}
        
        # 自定義指標
        self.custom_metrics: Dict[str, Any] = {}
        
        # 系統指標
        self.system_metrics = {
            "start_time": time.time(),
            "last_activity": time.time()
        }
        
        # 錯誤統計
        self.error_stats = defaultdict(int)
        self.error_history = deque(maxlen=100)
        
    def record_latency(self, name: str, latency_ms: float):
        """記錄延遲指標"""
        if name not in self.latency_metrics:
            self.latency_metrics[name] = LatencyMetric(name)
        
        self.latency_metrics[name].record(latency_ms)
        self.system_metrics["last_activity"] = time.time()
    
    def increment_counter(self, name: str, delta: int = 1):
        """增加計數器"""
        if name not in self.counter_metrics:
            self.counter_metrics[name] = CounterMetric(name)
        
        self.counter_metrics[name].increment(delta)
        self.system_metrics["last_activity"] = time.time()
    
    def record_rate_event(self, name: str):
        """記錄速率事件"""
        if name not in self.rate_metrics:
            self.rate_metrics[name] = RateMetric(name)
        
        self.rate_metrics[name].record_event()
        self.system_metrics["last_activity"] = time.time()
    
    def record_error(self, error_type: str, details: str = ""):
        """記錄錯誤"""
        self.error_stats[error_type] += 1
        self.error_history.append({
            "type": error_type,
            "details": details,
            "timestamp": time.time()
        })
        self.increment_counter("errors_total")
    
    def set_custom_metric(self, name: str, value: Any):
        """設置自定義指標"""
        self.custom_metrics[name] = value
        self.system_metrics["last_activity"] = time.time()
    
    def get_ttfd_metric(self) -> Dict[str, float]:
        """獲取 TTFD（Time To First Data）指標"""
        if "ttfd" in self.latency_metrics:
            return self.latency_metrics["ttfd"].get_percentiles()
        return {"p50": 0, "p95": 0, "p99": 0, "avg": 0, "count": 0}
    
    def get_end_to_end_metric(self) -> Dict[str, float]:
        """獲取端到端延遲指標"""
        if "end_to_end" in self.latency_metrics:
            return self.latency_metrics["end_to_end"].get_percentiles()
        return {"p50": 0, "p95": 0, "p99": 0, "avg": 0, "count": 0}
    
    def get_success_rate(self, window_seconds: int = 300) -> float:
        """計算成功率（5分鐘窗口）"""
        now = time.time()
        cutoff = now - window_seconds
        
        recent_errors = [e for e in self.error_history if e["timestamp"] >= cutoff]
        
        # 計算總請求數（假設每個事件都會觸發至少一個指標更新）
        total_events = 0
        for rate_metric in self.rate_metrics.values():
            total_events += len([t for t in rate_metric.events if t >= cutoff])
        
        if total_events == 0:
            return 1.0
        
        error_count = len(recent_errors)
        success_count = max(0, total_events - error_count)
        
        return success_count / total_events if total_events > 0 else 1.0
    
    def get_drop_rate(self) -> float:
        """計算丟包率"""
        total_attempts = self.counter_metrics.get("fetch_attempts", CounterMetric("")).value
        failed_attempts = self.counter_metrics.get("fetch_failures", CounterMetric("")).value
        
        if total_attempts == 0:
            return 0.0
        
        return failed_attempts / total_attempts
    
    def get_self_healing_rate(self) -> float:
        """計算自癒成功率"""
        total_healing = self.counter_metrics.get("healing_attempts", CounterMetric("")).value
        successful_healing = self.counter_metrics.get("healing_success", CounterMetric("")).value
        
        if total_healing == 0:
            return 1.0
        
        return successful_healing / total_healing
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """獲取綜合統計信息"""
        uptime = time.time() - self.system_metrics["start_time"]
        
        stats = {
            "system": {
                "uptime_seconds": uptime,
                "last_activity_ago": time.time() - self.system_metrics["last_activity"]
            },
            "performance": {
                "ttfd": self.get_ttfd_metric(),
                "end_to_end": self.get_end_to_end_metric(),
                "success_rate": self.get_success_rate(),
                "drop_rate": self.get_drop_rate(),
                "self_healing_rate": self.get_self_healing_rate()
            },
            "counters": {name: metric.value for name, metric in self.counter_metrics.items()},
            "rates": {
                name: {
                    "per_second": metric.get_rate(60),
                    "per_minute": metric.get_rate(60) * 60
                } for name, metric in self.rate_metrics.items()
            },
            "errors": {
                "by_type": dict(self.error_stats),
                "recent_count": len(self.error_history),
                "total_count": sum(self.error_stats.values())
            },
            "custom": self.custom_metrics.copy()
        }
        
        return stats
    
    def reset_stats(self):
        """重置所有統計信息"""
        for metric in self.latency_metrics.values():
            metric.values.clear()
        
        for metric in self.counter_metrics.values():
            metric.reset()
        
        for metric in self.rate_metrics.values():
            metric.events.clear()
        
        self.custom_metrics.clear()
        self.error_stats.clear()
        self.error_history.clear()
        
        self.system_metrics["start_time"] = time.time()
        self.system_metrics["last_activity"] = time.time()
        
        self.logger.info("📊 All metrics reset")


class WatchdogMonitor:
    """Watchdog 監控器 - 實現智能退避和熔斷"""
    
    def __init__(self, base_interval: float = 2.0, max_interval: float = 60.0):
        self.logger = logging.getLogger(f"{__name__}.WatchdogMonitor")
        self.base_interval = base_interval
        self.max_interval = max_interval
        
        # 狀態追踪
        self.last_success_time = time.monotonic()
        self.consecutive_failures = 0
        self.current_interval = base_interval
        self.circuit_breaker_open = False
        self.circuit_open_time = 0.0
        
        # 配置參數
        self.max_failures = 5
        self.circuit_timeout = 300.0  # 5分鐘後重試
        self.backoff_multiplier = 2.0
        
        # 統計信息
        self.total_checks = 0
        self.total_failures = 0
        self.total_recoveries = 0
        
    def record_success(self):
        """記錄成功事件"""
        self.last_success_time = time.monotonic()
        
        if self.consecutive_failures > 0:
            self.logger.info(f"🟢 Watchdog recovered after {self.consecutive_failures} failures")
            self.total_recoveries += 1
        
        self.consecutive_failures = 0
        self.current_interval = self.base_interval
        
        if self.circuit_breaker_open:
            self.circuit_breaker_open = False
            self.logger.info("🔓 Circuit breaker closed - service recovered")
    
    def record_failure(self, error_type: str = "unknown"):
        """記錄失敗事件"""
        self.consecutive_failures += 1
        self.total_failures += 1
        
        # 計算新的退避間隔
        self.current_interval = min(
            self.base_interval * (self.backoff_multiplier ** (self.consecutive_failures - 1)),
            self.max_interval
        )
        
        # 檢查是否需要開啟熔斷器
        if self.consecutive_failures >= self.max_failures and not self.circuit_breaker_open:
            self.circuit_breaker_open = True
            self.circuit_open_time = time.monotonic()
            self.logger.warning(
                f"🔒 Circuit breaker opened after {self.consecutive_failures} consecutive failures "
                f"(error: {error_type})"
            )
        
        self.logger.warning(
            f"🔴 Watchdog failure #{self.consecutive_failures} ({error_type}), "
            f"next check in {self.current_interval:.1f}s"
        )
    
    def should_check(self) -> bool:
        """判斷是否應該執行檢查"""
        self.total_checks += 1
        
        # 檢查熔斷器狀態
        if self.circuit_breaker_open:
            if (time.monotonic() - self.circuit_open_time) > self.circuit_timeout:
                self.logger.info("🔄 Circuit breaker timeout - attempting recovery")
                self.circuit_breaker_open = False
                self.consecutive_failures = 0  # 重置失敗計數
                return True
            else:
                return False
        
        return True
    
    def get_next_interval(self) -> float:
        """獲取下次檢查間隔"""
        return self.current_interval
    
    def get_time_since_last_success(self) -> float:
        """獲取距離上次成功的時間"""
        return time.monotonic() - self.last_success_time
    
    def is_healthy(self, max_silence_time: float = 300.0) -> bool:
        """判斷服務是否健康"""
        if self.circuit_breaker_open:
            return False
        
        if self.get_time_since_last_success() > max_silence_time:
            return False
        
        return self.consecutive_failures < 3
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取 Watchdog 統計信息"""
        return {
            "total_checks": self.total_checks,
            "total_failures": self.total_failures,
            "total_recoveries": self.total_recoveries,
            "consecutive_failures": self.consecutive_failures,
            "current_interval": self.current_interval,
            "circuit_breaker_open": self.circuit_breaker_open,
            "time_since_last_success": self.get_time_since_last_success(),
            "is_healthy": self.is_healthy(),
            "success_rate": (self.total_checks - self.total_failures) / max(self.total_checks, 1)
        }


# 全局實例
_global_metrics: Optional[MetricsCollector] = None
_global_watchdog: Optional[WatchdogMonitor] = None

def get_global_metrics() -> MetricsCollector:
    """獲取全局指標收集器"""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = MetricsCollector()
    return _global_metrics

def get_global_watchdog() -> WatchdogMonitor:
    """獲取全局 Watchdog 監控器"""
    global _global_watchdog
    if _global_watchdog is None:
        _global_watchdog = WatchdogMonitor()
    return _global_watchdog