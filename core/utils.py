#!/usr/bin/env python3
"""
通用工具函數 - 修復 Playwright evaluate 調用和 JWT 處理
"""

import asyncio
import json
import time
import base64
import hashlib
from typing import Any, Dict, Optional, Union
from playwright.async_api import Page


class PlaywrightUtils:
    """Playwright 操作工具類 - 修復 evaluate 調用問題"""
    
    @staticmethod
    async def safe_evaluate(page: Page, script: str, args: Optional[Dict[str, Any]] = None) -> Any:
        """
        安全的 page.evaluate 調用，修復多參數傳遞問題
        
        Args:
            page: Playwright 頁面對象
            script: JavaScript 代碼
            args: 參數字典，會作為單一對象傳遞給 JS
            
        Returns:
            JavaScript 執行結果
        """
        try:
            if args is None:
                return await page.evaluate(script)
            else:
                # 修復：統一打包成單個參數傳遞
                return await page.evaluate(script, args)
        except Exception as e:
            raise RuntimeError(f"Playwright evaluate failed: {e}")
    
    @staticmethod
    async def safe_evaluate_handle(page: Page, script: str, args: Optional[Dict[str, Any]] = None):
        """安全的 page.evaluate_handle 調用"""
        try:
            if args is None:
                return await page.evaluate_handle(script)
            else:
                return await page.evaluate_handle(script, args)
        except Exception as e:
            raise RuntimeError(f"Playwright evaluate_handle failed: {e}")


class JWTUtils:
    """JWT 工具類 - 處理解析、驗證和遮罩"""
    
    @staticmethod
    def extract_jwt_from_header(auth_header: str) -> Optional[str]:
        """
        從 Authorization 頭中提取單個有效 JWT
        處理被合併的多個 JWT 情況
        """
        if not auth_header:
            return None
            
        # 分割可能合併的多個 token
        pieces = [p.strip() for p in auth_header.split(",") if p.strip()] or [auth_header.strip()]
        
        # 移除前綴
        prefixes = ("bearer ", "Bearer ", "JWT ", "jwt ")
        def strip_prefix(s: str) -> str:
            for pre in prefixes:
                if s.startswith(pre):
                    return s[len(pre):].strip()
            return s.strip()
        
        # 提取所有可能的 JWT
        import re
        jwt_pattern = re.compile(r"[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+")
        candidates = []
        
        for segment in pieces:
            segment = strip_prefix(segment)
            for match in jwt_pattern.findall(segment):
                if len(match) > 50:  # 過濾太短的假 JWT
                    candidates.append(match)
        
        if not candidates:
            return None
        
        # 選擇最佳候選（按過期時間和長度）
        best = max(candidates, key=lambda t: (JWTUtils.get_exp_timestamp(t) or 0, len(t)))
        return best
    
    @staticmethod
    def get_exp_timestamp(jwt: str) -> Optional[float]:
        """提取 JWT 的過期時間戳"""
        try:
            parts = jwt.split(".")
            if len(parts) != 3:
                return None
                
            # 解碼 payload
            payload_b64 = parts[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)  # 補充 padding
            payload_bytes = base64.urlsafe_b64decode(payload_b64.encode())
            payload = json.loads(payload_bytes.decode())
            
            return float(payload.get("exp", 0))
        except Exception:
            return None
    
    @staticmethod
    def is_jwt_fresh(jwt: str, buffer_seconds: int = 30) -> bool:
        """檢查 JWT 是否新鮮（未過期）"""
        if not jwt:
            return False
            
        exp = JWTUtils.get_exp_timestamp(jwt)
        if exp is None:
            return True  # 沒有過期時間的視為有效
            
        return exp > time.time() + buffer_seconds
    
    @staticmethod
    def mask_jwt(jwt: str, show_chars: int = 10) -> str:
        """遮罩 JWT 用於日誌顯示"""
        if not jwt or len(jwt) <= show_chars * 2:
            return "***MASKED***"
        return f"{jwt[:show_chars]}...{jwt[-show_chars:]}"
    
    @staticmethod
    def generate_jwt_version(jwt: str) -> str:
        """生成 JWT 版本號用於追踪"""
        if not jwt:
            return "none"
        hash_obj = hashlib.md5(jwt.encode())
        return f"v{int(time.time())}-{hash_obj.hexdigest()[:8]}"


class BackoffCalculator:
    """退避策略計算器"""
    
    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0, backoff_factor: float = 2.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.attempt_count = 0
        self.last_success_time = time.monotonic()
    
    def get_delay(self) -> float:
        """計算下次重試延遲時間"""
        if self.attempt_count == 0:
            return 0  # 首次嘗試無延遲
        
        delay = self.base_delay * (self.backoff_factor ** (self.attempt_count - 1))
        return min(delay, self.max_delay)
    
    def record_attempt(self):
        """記錄一次嘗試"""
        self.attempt_count += 1
    
    def record_success(self):
        """記錄成功，重置計數器"""
        self.attempt_count = 0
        self.last_success_time = time.monotonic()
    
    def should_circuit_break(self, max_attempts: int = 10, circuit_timeout: float = 300.0) -> bool:
        """判斷是否應該熔斷"""
        if self.attempt_count >= max_attempts:
            return True
        
        # 如果超過熔斷超時時間，允許重試
        if (time.monotonic() - self.last_success_time) > circuit_timeout:
            self.attempt_count = 0  # 重置嘗試計數
            return False
        
        return False


class RateLimiter:
    """令牌桶限流器"""
    
    def __init__(self, rate: float, burst: int = 1):
        self.rate = rate  # 每秒令牌數
        self.burst = burst  # 桶容量
        self.tokens = burst
        self.last_update = time.monotonic()
    
    async def acquire(self) -> bool:
        """嘗試獲取令牌"""
        now = time.monotonic()
        
        # 補充令牌
        elapsed = now - self.last_update
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_update = now
        
        # 檢查是否有可用令牌
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        
        return False
    
    async def wait_for_token(self):
        """等待直到獲得令牌"""
        while not await self.acquire():
            await asyncio.sleep(0.01)


def format_duration(seconds: float) -> str:
    """格式化時間間隔為人類可讀格式"""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}min"
    else:
        return f"{seconds/3600:.1f}h"


def safe_json_dumps(obj: Any) -> str:
    """安全的 JSON 序列化，處理不可序列化對象"""
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return str(obj)