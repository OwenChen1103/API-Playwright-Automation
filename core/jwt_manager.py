#!/usr/bin/env python3
"""
增強版 JWT 生命週期管理器 - 修復循環刷新和重複注入問題
"""

import asyncio
import time
import logging
from typing import Optional, Callable, Dict, Any
from enum import Enum
from dataclasses import dataclass
from core.utils import JWTUtils, BackoffCalculator


class JWTState(Enum):
    """JWT 狀態枚舉"""
    NONE = "none"
    MANUAL_SET = "manual_set"
    INTERCEPTED = "intercepted"
    RESPONSE_EXTRACTED = "response_extracted"
    PAGE_SYNCED = "page_synced"
    EXPIRED = "expired"
    REFRESHING = "refreshing"
    ERROR = "error"


@dataclass
class JWTInfo:
    """JWT 信息數據類"""
    token: str
    state: JWTState
    source: str
    version: str
    created_at: float
    last_used_at: float
    refresh_count: int = 0


class SingletonJWTManager:
    """
    單例 JWT 管理器 - 確保全系統只有一個 JWT 管理實例
    修復重複注入和狀態混亂問題
    """
    
    _instance: Optional['SingletonJWTManager'] = None
    _initialized: bool = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, manual_jwt: str = ""):
        # 防止重複初始化
        if self._initialized:
            return
        
        self.logger = logging.getLogger(f"{__name__}.JWTManager")
        
        # JWT 狀態
        self._current_jwt: Optional[JWTInfo] = None
        self._jwt_history: list[JWTInfo] = []
        
        # 狀態控制
        self._refresh_lock = asyncio.Lock()
        self._interceptor_installed = False
        self._last_injection_time = 0.0
        self._injection_version_history = set()
        
        # 退避控制器
        self._backoff = BackoffCalculator(base_delay=2.0, max_delay=30.0)
        
        # 回調函數
        self._on_jwt_updated: Optional[Callable] = None
        self._on_refresh_needed: Optional[Callable] = None
        
        # 初始化手動 JWT
        if manual_jwt:
            self._set_jwt(manual_jwt, JWTState.MANUAL_SET, "manual")
        
        self._initialized = True
        self.logger.info("🔐 JWT Manager initialized")
    
    @classmethod
    def get_instance(cls) -> 'SingletonJWTManager':
        """獲取單例實例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _set_jwt(self, token: str, state: JWTState, source: str) -> bool:
        """內部方法：設置 JWT"""
        if not token:
            return False
        
        # 清理和驗證 JWT
        cleaned_jwt = JWTUtils.extract_jwt_from_header(token)
        if not cleaned_jwt:
            self.logger.warning(f"⚠️ Invalid JWT format from {source}")
            return False
        
        # 檢查是否為相同 JWT
        if (self._current_jwt and 
            self._current_jwt.token == cleaned_jwt and 
            self._current_jwt.state == state):
            return False
        
        # 創建新的 JWT 信息
        now = time.time()
        version = JWTUtils.generate_jwt_version(cleaned_jwt)
        
        new_jwt_info = JWTInfo(
            token=cleaned_jwt,
            state=state,
            source=source,
            version=version,
            created_at=now,
            last_used_at=now
        )
        
        # 更新歷史記錄
        if self._current_jwt:
            self._jwt_history.append(self._current_jwt)
            if len(self._jwt_history) > 10:  # 只保留最近 10 個
                self._jwt_history.pop(0)
        
        # 更新當前 JWT
        old_masked = JWTUtils.mask_jwt(self._current_jwt.token if self._current_jwt else "")
        new_masked = JWTUtils.mask_jwt(cleaned_jwt)
        
        self._current_jwt = new_jwt_info
        
        # 重置退避計數器
        self._backoff.record_success()
        
        self.logger.info(
            f"🔄 JWT updated: {old_masked} → {new_masked} "
            f"({state.value}/{source}) v={version}"
        )
        
        # 觸發回調
        if self._on_jwt_updated:
            try:
                asyncio.create_task(self._on_jwt_updated(new_jwt_info))
            except Exception as e:
                self.logger.error(f"JWT update callback failed: {e}")
        
        return True
    
    def get_current_jwt(self) -> Optional[str]:
        """獲取當前有效的 JWT"""
        if not self._current_jwt:
            return None
        
        # 檢查是否過期
        if not JWTUtils.is_jwt_fresh(self._current_jwt.token):
            self._current_jwt.state = JWTState.EXPIRED
            self.logger.warning("⏰ Current JWT expired")
            return None
        
        # 更新使用時間
        self._current_jwt.last_used_at = time.time()
        return self._current_jwt.token
    
    def get_current_info(self) -> Optional[JWTInfo]:
        """獲取當前 JWT 完整信息"""
        return self._current_jwt
    
    def update_from_intercepted(self, token: str) -> bool:
        """從請求攔截更新 JWT"""
        return self._set_jwt(token, JWTState.INTERCEPTED, "intercepted")
    
    def update_from_response(self, response_data: Dict[str, Any]) -> bool:
        """從 API 響應提取並更新 JWT"""
        # 嘗試多種可能的 JWT 字段
        jwt_fields = [
            "access_token", "accessToken", "token", "jwt",
            "authToken", "authorization", "bearer"
        ]
        
        def extract_jwt_recursive(data: Any) -> Optional[str]:
            if isinstance(data, dict):
                # 直接查找
                for field in jwt_fields:
                    value = data.get(field)
                    if isinstance(value, str) and len(value) > 50 and value.count('.') >= 2:
                        return value
                
                # 遞歸查找
                for key, value in data.items():
                    if isinstance(value, (dict, list)):
                        result = extract_jwt_recursive(value)
                        if result:
                            return result
            elif isinstance(data, list):
                for item in data:
                    result = extract_jwt_recursive(item)
                    if result:
                        return result
            return None
        
        extracted_jwt = extract_jwt_recursive(response_data)
        if extracted_jwt:
            return self._set_jwt(extracted_jwt, JWTState.RESPONSE_EXTRACTED, "response")
        
        return False
    
    def update_from_page_sync(self, token: str) -> bool:
        """從頁面同步更新 JWT"""
        # 降級保護：如果當前 JWT 是高優先級來源，則拒絕降級
        if (self._current_jwt and 
            self._current_jwt.state in {JWTState.INTERCEPTED, JWTState.RESPONSE_EXTRACTED} and
            time.time() - self._current_jwt.created_at < 120):  # 2分鐘保護期
            self.logger.info("🛡️ Ignoring page sync: downgrade protection active")
            return False
        
        return self._set_jwt(token, JWTState.PAGE_SYNCED, "page_sync")
    
    def is_refresh_needed(self) -> bool:
        """判斷是否需要刷新 JWT"""
        if not self._current_jwt:
            return True
        
        # 檢查是否過期或即將過期
        if not JWTUtils.is_jwt_fresh(self._current_jwt.token, buffer_seconds=60):
            return True
        
        # 檢查是否處於錯誤狀態
        if self._current_jwt.state == JWTState.ERROR:
            return True
        
        return False
    
    async def mark_refresh_start(self) -> bool:
        """標記開始刷新，返回是否成功獲取鎖"""
        if self._refresh_lock.locked():
            self.logger.info("🔒 Refresh already in progress")
            return False
        
        await self._refresh_lock.acquire()
        if self._current_jwt:
            self._current_jwt.state = JWTState.REFRESHING
            self._current_jwt.refresh_count += 1
        
        self._backoff.record_attempt()
        self.logger.info("🔄 JWT refresh started")
        return True
    
    def mark_refresh_end(self, success: bool = False):
        """標記刷新結束"""
        if self._refresh_lock.locked():
            if success:
                self._backoff.record_success()
            
            if self._current_jwt and self._current_jwt.state == JWTState.REFRESHING:
                self._current_jwt.state = JWTState.ERROR if not success else self._current_jwt.state
            
            self._refresh_lock.release()
            self.logger.info(f"✅ JWT refresh ended (success: {success})")
    
    def get_backoff_delay(self) -> float:
        """獲取退避延遲時間"""
        return self._backoff.get_delay()
    
    def should_circuit_break(self) -> bool:
        """判斷是否應該熔斷"""
        return self._backoff.should_circuit_break()
    
    def set_interceptor_installed(self, installed: bool):
        """設置攔截器安裝狀態"""
        if self._interceptor_installed != installed:
            self._interceptor_installed = installed
            self.logger.info(f"🎯 JWT interceptor {'installed' if installed else 'uninstalled'}")
    
    def is_interceptor_installed(self) -> bool:
        """檢查攔截器是否已安裝"""
        return self._interceptor_installed
    
    def should_inject_to_page(self) -> bool:
        """判斷是否需要注入到頁面"""
        if not self._current_jwt:
            return False
        
        # 防止重複注入
        now = time.time()
        if (now - self._last_injection_time) < 10:  # 10秒內不重複注入
            return False
        
        # 檢查版本是否已注入過
        if self._current_jwt.version in self._injection_version_history:
            return False
        
        return True
    
    def mark_injection_done(self):
        """標記注入完成"""
        if self._current_jwt:
            self._last_injection_time = time.time()
            self._injection_version_history.add(self._current_jwt.version)
            
            # 清理舊版本記錄
            if len(self._injection_version_history) > 20:
                # 保留最新 10 個版本
                recent_versions = list(self._injection_version_history)[-10:]
                self._injection_version_history = set(recent_versions)
    
    def set_update_callback(self, callback: Callable):
        """設置 JWT 更新回調"""
        self._on_jwt_updated = callback
    
    def set_refresh_callback(self, callback: Callable):
        """設置刷新需要回調"""
        self._on_refresh_needed = callback
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取統計信息"""
        return {
            "current_state": self._current_jwt.state.value if self._current_jwt else "none",
            "current_version": self._current_jwt.version if self._current_jwt else "none",
            "refresh_count": self._current_jwt.refresh_count if self._current_jwt else 0,
            "backoff_attempts": self._backoff.attempt_count,
            "interceptor_installed": self._interceptor_installed,
            "injection_versions_count": len(self._injection_version_history),
            "history_count": len(self._jwt_history)
        }