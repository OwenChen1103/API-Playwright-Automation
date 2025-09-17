#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Connect to Existing Browser - 连接到你现有的浏览器会话
这个方案不创建新的浏览器，而是连接到你已经登录的 Chrome 实例
"""

import asyncio
import os
import json
import time
import logging
from typing import Optional, Dict, Any, List
from collections import deque

from dotenv import load_dotenv
from playwright.async_api import async_playwright

# 载入环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("existing-browser-monitor")

# 配置参数
INGEST_URL = os.getenv("INGEST_URL", "http://127.0.0.1:8000/ingest")
INGEST_KEY = os.getenv("INGEST_KEY", "baccaratt9webapi")
POLL_INTERVAL_MS = int(os.getenv("POLL_INTERVAL_MS", "1000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "2"))  # 目前頁內腳本自行使用 2 分鐘

class ExistingBrowserMonitor:
    """连接现有浏览器的监控器"""

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.page = None
        self.is_running = False
        self.consecutive_101_count = 0
        self.req_template = None
        self.stats = {
            "start_time": time.time(),
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "records_processed": 0,
        }
        # 去重機制
        self._seen_set = set()
        self._seen_keys = deque(maxlen=1000)

        # Upsert 機制：狀態快取
        self._sig_by_key = {}         # key -> 上次簽名（用來判斷有沒有變）
        self._last_seen = {}          # key -> 最後見過時間（用於 TTL 清理）
        self._key_order = deque(maxlen=2000)  # 插入順序，用於 TTL 清理

        logger.info("Existing Browser Monitor initialized")

    def _uniq_key(self, record: Dict[str, Any]) -> Optional[str]:
        """產生記錄的唯一鍵 - 使用 table:round_id 作為主鍵"""
        table_id = record.get("table_id") or record.get("tableId") or record.get("table")
        round_id = (
            record.get("round_id")
            or record.get("roundId")
            or record.get("merchant_round_id")
            or record.get("id")
        )
        if table_id and round_id:
            return f"{table_id}:{round_id}"

        # 備案：table + start_time（如果沒有 round_id）
        start_time = (
            record.get("game_start_time")
            or record.get("openTime")
            or record.get("start_time")
            or record.get("開局時間")
        )
        if table_id and start_time:
            return f"{table_id}@{start_time}"
        return None

    def _status_signature(self, record: Dict[str, Any]) -> str:
        """為記錄生成狀態簽名，用來判斷狀態是否有變更"""
        r = record
        ps = r.get('game_payment_status') or r.get('payment_status') or r.get('payStatus')
        gr = None
        if isinstance(r.get('gameResult'), dict):
            gr = r['gameResult'].get('result')
        gr = gr or r.get('result')
        st = str(r.get('status') or r.get('game_status') or '').strip().lower()
        st2 = str(r.get('settle_status') or '').strip().lower()
        settled_at = r.get('settle_time') or r.get('settleTime') or r.get('paidAt') or ''
        # 視需求也可把莊/閒點數放進來，但通常不用
        return f"{ps}|{gr}|{st}|{st2}|{settled_at}"

    def _upsert_changes(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Upsert：同 key（table:round_id）若狀態簽名有變→輸出更新；沒變→略過；新 key→輸出新增。
        另外做簡單 TTL 清理，避免快取過大。
        """
        out: List[Dict[str, Any]] = []
        now = time.time()

        for r in records:
            k = self._uniq_key(r)
            if not k:
                continue
            sig = self._status_signature(r)
            prev = self._sig_by_key.get(k)

            if prev is None:
                # 首次出現 → 視為新增
                self._sig_by_key[k] = sig
                self._last_seen[k] = now
                self._key_order.append(k)
                out.append(r)
            elif prev != sig:
                # 狀態有變 → 視為更新
                self._sig_by_key[k] = sig
                self._last_seen[k] = now
                out.append(r)
            else:
                # 沒變 → 只更新 last_seen
                self._last_seen[k] = now

        # 偶爾做 TTL 清理（例如 1 小時沒再看到就丟掉）
        if len(self._key_order) >= self._key_order.maxlen * 0.9:
            cutoff = now - 3600  # 1 小時
            while self._key_order and self._last_seen.get(self._key_order[0], 0) < cutoff:
                old = self._key_order.popleft()
                self._sig_by_key.pop(old, None)
                self._last_seen.pop(old, None)

        return out

    def _dedupe_new(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """去重並返回新記錄，但允許狀態更新"""
        out = []

        # 維護進行中記錄的狀態快取
        if not hasattr(self, '_pending_records'):
            self._pending_records = {}  # key: uniq_key, value: record_state

        for r in records:
            k = self._uniq_key(r)
            if not k:
                continue

            # 檢查記錄是否為進行中狀態
            def is_in_progress_status(record):
                payment_status = record.get("game_payment_status")
                game_result = record.get("gameResult", {})
                result = game_result.get("result") if isinstance(game_result, dict) else None

                # payment_status=1 且沒有最終結果 = 進行中
                # 注意：result=0 代表取消/無效，也是有效的最終結果
                if payment_status == 1 and result not in [0, 1, 2, 3]:
                    return True

                # 其他進行中的判斷條件
                status = record.get("status", "") or record.get("game_status", "")
                if status and status.lower() in ['進行中', '開獎中', 'running', 'in_progress']:
                    return True

                return False

            is_currently_in_progress = is_in_progress_status(r)

            # 如果是新記錄
            if k not in self._seen_set:
                out.append(r)
                self._seen_set.add(k)
                self._seen_keys.append(k)

                # 如果是進行中狀態，記錄到pending快取
                if is_currently_in_progress:
                    self._pending_records[k] = {
                        'record': r,
                        'in_progress': True,
                        'last_update': time.time()
                    }
                    logger.debug(f"[BACKFILL] New in-progress record: {k}")

            # 如果是已見過的記錄，檢查是否有狀態變更
            elif k in self._pending_records:
                old_state = self._pending_records[k]

                # 如果從進行中變為已完成，允許更新
                if old_state.get('in_progress', False) and not is_currently_in_progress:
                    out.append(r)
                    logger.info(f"[BACKFILL] Status update: {k} completed")
                    # 從pending中移除已完成的記錄
                    del self._pending_records[k]

                elif is_currently_in_progress:
                    # 仍在進行中，更新記錄但不重複處理
                    self._pending_records[k] = {
                        'record': r,
                        'in_progress': True,
                        'last_update': time.time()
                    }

        # 清理超過10分鐘沒更新的pending記錄（避免記憶體洩漏）
        current_time = time.time()
        ten_minutes = 10 * 60  # 10分鐘
        keys_to_remove = [
            k for k, v in self._pending_records.items()
            if current_time - v.get('last_update', 0) > ten_minutes
        ]
        for k in keys_to_remove:
            logger.debug(f"[BACKFILL] Cleaning old pending record: {k}")
            del self._pending_records[k]

        # 當 deque 滿時自然會丟掉最舊的，但 set 還留著；
        # 簡單處理：當長度差太大時重建一次（偶爾執行即可）
        if len(self._seen_set) > len(self._seen_keys) + 500:
            self._seen_set = set(self._seen_keys)
        return out

    async def connect_to_existing_browser(self) -> bool:
        """连接到现有的 Chrome 实例"""
        try:
            self.playwright = await async_playwright().start()

            # 尝试连接到调试端口上的 Chrome
            try:
                self.browser = await self.playwright.chromium.connect_over_cdp("http://localhost:9222")
                logger.info("Connected to existing Chrome instance")
            except Exception as e:
                logger.error(f"Failed to connect to Chrome on port 9222: {e}")
                logger.info("Please start Chrome with: chrome.exe --remote-debugging-port=9222")
                return False

            # 获取现有的浏览器上下文
            if not self.browser.contexts:
                logger.error("No browser contexts found. Please open a browser window first.")
                return False

            context = self.browser.contexts[0]
            logger.info(f"Found {len(self.browser.contexts)} browser contexts")

            # 获取页面
            if context.pages:
                # 寻找包含目标域名的页面
                target_pages = [p for p in context.pages if "t9live3.vip" in p.url]
                if target_pages:
                    self.page = target_pages[0]
                    logger.info(f"Found target page: {self.page.url}")
                else:
                    # 排除 DevTools 页面，寻找其他页面
                    non_devtools_pages = [p for p in context.pages if not p.url.startswith("devtools://")]
                    if non_devtools_pages:
                        self.page = non_devtools_pages[0]
                        logger.info(f"Using non-devtools page: {self.page.url}")
                        await self.page.goto("https://i.t9live3.vip/#/gameResult", wait_until="networkidle")
                        logger.info("Navigated to target page")
                    else:
                        # 使用第一个页面
                        self.page = context.pages[0]
                        logger.info(f"Using first available page: {self.page.url}")
                        await self.page.goto("https://i.t9live3.vip/#/gameResult", wait_until="networkidle")
                        logger.info("Navigated to target page")
            else:
                # 创建新页面
                self.page = await context.new_page()
                await self.page.goto("https://i.t9live3.vip/#/gameResult", wait_until="networkidle")
                logger.info("Created new page and navigated to target")

            await asyncio.sleep(3)  # 等待页面稳定

            # Clean start: reload page once to kill any previously injected pollers
            try:
                await self.page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(1)
                logger.info("[CLEAN] Page reloaded to clear old in-page pollers")
            except Exception as e:
                logger.debug(f"[CLEAN] Initial reload skipped: {e}")

            # 阻擋把 /api/** 當成 document 導航（防 405 GET）
            async def _api_doc_guard(route, req):
                if req.resource_type == "document":
                    await route.abort()
                else:
                    await route.continue_()

            await self.page.route("**/api/**", _api_doc_guard)

            # JWT 热更新初始化脚本：维护 window._jwt_current 并让 window.fetch 每次现读
            page = self.page
            await page.add_init_script("""
(() => {
  // 先从 storage 尝试读一次
  const pick = () => window._jwt_current || localStorage.getItem('access_token') || sessionStorage.getItem('access_token') || '';
  window._jwt_current = pick();

  const origFetch = window.fetch.bind(window);
  window.fetch = async (input, init = {}) => {
    const h = new Headers(init.headers || {});
    const t = (typeof window._jwt_current === 'string' && window._jwt_current) ? window._jwt_current : '';
    if (t && !h.has('Authorization')) h.set('Authorization', 'Bearer ' + t);
    init = { ...init, headers: h, credentials: 'include', cache: 'no-store' };

    const res = await origFetch(input, init);

    // 嘗試從 JSON 回應裡面熱更新 access_token
    try {
      const clone = res.clone();
      const ct = clone.headers.get('content-type') || '';
      if (ct.includes('application/json')) {
        const data = await clone.json();
        const cand = (data && (data.access_token || (data.data && data.data.access_token) || data.token)) || '';
        if (cand && cand !== window._jwt_current) {
          window._jwt_current = cand;
          try { localStorage.setItem('access_token', cand); } catch {}
          try { sessionStorage.setItem('access_token', cand); } catch {}
          console.debug('[JWT] updated via fetch hook');
        }
      }
    } catch (e) {}

    // 若命中 result/search/list，且回應成功，把本次請求形狀保存成模板
    try {
      const urlStr = (typeof input === 'string') ? input : (input && input.url) || '';
      const low = (urlStr || '').toLowerCase();
      if (urlStr && low.includes('/api/') && low.includes('result') && (low.includes('search') || low.includes('list')) && res.ok) {
        const tpl = {
          method: (init && init.method) || 'GET',
          url: urlStr,
          headers: Object.fromEntries((init && init.headers) ? (new Headers(init.headers)).entries() : []),
          body: (init && init.body && typeof init.body !== 'string') ? JSON.stringify(init.body) : (init && init.body) || ''
        };
        window.__req_template__ = tpl;
        console.debug('[TEMPLATE] saved from fetch hook');
      }
    } catch (e) {}

    return res;
  };
})();
""")

            # 補丁1: 真实检查 HttpOnly cookies + 等待登录
            ctx = context
            timeout = 300  # 最多等 300 秒 (5分钟)
            logger.info("Waiting for login in that Chrome window (polling cookies)...")

            auth_status = {}
            all_cookies: List[Dict[str, Any]] = []

            for i in range(timeout):
                # 检查所有相关域名的 cookies
                all_cookies = []
                for domain in ["https://i.t9live3.vip", "https://t9live3.vip", "https://www.t9live3.vip"]:
                    try:
                        cookies = await ctx.cookies(domain)
                        all_cookies.extend(cookies)
                    except Exception:
                        pass

                # 也检查当前页面的所有 cookies
                try:
                    page_cookies = await ctx.cookies()
                    all_cookies.extend(page_cookies)
                except Exception:
                    pass

                # 检查页面中的认证状态
                auth_status = await page.evaluate("""
() => {
  try {
    return {
      hasLocalToken: !!(localStorage.getItem('access_token') || localStorage.getItem('token')),
      hasSessionToken: !!(sessionStorage.getItem('access_token') || sessionStorage.getItem('token')),
      hasJWT: !!window._jwt_current,
      currentUrl: location.href,
      cookieCount: document.cookie.split(';').filter(c => c.trim()).length,
      documentCookie: (document.cookie || '').substring(0, 100) + '...'
    };
  } catch (e) {
    return { error: e.message };
  }
}
""")

                # 更寬鬆的登入檢測條件
                has_cookies = len(all_cookies) > 0
                has_tokens = auth_status.get("hasLocalToken") or auth_status.get("hasSessionToken") or auth_status.get("hasJWT")
                has_auth_cookies = auth_status.get("cookieCount", 0) > 0

                if has_cookies or has_tokens or has_auth_cookies:
                    domains = sorted({c.get("domain", "unknown") for c in all_cookies})
                    logger.info("Detected login state:")
                    logger.info(f"  - Cookies: {len(all_cookies)} from domains: {domains}")
                    logger.info(f"  - Local token: {auth_status.get('hasLocalToken')}")
                    logger.info(f"  - Session token: {auth_status.get('hasSessionToken')}")
                    logger.info(f"  - JWT in page: {auth_status.get('hasJWT')}")
                    logger.info(f"  - Document cookies: {auth_status.get('cookieCount')}")
                    logger.info("Login detected, proceeding with monitor setup.")
                    break

                if i % 10 == 0:
                    # 每10秒提示一次
                    logger.warning(f"No login detected yet (attempt {i+1}/{timeout}) — please complete login in the Chrome window.")
                    logger.debug(f"Current URL: {auth_status.get('currentUrl')}")
                await asyncio.sleep(1)
            else:
                logger.error("Timed out waiting for login; still not logged in after 5 minutes.")
                logger.error("Please ensure you complete the full login process in the Chrome debug window.")
                return False

            # 捕獲真實頁面請求模板（放寬條件；僅做參考，不依賴它抓數據）
            async def _capture_template(request):
                try:
                    url = request.url
                    if "t9live3.vip/api" not in url:
                        return
                    low = url.lower()
                    if not (("result" in low) and ("search" in low or "list" in low)):
                        return
                    body = await request.post_data() if request.method in ("POST", "PUT", "PATCH") else ""
                    if request.method == "POST" and not body:
                        return  # 不覆蓋空 body
                    self.req_template = {
                        "method": request.method,
                        "url": url,
                        "headers": dict(request.headers),
                        "body": body or "",
                    }
                    await page.evaluate("(tpl) => { window.__req_template__ = tpl; }", self.req_template)
                    logger.info(f"[TEMPLATE] captured {request.method} {url} (len(body)={len(body)})")
                except Exception as e:
                    logger.debug(f"Template capture error: {e}")

            self.page.on("request", _capture_template)

            # XHR/Fetch 鏡射：遇到 /api/game/result/search 的 JSON 就送進處理管線
            async def _mirror_results(response):
                try:
                    req = response.request
                    if getattr(req, "resource_type", None) not in ("xhr", "fetch"):
                        return
                    if "/api/game/result/search" not in response.url:
                        return
                    if not response.ok:
                        return
                    if "application/json" not in (response.headers or {}).get("content-type", ""):
                        return

                    # Add DOM stability wait
                    await asyncio.sleep(0.6)  # 600ms wait for DOM to stabilize

                    data = await response.json()

                    # 提取記錄並處理
                    records = self.extract_records(data)
                    if records:
                        # Upsert：同一局狀態變了才送，新增也送；沒變就不送
                        upserts = self._upsert_changes(records)
                        if upserts:
                            logger.info(f"[MIRROR] upserts {len(upserts)} record(s)")
                            success = await self.send_to_ingest(upserts)
                            if success:
                                self.stats["records_processed"] += len(upserts)
                except Exception as e:
                    logger.debug(f"Mirror error: {e}")

            self.page.on("response", _mirror_results)

            # API 请求拦截器：改為只攔截明確標記為測試的請求，讓軟刷新正常工作
            async def _with_cookie_and_bearer(route, request):
                try:
                    # 只攔截明確標記為測試的請求，不攔截正常的 X-Monitor 請求
                    if request.headers.get("x-test-block") == "1":
                        logger.info("[BLOCK] Dropping test request")
                        await route.abort()
                        return
                    # 其餘請求照常放行，包括帶 X-Monitor 的請求
                    await route.continue_()
                except Exception:
                    await route.continue_()

            await self.page.route("**/*", _with_cookie_and_bearer)

            # 印出所有 4xx/5xx，找出一直 500 的真實 URL
            async def _resp_tracer(response):
                try:
                    status = response.status
                    if status and status >= 400:
                        req = response.request
                        logger.warning(f"[HTTP {status}] {req.method} {response.url}")
                        ct = (response.headers or {}).get("content-type", "")
                        if "application/json" in ct:
                            try:
                                txt = await response.text()
                                logger.debug(f"[HTTP {status} BODY] {txt[:300]}")
                            except Exception:
                                pass
                except Exception:
                    pass

            self.page.on("response", _resp_tracer)

            # 把頁面 console 打到你的日誌（方便觀察輪詢是否真的在跑）
            self.page.on("console", lambda msg: logger.info(f"[PAGE:{msg.type}] {msg.text}"))

            # 检查页面状态
            page_info = await self.page.evaluate("""
() => {
  try {
    const tok = (window._jwt_current || localStorage.getItem('access_token') || sessionStorage.getItem('access_token') || '');
    return {
      url: window.location.href,
      title: document.title,
      hasJWT: !!tok,
      jwtPreview: (tok || '').substring(0, 20)
    };
  } catch (e) {
    return { error: e.message };
  }
}
""")
            logger.info("Page Status:")
            logger.info(f"  URL: {page_info.get('url')}")
            logger.info(f"  Title: {page_info.get('title')}")
            logger.info(f"  Has JWT: {page_info.get('hasJWT')}")
            logger.info(f"  JWT Preview: {page_info.get('jwtPreview')}")
            logger.info(f"  HttpOnly Cookies: {len(all_cookies)}")

            # 小檢查（避免「看起來登入但其實沒 Cookie」）
            t9_cookies = await ctx.cookies("https://i.t9live3.vip")
            logger.info(f"T9 cookies: {len(t9_cookies)}")

            # 更寬鬆的認證檢測 - 如果在正確頁面就繼續
            if not page_info.get("hasJWT"):
                logger.warning("No JWT found in localStorage/sessionStorage")

                # 檢查是否有 T9 cookies 或在正確的頁面
                if len(t9_cookies) > 0 or "gameResult" in page_info.get('url', ''):
                    logger.info("Found T9 cookies or on correct page - proceeding anyway")
                else:
                    logger.info("Please login in your browser, then restart this monitor")
                    return False

            logger.info("Successfully connected to existing browser session")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to existing browser: {e}")
            return False

    def extract_records(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """从响应中提取记录"""
        try:
            if not isinstance(data, dict):
                return []

            records: List[Dict[str, Any]] = []

            # 尝试多种数据结构
            if "data" in data and isinstance(data["data"], dict):
                d = data["data"]
                if isinstance(d.get("rows"), list):
                    records = d["rows"]
                elif isinstance(d.get("list"), list):
                    records = d["list"]
                elif isinstance(d.get("data"), list):
                    records = d["data"]
                elif isinstance(data.get("data"), list):
                    records = data["data"]
            else:
                # 通用提取
                for key in ("records", "items", "list", "result", "rows"):
                    value = data.get(key)
                    if isinstance(value, list):
                        records = value
                        break

            # 過濾不穩定的記錄
            if records:
                filtered_records = []
                blockchain_filtered = 0
                unstable_status_filtered = 0

                for record in records:
                    table_id = record.get("table_id", "")
                    payment_status = record.get("game_payment_status")
                    game_result = record.get("gameResult", {})
                    result = game_result.get("result") if isinstance(game_result, dict) else None

                    # 過濾1: T9Blockchains桌台（取消率過高）
                    if isinstance(table_id, str) and table_id.startswith("T9Blockchains_"):
                        blockchain_filtered += 1
                        continue

                    # 調整過濾邏輯：只過濾真正沒有結果的進行中記錄
                    # 如果 payment_status=1 但已經有明確結果(0,1,2,3)，仍然保留（可能是狀態更新延遲）
                    # 注意：result=0 代表取消/無效，也是有效的最終結果，不應該被過濾
                    if payment_status == 1 and result not in [0, 1, 2, 3]:
                        # 進一步檢查是否真的沒有結果（避免過度過濾）
                        status_text = str(record.get('status') or record.get('game_status') or '').lower()
                        settle_status = str(record.get('settle_status') or '').lower()

                        # 如果明確是進行中狀態且沒有結果，才過濾
                        if any(word in status_text for word in ['進行', '開獎', 'running', 'progress']) or \
                           any(word in settle_status for word in ['進行', 'pending', 'processing']):
                            unstable_status_filtered += 1
                            continue

                    filtered_records.append(record)

                if blockchain_filtered > 0 or unstable_status_filtered > 0:
                    logger.debug(
                        f"Filtered out {blockchain_filtered} blockchain + {unstable_status_filtered} unstable status records"
                    )
                return filtered_records

            return []
        except Exception as e:
            logger.error(f"Record extraction error: {e}")
            return []

    async def send_to_ingest(self, records: List[Dict[str, Any]]) -> bool:
        """发送记录到 ingest API"""
        try:
            import aiohttp

            payload = {"records": records}
            headers = {
                "X-INGEST-KEY": INGEST_KEY,
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    INGEST_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        logger.info(f"Successfully sent {len(records)} records to ingest")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Ingest API error {response.status}: {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Ingest error: {e}")
            return False

    async def discover_and_arm_template(self, timeout_ms: int = 20000) -> bool:
        """發現並武裝請求模板（同時點一下「搜索/檢索」）"""
        page = self.page
        context = self.browser.contexts[0]

        # 寬鬆監聽：收集 t9live3.vip 的 XHR/Fetch（不限關鍵字）
        hits = []

        def _collector(req):
            try:
                if req.resource_type in ("xhr", "fetch") and "t9live3.vip" in req.url:
                    hits.append(req)
            except Exception:
                pass

        context.on("request", _collector)

        # 更強化的點擊「搜索/檢索」邏輯 - 嚴格限定主內容區
        clicked = await page.evaluate("""
(() => {
  // 檢查元素是否可見且可互動
  const isElementVisible = (el) => {
    if (!el || !el.offsetParent) return false;
    const style = window.getComputedStyle(el);
    return style.display !== 'none' &&
           style.visibility !== 'hidden' &&
           style.pointerEvents !== 'none' &&
           style.opacity !== '0';
  };

  // 檢查父層是否為導航區域
  const isInNavigationArea = (el) => {
    let parent = el.parentElement;
    let depth = 0;
    while (parent && depth < 15) {
      const className = (parent.className || '').toLowerCase();
      const id = (parent.id || '').toLowerCase();
      const combined = className + ' ' + id;

      // 嚴格排除導航相關區域
      if (/\b(aside|nav|navbar|navigation|sidebar|menu|drawer|collapse|left-panel|side-panel)\b/.test(combined)) {
        console.debug('[SEARCH] Rejected - in navigation area:', combined);
        return true;
      }

      // 檢查 data 屬性
      const dataAttrs = Array.from(parent.attributes)
        .filter(attr => attr.name.startsWith('data-'))
        .map(attr => attr.value.toLowerCase())
        .join(' ');
      if (/\b(nav|sidebar|menu|drawer)\b/.test(dataAttrs)) {
        console.debug('[SEARCH] Rejected - navigation data attr:', dataAttrs);
        return true;
      }

      parent = parent.parentElement;
      depth++;
    }
    return false;
  };

  // 檢查位置是否在左側區域
  const isInLeftSideArea = (el) => {
    const rect = el.getBoundingClientRect();
    if (!rect) return true;

    // 方法1: 左側1/3區域
    if (rect.left < window.innerWidth / 3) {
      console.debug('[SEARCH] Rejected - left 1/3 area:', rect.left, '< ', window.innerWidth / 3);
      return true;
    }

    // 方法2: 固定像素排除（左側240px）
    if (rect.left < 240) {
      console.debug('[SEARCH] Rejected - left 240px area:', rect.left);
      return true;
    }

    return false;
  };

  // 獲取元素文本內容
  const getElementText = (el) => {
    const text = (el.innerText || el.textContent || el.value || '').trim();
    const title = (el.title || el.getAttribute('title') || '').trim();
    const ariaLabel = (el.getAttribute('aria-label') || '').trim();
    const placeholder = (el.placeholder || el.getAttribute('placeholder') || '').trim();
    return [text, title, ariaLabel, placeholder].join(' ').toLowerCase();
  };

  // 更精確的按鈕選擇器
  const buttonSelectors = [
    'button',
    'input[type="button"]',
    'input[type="submit"]',
    'a[role="button"]',
    '[role="button"]',
    '.el-button',
    '.ant-btn',
    '[class*="btn"]',
    '[class*="button"]'
  ];

  // 收集所有候選按鈕
  let allButtons = [];
  buttonSelectors.forEach(selector => {
    try {
      allButtons.push(...document.querySelectorAll(selector));
    } catch(e) {}
  });

  // 精確的搜索關鍵詞匹配
  const searchKeywords = [
    // 精確匹配（優先級最高）
    { pattern: /^(檢索|检索)$/i, priority: 1, name: 'exact_retrieve' },
    { pattern: /^(搜索|搜尋)$/i, priority: 2, name: 'exact_search' },
    { pattern: /^(查詢|查询)$/i, priority: 3, name: 'exact_query' },
    { pattern: /^search$/i, priority: 4, name: 'exact_search_en' },
    { pattern: /^query$/i, priority: 5, name: 'exact_query_en' },

    // 包含匹配（優先級較低）
    { pattern: /檢索|检索/i, priority: 6, name: 'contains_retrieve' },
    { pattern: /搜索|搜尋/i, priority: 7, name: 'contains_search' },
    { pattern: /查詢|查询/i, priority: 8, name: 'contains_query' }
  ];

  // 按優先級篩選有效按鈕
  const validButtons = [];

  for (const button of allButtons) {
    // 1. 檢查可見性
    if (!isElementVisible(button)) {
      console.debug('[SEARCH] Skip - not visible:', button.tagName, getElementText(button));
      continue;
    }

    // 2. 檢查是否在導航區域
    if (isInNavigationArea(button)) {
      console.debug('[SEARCH] Skip - in navigation area:', getElementText(button));
      continue;
    }

    // 3. 檢查位置
    if (isInLeftSideArea(button)) {
      console.debug('[SEARCH] Skip - in left area:', getElementText(button));
      continue;
    }

    // 4. 檢查文本匹配
    const buttonText = getElementText(button);
    for (const keyword of searchKeywords) {
      if (keyword.pattern.test(buttonText)) {
        validButtons.push({
          element: button,
          text: buttonText,
          priority: keyword.priority,
          matchType: keyword.name,
          rect: button.getBoundingClientRect()
        });
        console.debug('[SEARCH] Valid candidate:', keyword.name, buttonText, button.getBoundingClientRect().left);
        break;
      }
    }
  }

  // 按優先級排序（優先級數字越小越優先）
  validButtons.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    // 同優先級下，選擇更靠右的（更可能在主內容區）
    return b.rect.left - a.rect.left;
  });

  console.log('[SEARCH] Found valid buttons:', validButtons.length);
  validButtons.forEach((btn, index) => {
    console.log(`[SEARCH] ${index + 1}. ${btn.matchType}: "${btn.text}" at x:${btn.rect.left}`);
  });

  // 嘗試點擊最佳候選按鈕
  if (validButtons.length > 0) {
    const bestButton = validButtons[0];
    console.log('[SEARCH] Clicking best button:', bestButton.matchType, bestButton.text);

    try {
      bestButton.element.click();
      return true;
    } catch(e1) {
      try {
        bestButton.element.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}));
        return true;
      } catch(e2) {
        try {
          bestButton.element.dispatchEvent(new Event('click', {bubbles: true}));
          return true;
        } catch(e3) {
          console.error('[SEARCH] All click methods failed:', e1.message, e2.message, e3.message);
        }
      }
    }
  }

  // 備案1：嘗試主內容區的表單提交
  try {
    const mainContentSelectors = ['main', '[role="main"]', '.main-content', '.content', '.page-content'];
    for (const selector of mainContentSelectors) {
      const mainArea = document.querySelector(selector);
      if (mainArea) {
        const forms = mainArea.querySelectorAll('form');
        for (const form of forms) {
          if (form.querySelector('input[type="search"], input[name*="search"], input[name*="query"]')) {
            form.dispatchEvent(new Event('submit', {bubbles: true, cancelable: true}));
            console.log('[SEARCH] Submitted form in main content area');
            return true;
          }
        }
      }
    }
  } catch(e) {}

  // 備案2：Enter鍵（最後手段）
  try {
    document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Enter', bubbles: true}));
    console.log('[SEARCH] Triggered Enter key as fallback');
    return true;
  } catch(e) {}

  console.warn('[SEARCH] No valid search button found in main content area');
  console.log('[SEARCH] Total buttons checked:', allButtons.length);
  console.log('[SEARCH] Valid buttons after filtering:', validButtons.length);

  return false;
})()
""")

        if not clicked:
            # 聚焦頁面按 Enter，萬一這頁支持鍵盤提交
            try:
                await page.keyboard.press("Enter")
            except Exception:
                pass

        # 在時間窗內輪詢查看是否有命中的請求
        deadline = time.time() + (timeout_ms / 1000.0)
        picked = None
        while time.time() < deadline:
            prefer = [
                r
                for r in hits
                if ("/api/" in r.url.lower() and any(k in r.url.lower() for k in ("result", "search", "list", "round", "game")))
            ]
            picked = prefer[0] if prefer else (hits[0] if hits else None)
            if picked:
                break
            await asyncio.sleep(0.2)

        # 停止收集
        try:
            context.off("request", _collector)
        except Exception:
            pass

        if not picked:
            logger.warning("discover_and_arm_template: no XHR/fetch captured after clicking search")
            return False

        # 取出模板（method/url/headers/body）
        body = ""
        try:
            if picked.method in ("POST", "PUT", "PATCH"):
                body = await picked.post_data() or ""
        except Exception:
            pass

        self.req_template = {
            "method": picked.method,
            "url": picked.url,
            "headers": dict(picked.headers),
            "body": body,
        }
        logger.info(f"[TEMPLATE] {picked.method} {picked.url} captured (len(body)={len(body)})")
        await self.page.evaluate("(tpl) => { window.__req_template__ = tpl; }", self.req_template)
        return True

    async def start_poller(self, interval_ms: int = 1000):
        """
        在「頁面內」每 ~1 秒打一次 /api/game/result/search（如果未被攔截）；
        目前我們用路由把 X-Monitor:1 的請求擋掉，所以主要數據仍靠鏡射。
        """
        page = self.page

        # 讓頁面可以把新資料回呼回來（如果你日後要開啟 poller）
        async def _ingest_results(items):
            try:
                if not items:
                    return
                ok = await self.send_to_ingest(items)
                if ok:
                    self.stats["records_processed"] += len(items)
                    logger.info(f"[POLLER] processed {len(items)} new records")
            except Exception as e:
                logger.debug(f"Ingest callback error: {e}")

        await page.expose_function("ingest_results", _ingest_results)

        # 安裝輪詢器（頁面上下文）
        await page.evaluate("""
(() => {
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const jitter = (b, j=300) => b + Math.floor(Math.random()*j);

  function pad(n){ return n < 10 ? '0' + n : String(n); }
  function ts(d){
    return d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate())
      + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  }

  // 改進的去重鍵邏輯 - 適配站方回傳的真實欄位
  const idOf = r => r?.round_id || r?.roundId || r?.merchant_round_id || r?.id || null;
  const tableOf = r => r?.table_id || r?.tableId || r?.table_code || r?.table || r?.['台號'] || null;
  const startOf = r => r?.game_start_time || r?.openTime || r?.start_time || r?.['開局時間'] || null;
  const uniqId = r => {
    const rid = idOf(r);
    if (rid) return String(rid);
    const t = tableOf(r), s = startOf(r);
    return (t && s) ? (t + '@' + s) : null;
  };

  // 初始化模板變數
  window.__req_template__ = window.__req_template__ || null;

  // 添加強制立即執行一次的入口
  window.__poller_force_now__ = () => { window.__poller_force_flag__ = true; };

  window.__result_poller__ = async (opts) => {
    const state = { etag: null, seen: new Set(), consecutiveFails: 0 };
    let lastRoundIdByTable = new Map(); // 改成每桌獨立追蹤最新 round_id
    let lookbackMinutesByTable = new Map(); // 每桌的動態回看時間（分鐘）

    const url = (opts && opts.url) || 'https://i.t9live3.vip/api/game/result/search';
    const baseInterval = (opts && opts.interval_ms) || 1000;
    const pageSize = (opts && opts.batch_size) || 100;
    const defaultLookbackMinutes = 2; // 預設回看2分鐘

    console.debug('[POLLER] Starting in-page poller with interval:', baseInterval);

    while (true) {
      try {
        // 檢查停止標記
        if (window.__stop_poller__) {
          console.debug('[POLLER] Stop flag detected, exiting poller');
          break;
        }

        if (window.__poller_force_flag__) {
          window.__poller_force_flag__ = false;
          console.debug('[POLLER] Force trigger activated');
        }

        // 每輪都算出時間窗（動態回看時間）
        const now = new Date();
        const endTime = ts(now);
        // 使用平均動態回看時間，若無資料則用預設值
        let avgLookbackMinutes = defaultLookbackMinutes;
        if (lookbackMinutesByTable.size > 0) {
          const totalMinutes = Array.from(lookbackMinutesByTable.values()).reduce((a, b) => a + b, 0);
          avgLookbackMinutes = Math.max(defaultLookbackMinutes, totalMinutes / lookbackMinutesByTable.size);
        }
        const startTime = ts(new Date(now.getTime() - avgLookbackMinutes * 60 * 1000));

        const token = (window._jwt_current || localStorage.getItem('access_token') || sessionStorage.getItem('access_token') || '').toString();
        let resp;

        if (window.__req_template__) {
          // 使用模板（若有）
          const t = window.__req_template__;
          const h = new Headers(t.headers || {});
          h.set('X-Monitor','1'); // 標記為監控請求，但不再被攔截
          if (token) h.set('Authorization','Bearer '+token);
          h.set('Accept','application/json, text/plain, */*');

          const method = (t.method || 'POST').toUpperCase();
          const opts2 = { method, headers: h, credentials:'include', cache:'no-store' };
          let body = t.body || '';

          // 更新 URL 查詢參數
          const urlObj = new URL(t.url, location.origin);
          const sp = urlObj.searchParams;
          ['startTime','endTime','page','pageIndex','pageSize','limit','rowsCount'].forEach(k => { if (sp.has(k)) sp.delete(k); });
          sp.set('startTime', startTime);
          sp.set('endTime',   endTime);
          sp.set('pageIndex', '1');
          sp.set('pageSize', String(pageSize));
          const targetUrl = urlObj.toString();

          // 嘗試更新 body 的時間窗
          try {
            const obj = JSON.parse(body);
            obj.startTime = startTime;
            obj.endTime   = endTime;
            if ('pageIndex' in obj) obj.pageIndex = 1;
            if ('pageSize'  in obj) obj.pageSize  = pageSize;
            body = JSON.stringify(obj);
            if (!h.has('Content-Type')) h.set('Content-Type', 'application/json;charset=UTF-8');
          } catch {
            try {
              const p = new URLSearchParams(body);
              p.set('startTime', startTime);
              p.set('endTime',   endTime);
              p.set('pageIndex', '1');
              p.set('pageSize',  String(pageSize));
              body = p.toString();
              if (!h.has('Content-Type')) h.set('Content-Type','application/x-www-form-urlencoded; charset=UTF-8');
            } catch {}
          }

          if (method !== 'GET') opts2.body = body;

          resp = await fetch(targetUrl, opts2);
          console.debug('[POLLER] used template', method, targetUrl, '→', resp.status);
        } else {
          // 預設 payload
          const body = {
            startTime, endTime,
            game_code: 'baccarat',
            page: 1, pageNum: 1, pageIndex: 1,
            pageSize: pageSize, limit: pageSize, rowsCount: pageSize
          };

          // HTTP 500 重試
          let http500_retries = 0;
          const maxRetries = 2;
          do {
            resp = await fetch(url, {
              method:'POST',
              headers:(()=>{
                const hh = new Headers({
                  'X-Monitor':'1',                 // 標記為監控請求，但不再被攔截
                  'Accept':'application/json, text/plain, */*',
                  'Content-Type':'application/json;charset=UTF-8',
                  'X-Requested-With':'XMLHttpRequest'
                });
                if (token) hh.set('Authorization','Bearer '+token);
                return hh;
              })(),
              credentials:'include',
              cache:'no-store',
              body: JSON.stringify(body)
            });
            if (resp.status === 500 && http500_retries < maxRetries) {
              http500_retries++;
              const retryDelay = 800 + (http500_retries * 400); // 0.8s, 1.2s
              console.debug('[POLLER] HTTP 500 retry', http500_retries, '/', maxRetries, 'after', retryDelay, 'ms');
              await sleep(retryDelay);
            } else {
              break;
            }
          } while (http500_retries <= maxRetries);

          console.debug('[POLLER] used default payload →', resp.status);
        }

        // 非 OK 狀態處理
        if (!resp.ok) {
          try {
            const txt = await resp.text();
            console.debug('[POLLER] HTTP', resp.status, (txt || '').slice(0, 300));
          } catch(e) {}
          state.consecutiveFails++;
          if (state.consecutiveFails >= 3) {
            console.warn('[POLLER] Too many consecutive failures, pausing...');
            await sleep(10000);
            state.consecutiveFails = 0;
          }
          await sleep(jitter(baseInterval));
          continue;
        }

        // 重置失敗計數
        state.consecutiveFails = 0;

        if (resp.status === 202) {
          const ra = parseInt(resp.headers.get('Retry-After') || '1', 10);
          await sleep((isNaN(ra) ? 1 : ra) * 1000);
          const loc = resp.headers.get('Location');
          if (loc) resp = await fetch(loc, { credentials: 'include', cache: 'no-store' });
        }

        if (resp.status === 304) {
          await sleep(jitter(baseInterval));
          continue;
        }

        const ct = resp.headers.get('content-type') || '';
        if (!ct.includes('application/json')) {
          await sleep(jitter(baseInterval));
          continue;
        }

        state.etag = resp.headers.get('ETag') || state.etag;

        const json = await resp.json();
        const biz = (json && (json.code ?? (json.data && json.data.code)));
        if (typeof biz !== 'undefined' && biz !== 200) {
          console.debug('[POLLER] api code=', biz, 'msg=', (json && (json.msg || json.message)));

          // 處理業務邏輯中的 202 - 需要軟刷新（點擊檢索按鈕）
          if (biz === 202) {
            console.debug('[POLLER] Business code 202 detected, triggering soft refresh...');
            try {
              // 使用更強大的軟刷新邏輯（來自之前的實現）
              const softRefreshSuccess = (() => {
                const byText = (el) => (el.innerText || el.textContent || "").trim();

                // 深度遍歷所有元素
                const all = [];
                const walk = (root) => {
                  const iter = document.createNodeIterator(root, NodeFilter.SHOW_ELEMENT);
                  for (let n = iter.nextNode(); n; n = iter.nextNode()) {
                    all.push(n);
                    if (n.shadowRoot) walk(n.shadowRoot);
                  }
                };
                walk(document);

                const click = (el, why) => {
                  try {
                    el.click?.();
                    el.dispatchEvent?.(new MouseEvent('click', {bubbles:true, cancelable:true}));
                    console.debug('[SOFT_REFRESH] Clicked:', why, '→', (byText(el) || el.className || el.id || el.tagName));
                    return true;
                  } catch (e) {
                    console.debug('[SOFT_REFRESH] click failed:', e.message);
                    return false;
                  }
                };

                // 優先搜索正確的檢索按鈕 - 必須是純粹的"檢索"文字

                // 1) 首先查找純粹的"檢索"按鈕（排除左側導航）
                let searchButton = null;

                for (const el of all) {
                  // 必須是按鈕元素
                  if (!el.matches?.('button,[role="button"],.el-button,.ant-btn,.btn')) continue;

                  const text = byText(el);
                  // 必須是純粹的"檢索"文字（繁體或簡體，不能包含其他文字）
                  if (text !== '檢索' && text !== '检索') continue;

                  // 檢查位置 - 排除左側區域的按鈕
                  const rect = el.getBoundingClientRect?.();
                  if (rect) {
                    // 如果按鈕在頁面左側1/3區域，很可能是導航按鈕，跳過
                    if (rect.left < window.innerWidth / 3) {
                      console.debug('[SOFT_REFRESH] Skipping left-side button:', text, 'at x:', rect.left);
                      continue;
                    }
                  }

                  // 檢查父容器是否為導航相關
                  let isNavButton = false;
                  let parent = el.parentElement;
                  let depth = 0;
                  while (parent && depth < 10) {
                    const parentClass = (parent.className || '').toLowerCase();
                    const parentId = (parent.id || '').toLowerCase();
                    const combined = parentClass + ' ' + parentId;

                    // 更嚴格的導航檢查
                    if (/sidebar|nav|menu|aside|left|drawer|collapse/.test(combined)) {
                      isNavButton = true;
                      console.debug('[SOFT_REFRESH] Found nav parent:', parentClass, parentId);
                      break;
                    }
                    parent = parent.parentElement;
                    depth++;
                  }

                  if (!isNavButton) {
                    searchButton = el;
                    console.debug('[SOFT_REFRESH] Found valid search button at x:', rect?.left || 'unknown');
                    break;
                  }
                }

                if (searchButton && click(searchButton, 'pureSearch')) return true;

                // 2) 如果沒找到純粹的"檢索"，試試其他搜索關鍵詞（包含簡繁體）
                const searchKeywords = /^(检索|檢索|搜索|搜尋|查詢|查询|Search)$/;

                for (const el of all) {
                  if (!el.matches?.('button,[role="button"],.el-button,.ant-btn,.btn')) continue;

                  const text = byText(el);
                  if (!searchKeywords.test(text)) continue;

                  // 同樣的位置檢查
                  const rect = el.getBoundingClientRect?.();
                  if (rect && rect.left < window.innerWidth / 3) continue;

                  // 排除導航按鈕
                  let isNavButton = false;
                  let parent = el.parentElement;
                  let depth = 0;
                  while (parent && depth < 10) {
                    const parentClass = (parent.className || '').toLowerCase();
                    const parentId = (parent.id || '').toLowerCase();
                    if (/sidebar|nav|menu|aside|left|drawer/.test(parentClass + ' ' + parentId)) {
                      isNavButton = true;
                      break;
                    }
                    parent = parent.parentElement;
                    depth++;
                  }

                  if (!isNavButton && click(el, 'altSearch')) return true;
                }

                // 3) 最後才按 class 尋找（但也要排除左側）
                for (const el of all) {
                  if (!el.matches?.('[class*="search"],[data-action*="search"],.el-button--primary')) continue;

                  const rect = el.getBoundingClientRect?.();
                  if (rect && rect.left < window.innerWidth / 3) continue;

                  if (click(el, 'byClass')) return true;
                }

                // 3) 備案：按 Enter 鍵
                try {
                  document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Enter', bubbles: true}));
                  console.debug('[SOFT_REFRESH] Triggered Enter key');
                  return true;
                } catch(e) {}

                return false;
              })();

              if (softRefreshSuccess) {
                console.debug('[POLLER] Soft refresh triggered successfully');
                // 等待軟刷新完成，然後強制觸發下一次輪詢
                await sleep(2000);
                if (typeof window.__poller_force_now__ === 'function') {
                  window.__poller_force_now__();
                  console.debug('[POLLER] Forced next poll after soft refresh');
                }
              } else {
                console.debug('[POLLER] Soft refresh failed, no suitable button found');
              }
            } catch (e) {
              console.debug('[POLLER] Soft refresh failed:', e);
            }
          }
        }

        const list = (json?.data?.list) || (json?.data?.rows) || (json?.data?.data) || json?.records || json?.list || [];
        const fresh = [];
        const updates = []; // 用於存放已存在但狀態有變的記錄

        // 維護進行中記錄的快取，用於回填機制
        if (!state.pendingRounds) {
          state.pendingRounds = new Map(); // key: uniqId, value: {record, lastSeen}
        }

        for (const r of list) {
          const k = uniqId(r);
          if (!k) continue;

          const ridNum = Number(r.round_id || r.roundId || 0) || 0;
          const table = tableOf(r) || 'unknown';
          const lastRoundForTable = lastRoundIdByTable.get(table) || 0;

          // 檢查記錄狀態 - 判斷是否為進行中狀態
          const isInProgress = (() => {
            const paymentStatus = r.game_payment_status;
            const gameResult = r.gameResult || {};
            const result = Number(gameResult.result);

            // payment_status=1 且沒有最終結果 = 進行中
            // 注意：result=0 代表取消/無效，也是有效的最終結果
            if (paymentStatus === 1 && ![0, 1, 2, 3].includes(result)) {
              return true;
            }

            // 其他進行中的判斷條件
            const status = r.status || r.game_status || '';
            if (status && ['進行中', '開獎中', 'running', 'in_progress'].includes(status.toLowerCase())) {
              return true;
            }

            return false;
          })();

          // 如果這個記錄之前見過
          if (state.seen.has(k)) {
            // 檢查是否從進行中變為已完成
            if (state.pendingRounds.has(k)) {
              const oldRecord = state.pendingRounds.get(k);

              // 如果狀態從進行中變為已完成，加入更新列表
              if (!isInProgress) {
                console.debug('[BACKFILL] Status update detected:', k, 'completed');
                updates.push(r);
                state.pendingRounds.delete(k); // 移除已完成的記錄
              } else {
                // 仍在進行中，更新記錄和時間
                state.pendingRounds.set(k, {record: r, lastSeen: Date.now()});
              }
            }
            continue; // 已見過的記錄不加入fresh，但可能加入updates
          }

          // 新記錄處理
          state.seen.add(k);

          // 如果是新記錄且大於該桌最大round，才加入fresh
          if (ridNum && ridNum > lastRoundForTable) {
            fresh.push(r);
          } else if (ridNum <= lastRoundForTable && ridNum > 0) {
            // 舊的round但是新的記錄（可能是之前漏掉的），也加入fresh
            console.debug('[BACKFILL] Found missed old record for table', table, ':', k);
            fresh.push(r);
          } else {
            fresh.push(r);
          }

          // 如果是進行中狀態，加入pending追蹤
          if (isInProgress) {
            state.pendingRounds.set(k, {record: r, lastSeen: Date.now()});
            console.debug('[BACKFILL] Tracking in-progress record:', k);
          }
        }

        // 清理超過5分鐘沒更新的pending記錄（避免記憶體洩漏）
        const nowTime = Date.now();
        const fiveMinutes = 5 * 60 * 1000;
        for (const [key, {lastSeen}] of state.pendingRounds.entries()) {
          if (nowTime - lastSeen > fiveMinutes) {
            console.debug('[BACKFILL] Cleaning old pending record:', key);
            state.pendingRounds.delete(key);
          }
        }

        // 檢查資料密度並動態調整回看時間
        const densityThreshold = pageSize * 0.9; // 若達到頁面大小的90%就認為密度過高
        const recordsByTable = new Map();
        for (const r of list) {
          const table = tableOf(r) || 'unknown';
          recordsByTable.set(table, (recordsByTable.get(table) || 0) + 1);
        }

        // 調整每桌的動態回看時間
        for (const [table, count] of recordsByTable.entries()) {
          const currentLookback = lookbackMinutesByTable.get(table) || defaultLookbackMinutes;

          if (count >= densityThreshold) {
            // 資料密度過高，增加回看時間
            const newLookback = Math.min(10, currentLookback + 2); // 最多10分鐘
            lookbackMinutesByTable.set(table, newLookback);
            console.debug('[DYNAMIC_LOOKBACK] Table', table, 'density high (' + count + '), increased lookback to', newLookback, 'min');
          } else if (count < densityThreshold * 0.5 && currentLookback > defaultLookbackMinutes) {
            // 資料密度較低且當前回看時間大於預設值，逐漸減少回看時間
            const newLookback = Math.max(defaultLookbackMinutes, currentLookback - 0.5);
            lookbackMinutesByTable.set(table, newLookback);
            console.debug('[DYNAMIC_LOOKBACK] Table', table, 'density low (' + count + '), decreased lookback to', newLookback, 'min');
          }
        }

        // 更新每桌的lastRoundId（只考慮fresh中的新記錄）
        if (fresh.length) {
          const roundsByTable = new Map();
          for (const r of fresh) {
            const ridNum = Number(r.round_id || r.roundId || 0) || 0;
            const table = tableOf(r) || 'unknown';
            if (ridNum > 0) {
              const existing = roundsByTable.get(table) || 0;
              if (ridNum > existing) roundsByTable.set(table, ridNum);
            }
          }
          // 更新各桌台的最新round
          for (const [table, maxRound] of roundsByTable.entries()) {
            const currentMax = lastRoundIdByTable.get(table) || 0;
            if (maxRound > currentMax) {
              lastRoundIdByTable.set(table, maxRound);
            }
          }
        }

        // === 桌台分組 BACKFILL 掃描（在處理完主要資料後，發送到 ingest 前） ===
        // 只要有 pendingRounds，就做「桌台分組 backfill 掃描」
        if (state.pendingRounds && state.pendingRounds.size > 0 && window.__req_template__) {
          // 1) 將 pending 依桌台分組，並抓每桌最早的 start_time
          const groupByTable = new Map(); // table -> { earliestStart, keys:Set, rounds:Set }
          for (const [k, info] of state.pendingRounds.entries()) {
            const rec = info.record || {};
            const table = tableOf(rec);
            const start = startOf(rec);
            if (!table || !start) continue;
            const bucket = groupByTable.get(table) || { earliestStart: start, keys: new Set(), rounds: new Set() };
            // 更新最早時間
            if (new Date(start) < new Date(bucket.earliestStart)) bucket.earliestStart = start;
            bucket.keys.add(k);
            const rid = idOf(rec);
            if (rid) bucket.rounds.add(String(rid));
            groupByTable.set(table, bucket);
          }

          // 2) 控制每次最多掃 3 個桌台（輪轉）
          const MAX_TABLE_SWEEPS = 3;
          const tables = Array.from(groupByTable.keys()).slice(0, MAX_TABLE_SWEEPS);

          for (const table of tables) {
            const bucket = groupByTable.get(table);
            // 使用該桌台的動態回看時間，至少5分鐘
            const tableLookbackMinutes = Math.max(5, lookbackMinutesByTable.get(table) || defaultLookbackMinutes);
            // 目標時間窗：該桌最早 pending 開局時間往前動態分鐘數到現在
            const startBase = new Date(bucket.earliestStart || Date.now() - 10*60*1000);
            const startTime = ts(new Date(startBase.getTime() - tableLookbackMinutes * 60 * 1000));
            const endTime   = ts(new Date());

            // 從模板複製請求（保證帶到正確 headers/其他必要欄位）
            const t = window.__req_template__;
            const h = new Headers(t.headers || {});
            const token = (window._jwt_current || localStorage.getItem('access_token') || sessionStorage.getItem('access_token') || '').toString();
            if (token) h.set('Authorization','Bearer '+token);
            h.set('Accept','application/json, text/plain, */*');

            // 把 URL 與 body 改成「單桌 + 時間窗」，並翻 1～3 頁
            const MAX_PAGES = 3;
            for (let pageIdx = 1; pageIdx <= MAX_PAGES; pageIdx++) {
              const urlObj = new URL(t.url, location.origin);
              const sp = urlObj.searchParams;
              // 清掉時間/分頁參數
              ['startTime','endTime','page','pageIndex','pageSize','limit','rowsCount'].forEach(k => sp.delete(k));
              // 桌台參數鍵名（多猜幾個）
              const tableKeys = ['table','table_id','tableId','table_code','tableCode','deskNo'];
              for (const k of tableKeys) if (sp.has(k)) sp.set(k, table);

              sp.set('startTime', startTime);
              sp.set('endTime',   endTime);
              sp.set('pageIndex', String(pageIdx));
              sp.set('pageSize',  String(100)); // 環境上限

              let body = t.body || '';
              // 同步 body 參數（JSON or x-www-form-urlencoded）
              try {
                const obj = JSON.parse(body || '{}');
                obj.startTime = startTime;
                obj.endTime   = endTime;
                obj.pageIndex = pageIdx;
                obj.pageSize  = 100;
                // 同樣猜測桌台欄位名
                for (const k of tableKeys) if (k in obj) obj[k] = table;
                body = JSON.stringify(obj);
                if (!h.has('Content-Type')) h.set('Content-Type','application/json;charset=UTF-8');
              } catch {
                try {
                  const p = new URLSearchParams(body);
                  p.set('startTime', startTime);
                  p.set('endTime',   endTime);
                  p.set('pageIndex', String(pageIdx));
                  p.set('pageSize',  String(100));
                  for (const k of tableKeys) if (p.has(k)) p.set(k, table);
                  body = p.toString();
                  if (!h.has('Content-Type')) h.set('Content-Type','application/x-www-form-urlencoded; charset=UTF-8');
                } catch {}
              }

              const resp = await fetch(urlObj.toString(), {
                method: (t.method || 'POST').toUpperCase(),
                headers: h, credentials:'include', cache:'no-store',
                body: ((t.method || 'POST').toUpperCase() === 'GET') ? undefined : body
              });

              if (!resp.ok) continue;
              const ct = resp.headers.get('content-type') || '';
              if (!ct.includes('application/json')) continue;

              const js = await resp.json();
              const list2 = (js?.data?.list) || (js?.data?.rows) || (js?.data?.data) || js?.records || js?.list || [];
              if (!Array.isArray(list2) || list2.length === 0) break;

              // 專門檢查這個桌台的 pending 目標
              const backfillUpdates = [];
              for (const r of list2) {
                const rid = String(idOf(r) || '');
                const k = tableOf(r) + ':' + rid;
                if (!bucket.keys.has(k)) continue; // 只關注這批 pending

                // 判斷是否已完成
                const paymentStatus = r.game_payment_status;
                const result = (r.gameResult && r.gameResult.result);
                const statusTxt = (r.status || r.game_status || '').toString().toLowerCase();
                const done = (paymentStatus !== 1 && [0,1,2,3].includes(result)) || ['已派彩','closed','complete','completed','已結算','已派彩'].some(t => statusTxt.includes(t));

                if (done) {
                  backfillUpdates.push(r);
                  state.pendingRounds.delete(k); // 解除追蹤
                }
              }

              if (backfillUpdates.length && typeof window.ingest_results === 'function') {
                console.debug('[BACKFILL] table', table, 'page', pageIdx, 'updates', backfillUpdates.length);
                await window.ingest_results(backfillUpdates);
              }

              // 若這個桌台的目標都清掉了，就不用再翻下一頁
              let anyLeft = false;
              for (const key of bucket.keys) if (state.pendingRounds.has(key)) { anyLeft = true; break; }
              if (!anyLeft) break;
            }
          }
        }

        // 合併新記錄和狀態更新記錄
        const allRecords = [...fresh, ...updates];
        const pendingTables = new Map();
        for (const [k, info] of state.pendingRounds.entries()) {
          const table = tableOf(info.record);
          pendingTables.set(table, (pendingTables.get(table) || 0) + 1);
        }
        console.debug('[BACKFILL] Processing:', 'total=', list.length, 'fresh=', fresh.length, 'updates=', updates.length, 'pending=', state.pendingRounds.size, 'pending_by_table=', Object.fromEntries(pendingTables));

        if (typeof window.ingest_results === 'function' && allRecords.length > 0) {
          console.debug('[POLLER]', 'total_records=', allRecords.length, 'fresh=', fresh.length, 'status_updates=', updates.length);
          await window.ingest_results(allRecords);
        }

      } catch (e) {
        console.debug('[POLLER] Error:', e);
        state.consecutiveFails++;
      }

      await sleep(jitter(baseInterval));
    }
  };
})();
""")

        # 等待輪詢器函數可用，然後啟動（fire-and-forget，不阻塞）
        await asyncio.sleep(1)
        await page.evaluate(
            """(opts) => {
  if (typeof window.__result_poller__ === 'function') {
    setTimeout(() => { window.__result_poller__(opts); }, 0);
    console.debug('[POLLER] fired');
  } else {
    console.error('Poller function not available');
  }
}""",
            {
                "url": "https://i.t9live3.vip/api/game/result/search",
                "interval_ms": int(interval_ms),
                "batch_size": int(BATCH_SIZE),
            },
        )
        logger.info(f"Started in-page poller with {interval_ms}ms interval")

    async def start(self):
        """启动监控器"""
        logger.info("Starting Existing Browser Monitor...")
        if not await self.connect_to_existing_browser():
            raise RuntimeError("Failed to connect to existing browser")

        self.is_running = True

        # 嘗試發現並武裝模板（同時點「搜索/檢索」）
        try:
            ok = await self.discover_and_arm_template(timeout_ms=20000)
            logger.info(f"Template armed: {ok}")
        except Exception as e:
            logger.warning(f"Template discovery skipped: {e}")

        # 啟動輪詢器（目前 poller 發出的請求會被路由擋掉，主要靠鏡射）
        await self.start_poller(interval_ms=POLL_INTERVAL_MS)
        logger.info("Monitor started successfully")

        try:
            # 簡單保活即可（主要工作由鏡射完成）
            while self.is_running:
                await asyncio.sleep(60)
        finally:
            await self.cleanup()

    async def stop(self):
        """停止监控器"""
        logger.info("Stopping monitor...")
        self.is_running = False

    async def cleanup(self):
        """清理资源"""
        try:
            # 不要关闭 browser，因为那是用户的浏览器
            if self.playwright:
                await self.playwright.stop()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        uptime = time.time() - self.stats["start_time"]
        success_rate = (self.stats["successful_requests"] / max(1, self.stats["total_requests"]) * 100)

        # 回填機制統計
        pending_count = len(getattr(self, '_pending_records', {}))
        backfill_stats = {
            "pending_in_progress_records": pending_count,
            "total_seen_unique_keys": len(self._seen_set),
        }

        return {
            **self.stats,
            "uptime_seconds": uptime,
            "success_rate_percent": round(success_rate, 2),
            "is_running": self.is_running,
            "backfill_stats": backfill_stats,
        }


async def main():
    """主程式入口"""
    print("=" * 80)
    print("Existing Browser Monitor")
    print("Connects to your already-logged-in Chrome browser")
    print("=" * 80)
    print("REQUIREMENTS:")
    print("1. Start Chrome with debug port:")
    print("   chrome.exe --remote-debugging-port=9222")
    print("2. Login to https://i.t9live3.vip in that Chrome window")
    print("3. Keep the browser window open")
    print("=" * 80)

    monitor = ExistingBrowserMonitor()
    try:
        await monitor.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
    finally:
        await monitor.stop()

    # 显示最终统计
    stats = monitor.get_stats()
    print("\n" + "=" * 60)
    print("FINAL STATISTICS")
    print("=" * 60)
    print(f"Uptime: {stats['uptime_seconds']:.1f}s")
    print(f"Total Requests: {stats['total_requests']}")
    print(f"Success Rate: {stats['success_rate_percent']:.1f}%")
    print(f"Records Processed: {stats['records_processed']}")
    print("\nBACKFILL MECHANISM:")
    backfill = stats.get('backfill_stats', {})
    print(f"Pending In-Progress Records: {backfill.get('pending_in_progress_records', 0)}")
    print(f"Total Unique Records Seen: {backfill.get('total_seen_unique_keys', 0)}")


if __name__ == "__main__":
    asyncio.run(main())
