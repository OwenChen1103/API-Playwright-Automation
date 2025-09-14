#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Connect to Existing Browser - 连接到你现有的浏览器会话

这个方案不创建新的浏览器，而是连接到你已经登录的Chrome实例
"""

import asyncio
import os
import json
import time
import datetime
import logging
import random
import re
from typing import Optional, Dict, Any, List
from collections import deque

from dotenv import load_dotenv
from playwright.async_api import async_playwright

# 载入环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("existing-browser-monitor")

# 配置参数
INGEST_URL = os.getenv("INGEST_URL", "http://127.0.0.1:8000/ingest")
INGEST_KEY = os.getenv("INGEST_KEY", "baccaratt9webapi")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_SEC", "5.0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "2"))
ACTIVE_PULL = False  # 先關掉主動 make_api_request

# === 新增滑動帶回填參數 ===
SCAN_ALL_ROWS = int(os.getenv("SCAN_ALL_ROWS", "100"))      # 掃描第一頁全部100筆
SEARCH_BAND = int(os.getenv("SEARCH_BAND", "60"))           # 搜索帶寬：前60名
SOFT_REFRESH_SEC = float(os.getenv("SOFT_REFRESH_SEC", "7.0"))  # 軟刷新間隔
UPGRADE_TIME_SEC = float(os.getenv("UPGRADE_TIME_SEC", "90.0"))  # 90秒後升級精準搜尋
UPGRADE_ATTEMPTS = int(os.getenv("UPGRADE_ATTEMPTS", "8"))   # 8次找不到後升級
ZOMBIE_TIME_SEC = float(os.getenv("ZOMBIE_TIME_SEC", "600.0"))  # 10分鐘僵屍線
PRECISE_WORKER_INTERVAL = float(os.getenv("PRECISE_WORKER_INTERVAL", "1.2"))  # 精準搜尋節流

class ExistingBrowserMonitor:
    """连接现有浏览器的监控器"""
    
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.page = None
        self.is_running = False
        self.consecutive_101_count = 0  # 连续101错误计数器
        self.req_template = None  # 儲存真實頁面請求模板
        self.stats = {
            "start_time": time.time(),
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "records_processed": 0
        }
        
        # 去重機制
        self._seen_set = set()
        self._seen_keys = deque(maxlen=1000)  # 最多保留 1000 個 key，避免記憶體無限增長

        # === 新增滑動帶回填機制 ===
        from collections import defaultdict
        self.pending = defaultdict(dict)  # {table: {round_id: metadata}}
        # metadata: {"first_seen_ts", "last_seen_ts", "last_index", "attempts", "status", "stale"}
        self.precise_queue = []  # 需要精準搜尋的隊列
        self.current_scan_results = {}  # 當前掃描結果: {round_id: {"index", "status", "result", "table"}}

        # 防抖機制
        self.last_first_round = None
        self.skip_count = 0

        logger.info("Existing Browser Monitor initialized")
    
    def _uniq_key(self, record: Dict[str, Any]) -> Optional[str]:
        """產生記錄的唯一鍵 - 使用 table:round_id 作為主鍵"""
        table_id = record.get("table_id") or record.get("tableId") or record.get("table")
        round_id = record.get("round_id") or record.get("roundId") or record.get("merchant_round_id") or record.get("id")

        if table_id and round_id:
            return f"{table_id}:{round_id}"
        
        # 備案：table + start_time（如果沒有 round_id）
        start_time = (record.get("game_start_time") or record.get("openTime") or
                     record.get("start_time") or record.get("開局時間"))

        if table_id and start_time:
            return f"{table_id}@{start_time}"
        return None

    def _dedupe_new(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """去重並返回新記錄"""
        out = []
        for r in records:
            k = self._uniq_key(r)
            if not k:
                continue
            if k in self._seen_set:
                continue
            out.append(r)
            self._seen_set.add(k)
            self._seen_keys.append(k)
            # 當 deque 滿時自然會丟掉最舊的，但 set 還留著；
            # 簡單處理：當長度差太大時重建一次（偶爾執行即可）
            if len(self._seen_set) > len(self._seen_keys) + 500:
                self._seen_set = set(self._seen_keys)
        return out

    def track_pending_record(self, table: str, round_id: str, status: str, index: int):
        """追蹤未結束的牌局到滑動帶佇列"""
        try:
            now = time.time()
            is_pending_status = any(keyword in status for keyword in ["投注中", "停止", "進行中"])

            if is_pending_status:
                if round_id not in self.pending[table]:
                    # 新的待回填項目
                    self.pending[table][round_id] = {
                        "first_seen_ts": now,
                        "last_seen_ts": now,
                        "last_index": index,
                        "attempts": 0,
                        "status": status,
                        "stale": False
                    }
                    logger.debug(f"[SLIDING] Added to pending: {table}:{round_id} at index {index} ({status})")
                else:
                    # 更新現有項目
                    self.pending[table][round_id].update({
                        "last_seen_ts": now,
                        "last_index": index,
                        "status": status
                    })
            else:
                # 已結束的牌局從待回填中移除
                if round_id in self.pending[table]:
                    del self.pending[table][round_id]
                    logger.debug(f"[SLIDING] Removed from pending: {table}:{round_id} (completed: {status})")

        except Exception as e:
            logger.debug(f"Track pending error: {e}")

    def update_scan_results(self, records: List[Dict[str, Any]]):
        """更新當前掃描結果"""
        self.current_scan_results.clear()

        for index, record in enumerate(records):
            round_id = str(record.get("round_id") or record.get("roundId") or "")
            if round_id:
                table_id = (record.get("table_id") or record.get("tableId") or
                           record.get("table") or str(record.get("台號", "")))
                status = record.get("game_payment_status_name", "")
                game_result = record.get("gameResult", {})
                result = game_result.get("result", -1) if isinstance(game_result, dict) else -1

                self.current_scan_results[round_id] = {
                    "index": index,
                    "status": status,
                    "result": result,
                    "table": table_id,
                    "record": record
                }

                # 同時追蹤到待回填佇列
                if table_id:
                    self.track_pending_record(table_id, round_id, status, index)

    def sliding_band_backfill(self) -> List[Dict[str, Any]]:
        """滑動帶回填：在第一頁搜尋帶內尋找已派彩的牌局"""
        backfilled_records = []
        now = time.time()

        for table, rounds in self.pending.items():
            rounds_to_remove = []
            rounds_to_upgrade = []

            for round_id, meta in rounds.items():
                age = now - meta["first_seen_ts"]
                last_index = meta["last_index"]

                # 檢查是否在當前掃描結果中
                if round_id in self.current_scan_results:
                    scan_result = self.current_scan_results[round_id]
                    current_index = scan_result["index"]
                    current_status = scan_result["status"]
                    current_result = scan_result["result"]

                    # 檢查是否在搜索帶內 (前 SEARCH_BAND 名)
                    if current_index < SEARCH_BAND:
                        # 更新位置信息
                        meta["last_seen_ts"] = now
                        meta["last_index"] = current_index
                        meta["attempts"] += 1

                        # 檢查是否已派彩
                        is_completed = (current_result in (0, 1, 2, 3) or
                                      any(keyword in current_status for keyword in ["已派彩", "派彩", "結束"]))

                        if is_completed:
                            logger.info(f"[SLIDING] Found completed game in band: {table}:{round_id} at index {current_index} ({current_status})")
                            backfilled_records.append(scan_result["record"])
                            rounds_to_remove.append(round_id)
                        elif current_index > last_index + 40:  # 漂移太遠
                            logger.debug(f"[SLIDING] Round drifted too far: {table}:{round_id} from {last_index} to {current_index}")
                    else:
                        meta["attempts"] += 1
                        logger.debug(f"[SLIDING] Round outside search band: {table}:{round_id} at index {current_index}")

                else:
                    # 在當前掃描中找不到這筆
                    meta["attempts"] += 1
                    logger.debug(f"[SLIDING] Round not found in current scan: {table}:{round_id} (attempts: {meta['attempts']})")

                # 檢查是否需要升級到精準搜尋
                if (age > UPGRADE_TIME_SEC or meta["attempts"] >= UPGRADE_ATTEMPTS) and not meta["stale"]:
                    if age < ZOMBIE_TIME_SEC:
                        rounds_to_upgrade.append((table, round_id))
                        meta["stale"] = True  # 標記為已升級，避免重複
                        logger.info(f"[SLIDING] Upgrading to precise search: {table}:{round_id} (age: {age:.1f}s, attempts: {meta['attempts']})")
                    else:
                        # 超過僵屍線，直接放棄
                        logger.info(f"[SLIDING] Dropping zombie round: {table}:{round_id} (age: {age:.1f}s)")
                        rounds_to_remove.append(round_id)

            # 清理已完成的項目
            for round_id in rounds_to_remove:
                del rounds[round_id]

            # 添加到精準搜尋隊列
            for item in rounds_to_upgrade:
                if item not in self.precise_queue:
                    self.precise_queue.append(item)

        return backfilled_records

    async def connect_to_existing_browser(self):
        """连接到现有的Chrome实例"""
        try:
            self.playwright = await async_playwright().start()
            
            # 尝试连接到调试端口上的Chrome
            # 你需要用以下命令启动Chrome：
            # chrome.exe --remote-debugging-port=9222 --user-data-dir="./chrome-debug"
            
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
            
            # 选择第一个上下文（通常是默认的）
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
                    # 使用第一个页面
                    self.page = context.pages[0]
                    logger.info(f"Using first available page: {self.page.url}")
                    # 导航到目标页面
                    await self.page.goto("https://i.t9live3.vip/#/gameResult", wait_until="networkidle")
                    logger.info("Navigated to target page")
            else:
                # 创建新页面
                self.page = await context.new_page()
                await self.page.goto("https://i.t9live3.vip/#/gameResult", wait_until="networkidle")
                logger.info("Created new page and navigated to target")
            
            await asyncio.sleep(3)  # 等待页面稳定
            
            # 🔹 Clean start: reload page once to kill any previously injected pollers
            try:
                await self.page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(1)
                logger.info("[CLEAN] Page reloaded to clear old in-page pollers")
            except Exception as e:
                logger.debug(f"[CLEAN] Initial reload skipped: {e}")
            
            # 阻擋把 /api/** 當成 document 導航（防 405 GET）
            await self.page.route("**/api/**", lambda route, req: (
                asyncio.create_task(route.abort()) if req.resource_type == "document" else asyncio.create_task(route.continue_())
            ))
            
            # 1A) JWT 热更新初始化脚本：维护 window._jwt_current 并让 window.fetch 每次现读
            page = self.page
            await page.add_init_script("""
(() => {
  // 先从 storage 尝试读一次
  const pick = () =>
    window._jwt_current ||
    localStorage.getItem('access_token') ||
    sessionStorage.getItem('access_token') || '';

  window._jwt_current = pick();

  const origFetch = window.fetch.bind(window);
  window.fetch = async (input, init={}) => {
    const h = new Headers(init.headers || {});
    const t = (typeof window._jwt_current === 'string' && window._jwt_current) ? window._jwt_current : '';
    if (t && !h.has('Authorization')) h.set('Authorization', 'Bearer ' + t);
    init = { ...init, headers: h, credentials: 'include', cache: 'no-store' };
    const res = await origFetch(input, init);

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
      const low = urlStr.toLowerCase();
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
            
            # 1B) 补丁1: 真实检查 HttpOnly cookies + 等待登录
            ctx = context
            
            timeout = 300  # 最多等 300 秒 (5分钟)
            logger.info("Waiting for login in that Chrome window (polling cookies)...")
            
            for i in range(timeout):
                # 检查所有相关域名的 cookies
                all_cookies = []
                for domain in ["https://i.t9live3.vip", "https://t9live3.vip", "https://www.t9live3.vip"]:
                    try:
                        cookies = await ctx.cookies(domain)
                        all_cookies.extend(cookies)
                    except:
                        pass
                
                # 也检查当前页面的所有 cookies
                try:
                    page_cookies = await ctx.cookies()
                    all_cookies.extend(page_cookies)
                except:
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
                            documentCookie: document.cookie.substring(0, 100) + '...'
                        };
                    } catch (e) {
                        return { error: e.message };
                    }
                }
                """)
                
                logger.debug(f"Auth check {i}: cookies={len(all_cookies)}, auth_status={auth_status}")
                
                # 更寬鬆的登入檢測條件
                has_cookies = len(all_cookies) > 0
                has_tokens = auth_status.get('hasLocalToken') or auth_status.get('hasSessionToken') or auth_status.get('hasJWT')
                has_auth_cookies = auth_status.get('cookieCount', 0) > 0
                
                if has_cookies or has_tokens or has_auth_cookies:
                    domains = sorted({c.get("domain", "unknown") for c in all_cookies})
                    logger.info(f"Detected login state:")
                    logger.info(f"  - Cookies: {len(all_cookies)} from domains: {domains}")
                    logger.info(f"  - Local token: {auth_status.get('hasLocalToken')}")
                    logger.info(f"  - Session token: {auth_status.get('hasSessionToken')}")
                    logger.info(f"  - JWT in page: {auth_status.get('hasJWT')}")
                    logger.info(f"  - Document cookies: {auth_status.get('cookieCount')}")
                    logger.info("Login detected, proceeding with monitor setup.")
                    break
                    
                if i % 10 == 0:  # 每10秒提示一次
                    logger.warning(f"No login detected yet (attempt {i+1}/{timeout}) — please complete login in the Chrome window.")
                    logger.debug(f"Current URL: {auth_status.get('currentUrl')}")
                    
                await asyncio.sleep(1)
            else:
                logger.error("Timed out waiting for login; still not logged in after 5 minutes.")
                logger.error("Please ensure you complete the full login process in the Chrome debug window.")
                return False
            
            # 1C) 回应监听：只要任何 XHR/Fetch 的 JSON 回传带到 token，就即时更新
            async def _jwt_hot_update_from_response(response):
                try:
                    req = response.request
                    # 只看 XHR / fetch
                    if getattr(req, "resource_type", None) not in ("xhr", "fetch"):
                        return
                    ct = (response.headers or {}).get("content-type", "")
                    if "application/json" not in ct:
                        return
                    data = await response.json()
                    cand = (data.get("access_token")
                            or (data.get("data") or {}).get("access_token")
                            or data.get("token"))
                    if cand:
                        await page.evaluate(
                            """(t) => {
                               window._jwt_current = t;
                               try { localStorage.setItem('access_token', t); } catch {}
                               try { sessionStorage.setItem('access_token', t); } catch {}
                               console.debug('[JWT] updated via response hook');
                             }""",
                            cand
                        )
                        # 重置连续101计数
                        self.consecutive_101_count = 0
                except Exception:
                    pass

            page.on("response", _jwt_hot_update_from_response)
            
            # 1C+) 捕獲真實頁面請求模板（放寬條件）
            async def _capture_template(request):
                try:
                    url = request.url
                    if "t9live3.vip/api" not in url:
                        return
                    # 關鍵詞同時命中 'result' 與 'search'（或 'list'）
                    low = url.lower()
                    if not (("result" in low) and ("search" in low or "list" in low)):
                        return

                    body = await request.post_data() if request.method in ("POST","PUT","PATCH") else ""
                    if request.method == "POST" and not body:
                        return  # 不覆蓋
                    self.req_template = {
                        "method": request.method,
                        "url": url,
                        "headers": dict(request.headers),
                        "body": body or ""
                    }
                    await page.evaluate("window.__req_template__ = arguments[0]", self.req_template)
                    logger.info(f"[TEMPLATE] captured {request.method} {url} (len(body)={len(body)})")
                except Exception as e:
                    logger.debug(f"Template capture error: {e}")
            
            page.on("request", _capture_template)
            
            # 1D) XHR/Fetch 鏡射：遇到 /api/game/result/search 的 JSON 就送進處理管線
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
                        # === 掃描全部100筆，準備 upsert 數據 ===
                        all_records = records[:100]  # 確保掃描全部100筆

                        # 防抖檢查：如果第一筆的局號沒變，跳過這次處理
                        if all_records:
                            first_round = all_records[0].get("round_id") or all_records[0].get("roundId")
                            if first_round == self.last_first_round:
                                self.skip_count += 1
                                if self.skip_count <= 2:  # 容許連續 2 次相同
                                    logger.debug(f"[UPSERT] Same first round {first_round}, skipping ({self.skip_count})")
                                    return
                            else:
                                self.last_first_round = first_round
                                self.skip_count = 0

                        # 更新當前掃描結果 + 自動追蹤待回填
                        self.update_scan_results(all_records)

                        # 執行滑動帶回填檢查
                        backfilled_records = self.sliding_band_backfill()

                        # 合併所有記錄並發送完整快照用於 upsert
                        all_to_send = []
                        seen_keys = set()

                        # 包含所有記錄（新的 + 回填的），後端會進行 upsert
                        for record in all_records + backfilled_records:
                            table_id = record.get("table_id") or record.get("tableId") or record.get("table")
                            round_id = record.get("round_id") or record.get("roundId")
                            if table_id and round_id:
                                key = f"{table_id}:{round_id}"
                                if key not in seen_keys:
                                    all_to_send.append(record)
                                    seen_keys.add(key)

                        if all_to_send:
                            logger.info(f"[UPSERT] sending {len(all_to_send)} records for upsert processing")
                            success = await self.send_to_ingest(all_to_send)
                            if success:
                                self.stats["records_processed"] += len(all_to_send)
                except Exception as e:
                    logger.debug(f"Mirror error: {e}")

            page.on("response", _mirror_results)
            
            # 1E) WebSocket 鏡射（若站點有）：監聽 BrowserContext 的 websocket
            def _on_ws(ws):
                logger.info(f"[WS] connected: {ws.url}")
                def on_rx(payload):
                    try:
                        # 很多站會用 JSON；如果是文字就先判斷再 json.loads
                        logger.debug(f"[WS RX] {payload[:200] if len(payload) > 200 else payload}")
                        # TODO: 依實際格式解析和處理 WebSocket 數據
                    except Exception:
                        pass
                ws.on("framereceived", on_rx)

            ctx.on("websocket", _on_ws)
            
            # 2) 更新的 API 请求拦截器：僅對 API 網域加 Authorization，且每次現讀
            API_HOSTS = ("i.t9live3.vip", "i.t9live3.vip:443")
            API_PATH_PREFIXES = ("/api/",)
            
            async def _with_cookie_and_bearer(route, request):
                try:
                    # 🛑 在軟刷新模式下，任何帶 X-Monitor: 1 的請求都是舊 poller 打的，直接擋掉
                    if request.headers.get("x-monitor") == "1":
                        logger.info("[BLOCK] Dropping leftover in-page poller request")
                        await route.abort()
                        return

                    # 其餘請求照常放行（不再主動加 Authorization 之類）
                    await route.continue_()
                except Exception:
                    await route.continue_()
            
            await page.route("**/*", _with_cookie_and_bearer)
            
            # 印出所有 4xx/5xx，找出一直 500 的真實 URL
            async def _resp_tracer(response):
                try:
                    status = response.status
                    if status and status >= 400:
                        req = response.request
                        logger.warning(f"[HTTP {status}] {req.method} {response.url}")
                        # 若是我們關心的 JSON，就印出前段文字協助定位
                        ct = (response.headers or {}).get("content-type", "")
                        if "application/json" in ct:
                            try:
                                txt = await response.text()
                                logger.debug(f"[HTTP {status} BODY] {txt[:300]}")
                            except Exception:
                                pass
                except Exception:
                    pass

            page.on("response", _resp_tracer)
            
            # 把頁面 console 打到你的日誌（方便觀察輪詢是否真的在跑）
            page.on("console", lambda msg: logger.info(f"[PAGE:{msg.type}] {msg.text}"))
            
            # 检查页面状态
            page_info = await self.page.evaluate("""
            () => {
                try {
                    return {
                        url: window.location.href,
                        title: document.title,
                        hasJWT: !!(window._jwt_current || localStorage.getItem('access_token') || sessionStorage.getItem('access_token')),
                        jwtPreview: (window._jwt_current || localStorage.getItem('access_token') || sessionStorage.getItem('access_token') || '').substring(0, 20)
                    };
                } catch (e) {
                    return { error: e.message };
                }
            }
            """)
            
            logger.info(f"Page Status:")
            logger.info(f"  URL: {page_info.get('url')}")
            logger.info(f"  Title: {page_info.get('title')}")
            logger.info(f"  Has JWT: {page_info.get('hasJWT')}")
            logger.info(f"  JWT Preview: {page_info.get('jwtPreview')}")
            logger.info(f"  HttpOnly Cookies: {len(all_cookies)}")
            
            # 小檢查（避免「看起來登入但其實沒 Cookie」）
            t9_cookies = await ctx.cookies("https://i.t9live3.vip")
            logger.info(f"T9 cookies: {len(t9_cookies)}")
            
            if not page_info.get('hasJWT'):
                logger.warning("No JWT found in page - you may need to login first")
                logger.info("Please login in your browser, then restart this monitor")
                return False
            
            logger.info("Successfully connected to existing browser session")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to existing browser: {e}")
            return False
    
    async def make_api_request(self) -> Optional[Dict[str, Any]]:
        """在现有页面中发送 API 请求"""
        try:
            if not self.page:
                return {"_err": "NO_PAGE"}
            
            # 生成时间范围
            now = datetime.datetime.now()
            start_time = (now - datetime.timedelta(minutes=LOOKBACK_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
            end_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
            # 3) 更新的頁面評估：每次現讀 window._jwt_current
            api_result = await self.page.evaluate(f"""
            async () => {{
              // 每次「現讀」最新 token
              const jwt = (typeof window._jwt_current === 'string' && window._jwt_current) ? window._jwt_current : '';
            
              if (!jwt) return {{ error: 'No JWT in page' }};
            
              const buildHeaders = () => ({{ 
                'Content-Type': 'application/json;charset=UTF-8',
                'Accept': 'application/json, text/plain, */*',
                'X-Requested-With': 'XMLHttpRequest',
                'Authorization': 'Bearer ' + jwt
              }});
            
              const payload = {{
                game_code: 'baccarat',
                startTime: "{start_time}",
                endTime: "{end_time}",
                page: 1,
                pageNum: 1,
                limit: {BATCH_SIZE},
                pageSize: {BATCH_SIZE},
                rowsCount: {BATCH_SIZE}
              }};
            
              let res = await fetch('https://i.t9live3.vip/api/game/result/search', {{
                method: 'POST',
                headers: buildHeaders(),
                credentials: 'include',
                cache: 'no-store',
                body: JSON.stringify(payload)
              }});

              // HTTP 500 重試機制（最多 3 次）
              let http500_retries = 0;
              while (res.status === 500 && http500_retries < 3) {{
                http500_retries++;
                const retryDelay = 1000 + (http500_retries * 500); // 1s, 1.5s, 2s
                console.debug(`[HTTP 500] Retry ${{http500_retries}}/3 after ${{retryDelay}}ms`);
                await new Promise(r => setTimeout(r, retryDelay));

                res = await fetch('https://i.t9live3.vip/api/game/result/search', {{
                  method: 'POST',
                  headers: buildHeaders(),
                  credentials: 'include',
                  cache: 'no-store',
                  body: JSON.stringify(payload)
                }});
              }}

              // 202 正規輪詢（最多 5 次）
              let tries = 0;
              while (res.status === 202 && tries < 5) {{
                const ra = parseInt(res.headers.get('Retry-After') || '0', 10);
                const loc = res.headers.get('Location');
                const waitMs = ra > 0 ? ra*1000 : (900 + Math.random()*600);
                await new Promise(r => setTimeout(r, waitMs));
                res = await fetch(loc || 'https://i.t9live3.vip/api/game/result/search', {{
                  method: 'GET',
                  headers: buildHeaders(),
                  credentials: 'include',
                  cache: 'no-store'
                }});
                tries++;
              }}

              let data = {{}};
              try {{ data = await res.json(); }} catch {{}}
              return {{ success: res.ok, status: res.status, data, jwt_used: jwt.substring(0, 20), http500_retries }};
            }}
            """)
            
            self.stats["total_requests"] += 1
            
            if api_result.get("success") and api_result.get("status") == 200:
                data = api_result.get("data", {})
                code = data.get("code")
                
                logger.debug(f"API Response: status={api_result.get('status')}, code={code}")
                
                if code == 200:
                    self.stats["successful_requests"] += 1
                    return data
                elif code == 202:
                    logger.debug("Request queued (202)")
                    return None
                elif code == 101:
                    logger.warning("JWT expired or session invalid (101 error)")
                    self.stats["failed_requests"] += 1
                    return {"_err": "CODE_101"}
                else:
                    logger.warning(f"Unexpected response code: {code}")
                    self.stats["failed_requests"] += 1
                    return None
            else:
                error_msg = api_result.get("error", "Unknown error")
                logger.warning(f"API request failed: {error_msg}")
                self.stats["failed_requests"] += 1
                return {"_err": "OTHER"}
                
        except Exception as e:
            logger.error(f"API request error: {e}")
            self.stats["failed_requests"] += 1
            return {"_err": "EXCEPTION"}
    
    def extract_records(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """从响应中提取记录"""
        try:
            if not isinstance(data, dict):
                return []

            records = []

            # 尝试多种数据结构
            if 'data' in data and isinstance(data['data'], dict):
                d = data['data']
                if isinstance(d.get('rows'), list):
                    records = d['rows']
                elif isinstance(d.get('list'), list):
                    records = d['list']
                elif isinstance(d.get('data'), list):
                    records = d['data']
            elif isinstance(data.get('data'), list):
                records = data['data']
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
                    table_id = record.get('table_id', '')
                    payment_status = record.get('game_payment_status')
                    game_result = record.get('gameResult', {})
                    result = game_result.get('result') if isinstance(game_result, dict) else None

                    # 過濾1: T9Blockchains桌台（取消率過高）
                    if table_id.startswith('T9Blockchains_'):
                        blockchain_filtered += 1
                        continue

                    # 過濾2: payment_status=1且沒有最終結果的記錄（48%取消率）
                    if payment_status == 1 and result not in [1, 2, 3]:  # 只保留有明確結果的status=1記錄
                        unstable_status_filtered += 1
                        continue

                    filtered_records.append(record)

                if blockchain_filtered > 0 or unstable_status_filtered > 0:
                    logger.debug(f"Filtered out {blockchain_filtered} blockchain + {unstable_status_filtered} unstable status records")

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
                "Content-Type": "application/json"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    INGEST_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
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
    
    async def monitor_loop(self):
        """主监控循环"""
        logger.info("Starting monitor loop...")
        
        while self.is_running:
            try:
                if not ACTIVE_PULL:
                    await asyncio.sleep(POLL_INTERVAL + random.uniform(0.2, 0.6))
                    continue
                
                # 发送 API 请求
                data = await self.make_api_request()
                
                if data and not data.get("_err"):
                    # 重置连续101计数
                    self.consecutive_101_count = 0
                    
                    # 提取记录
                    records = self.extract_records(data)
                    
                    if records:
                        # 发送到 ingest
                        success = await self.send_to_ingest(records)
                        if success:
                            self.stats["records_processed"] += len(records)
                            logger.info(f"Processed {len(records)} records")
                elif data and data.get("_err") == "CODE_101":
                    # 只對真正的101錯誤進行自癒
                    await self._handle_consecutive_failures()
                else:
                    # 其他錯誤只記log，不進行101自癒
                    if data:
                        logger.debug(f"API error: {data.get('_err')}")
                
                # 4) 小防呆: 轮询抖动，避免固定节律
                await asyncio.sleep(POLL_INTERVAL + random.uniform(0.2, 0.6))
                
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(5)
    
    async def lookup_round_in_page(self, round_id: str) -> bool:
        """使用頁面 UI 精準搜索特定局號"""
        try:
            page = self.page
            if not page:
                return False

            logger.debug(f"[BACKFILL] Looking up round {round_id}")

            # 1) 尋找「局號」輸入框（多種可能的選擇器）
            input_box = None
            try:
                # 嘗試不同的選擇器
                selectors = [
                    'input[placeholder*="局"]',
                    'input[placeholder*="round"]',
                    'input[placeholder*="Round"]',
                    '.el-input input[placeholder*="局"]',
                    '.ant-input[placeholder*="局"]',
                    'input[name*="round"]'
                ]

                for selector in selectors:
                    elements = await page.query_selector_all(selector)
                    if elements:
                        input_box = elements[0]
                        logger.debug(f"[BACKFILL] Found input box with selector: {selector}")
                        break

                if not input_box:
                    logger.debug("[BACKFILL] No round input box found")
                    return False

            except Exception as e:
                logger.debug(f"[BACKFILL] Input box search error: {e}")
                return False

            # 2) 尋找搜索按鈕
            search_btn = None
            try:
                btn_selectors = [
                    'button:has-text("檢索")',
                    'button:has-text("搜索")',
                    'button:has-text("查詢")',
                    'button:has-text("查询")',
                    '.el-button:has-text("檢索")',
                    '.ant-btn:has-text("搜索")'
                ]

                for selector in btn_selectors:
                    try:
                        search_btn = await page.query_selector(selector)
                        if search_btn:
                            logger.debug(f"[BACKFILL] Found search button with selector: {selector}")
                            break
                    except:
                        continue

                if not search_btn:
                    logger.debug("[BACKFILL] No search button found")
                    return False

            except Exception as e:
                logger.debug(f"[BACKFILL] Search button error: {e}")
                return False

            # 3) 輸入局號並點擊搜索
            await input_box.clear()
            await input_box.fill(str(round_id))
            await asyncio.sleep(0.5)  # 短暫等待

            await search_btn.click()
            await asyncio.sleep(1.5)  # 等待搜索結果

            # 4) 檢查搜索結果
            try:
                # 尋找結果表格
                table_rows = await page.query_selector_all("table tbody tr")
                if not table_rows:
                    logger.debug(f"[BACKFILL] No results found for round {round_id}")
                    return False

                # 檢查第一行是否包含目標局號
                first_row = table_rows[0]
                row_text = await first_row.inner_text()

                if str(round_id) in row_text:
                    logger.debug(f"[BACKFILL] Found target round in results: {round_id}")

                    # 5) 解析並發送這條記錄
                    # 這裡需要根據實際頁面結構解析
                    # 簡化版本：觸發現有的鏡射機制來處理結果
                    await asyncio.sleep(0.5)  # 讓鏡射有時間捕獲
                    return True
                else:
                    logger.debug(f"[BACKFILL] Round {round_id} not found in results")
                    return False

            except Exception as e:
                logger.debug(f"[BACKFILL] Result parsing error: {e}")
                return False

        except Exception as e:
            logger.debug(f"[BACKFILL] Lookup error for round {round_id}: {e}")
            return False

    async def discover_and_arm_template(self, timeout_ms: int = 20000):
        page = self.page
        context = self.browser.contexts[0]

        # 1) 寬鬆監聽：收集 t9live3.vip 的 XHR/Fetch（不限關鍵字）
        hits = []

        def _collector(req):
            try:
                if req.resource_type in ("xhr", "fetch") and "t9live3.vip" in req.url:
                    hits.append(req)
            except:
                pass

        context.on("request", _collector)

        # 2) 可靠點擊「搜索」：多語系/多元素型態
        clicked = await page.evaluate("""
        (() => {
          const cand = Array.from(document.querySelectorAll(
            'button, input[type="button"], a[role="button"], .el-button'
          ));
          const txt = (el) => (el.innerText || el.value || '').trim();
          const hit = cand.find(b => /搜索|查询|搜尋|查詢|检索|檢索/.test(txt(b)));
          if (hit) { hit.click(); return true; }
          return false;
        })();
        """)
        if not clicked:
            # 聚焦頁面按 Enter，萬一這頁支持鍵盤提交
            try:
                await page.keyboard.press("Enter")
            except:
                pass

        # 3) 在時間窗內輪詢查看是否有命中的請求
        deadline = time.time() + (timeout_ms / 1000.0)
        picked = None
        while time.time() < deadline:
            # 優先挑 /api/ 且 URL 含 result/search/list/round/game 的
            prefer = [r for r in hits if ("/api/" in r.url.lower() and any(k in r.url.lower() for k in ("result","search","list","round","game")))]
            picked = prefer[0] if prefer else (hits[0] if hits else None)
            if picked:
                break
            await asyncio.sleep(0.2)

        # 4) 停止收集
        try:
            context.off("request", _collector)
        except:
            pass

        if not picked:
            raise TimeoutError("no XHR/fetch captured after clicking search")

        # 5) 取出模板（method/url/headers/body）
        body = ""
        try:
            if picked.method in ("POST","PUT","PATCH"):
                body = await picked.post_data() or ""
        except:
            pass

        self.req_template = {
            "method": picked.method,
            "url": picked.url,
            "headers": dict(picked.headers),
            "body": body
        }
        logger.info(f"[TEMPLATE] {picked.method} {picked.url} captured (len(body)={len(body)})")
        await page.evaluate("window.__req_template__ = arguments[0]", self.req_template)
        return True

    async def start_poller(self, interval_ms: int = 1000):
        """
        在「頁面內」每 ~1 秒打一次 /api/game/result/search，
        現讀 JWT、處理 202/ETag，且只把「新資料」送進 ingest。
        """
        page = self.page

        # 讓頁面可以把新資料回呼回來
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
  const pad = (n)=> (n<10?'0':'')+n;
  const ts = (d)=> `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  // 改進的去重鍵邏輯 - 適配站方回傳的真實欄位
  const idOf = r => r.round_id || r.roundId || r.merchant_round_id || r.id || null;
  const tableOf = r => r.table_id || r.tableId || r.table_code || r.table || r['台號'] || null;
  const startOf = r => r.game_start_time || r.openTime || r.start_time || r['開局時間'] || null;

  const uniqId = r => {
    const rid = idOf(r);
    if (rid) return String(rid);
    const t = tableOf(r), s = startOf(r);
    return (t && s) ? `${t}@${s}` : null;
  };

  // 初始化模板變數
  window.__req_template__ = window.__req_template__ || null;
  
  // ✅ 添加強制立即執行一次的入口
  window.__poller_force_now__ = () => { window.__poller_force_flag__ = true; };
  
  window.__result_poller__ = async (opts) => {
    const state = { etag: null, seen: new Set(), consecutiveFails: 0 };
    let lastMaxRound = 0; // 只處理比它新的 round_id
    const url = opts.url || 'https://i.t9live3.vip/api/game/result/search';
    const baseInterval = opts.interval_ms || 1000;

    console.debug('[POLLER] Starting in-page poller with interval:', baseInterval);

    while (true) {
      try {
        // ✅ 檢查是否需要強制立即執行
        if (window.__poller_force_flag__) {
          window.__poller_force_flag__ = false;
          console.debug('[POLLER] Force trigger activated');
        }

        // 這裡每輪都算出時間窗
        const now = new Date();
        const pad = (n)=> (n<10?'0':'')+n;
        const ts = (d)=> `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
        const endTime = ts(now);
        const startTime = ts(new Date(now.getTime() - {LOOKBACK_MINUTES}*60*1000)); // 近 {LOOKBACK_MINUTES} 分鐘

        const token = (window._jwt_current
                       || localStorage.getItem('access_token')
                       || sessionStorage.getItem('access_token')
                       || '').toString();
        let resp;

        if (window.__req_template__) {
          const t = window.__req_template__;
          const h = new Headers(t.headers || {});
          h.set('X-Monitor','1');                                   // 只讓我們的攔截器處理
          if (token) h.set('Authorization','Bearer '+token);
          h.set('Accept','application/json, text/plain, */*');

          const method = (t.method || 'POST').toUpperCase();
          const opts = { method, headers: h, credentials:'include', cache:'no-store' };

          let body = t.body || '';
          // ① 無論 GET/POST，一律把 URL 上的查詢參數換成最新時間窗
          const urlObj = new URL(t.url, location.origin);
          const sp = urlObj.searchParams;
          // 先清後設（避免重複/殘留）
          ['startTime','endTime','page','pageIndex','pageSize','limit','rowsCount']
            .forEach(k => { if (sp.has(k)) sp.delete(k) });
          sp.set('startTime', startTime);
          sp.set('endTime',   endTime);
          sp.set('pageIndex', '1');
          sp.set('pageSize',  '{BATCH_SIZE}');
          const targetUrl = urlObj.toString();

          try {
            // JSON body
            const obj = JSON.parse(body);
            obj.startTime = startTime;
            obj.endTime = endTime;
            if ('pageIndex' in obj) obj.pageIndex = 1;
            if ('pageSize'  in obj) obj.pageSize  = {BATCH_SIZE};
            body = JSON.stringify(obj);
            h.set('Content-Type', h.get('Content-Type') || 'application/json;charset=UTF-8');
          } catch {
            // x-www-form-urlencoded
            try {
              const p = new URLSearchParams(body);
              if (p.has('startTime')) p.set('startTime', startTime);
              if (p.has('endTime'))   p.set('endTime',   endTime);
              if (p.has('pageIndex')) p.set('pageIndex', '1');
              if (p.has('pageSize'))  p.set('pageSize',  '{BATCH_SIZE}');
              body = p.toString();
              h.set('Content-Type', h.get('Content-Type') || 'application/x-www-form-urlencoded; charset=UTF-8');
            } catch {}
          }

          if (method !== 'GET') opts.body = body;
          // ② 一律以改好的 targetUrl 發請求（GET/POST 都能用）
          resp = await fetch(targetUrl, opts);
          console.debug('[POLLER] used template', method, targetUrl, '→', resp.status);
        } else {
          const body = {
            // 站方常用欄位，盡量齊
            startTime, endTime,
            game_code: 'baccarat',   // 若不需要也無妨；後端會忽略
            page: 1, pageNum: 1, pageIndex: 1,
            pageSize: {BATCH_SIZE}, limit: {BATCH_SIZE}, rowsCount: {BATCH_SIZE}
          };
          // HTTP 500 重試機制 for in-page poller
          let http500_retries = 0;
          const maxRetries = 2;

          do {
            resp = await fetch('https://i.t9live3.vip/api/game/result/search', {
              method:'POST',
              headers:(()=>{const hh=new Headers({
                'X-Monitor':'1',
                'Accept':'application/json, text/plain, */*',
                'Content-Type':'application/json;charset=UTF-8',
                'X-Requested-With':'XMLHttpRequest'
              }); if(token) hh.set('Authorization','Bearer '+token); return hh;})(),
              credentials:'include', cache:'no-store',
              body: JSON.stringify(body)
            });

            if (resp.status === 500 && http500_retries < maxRetries) {
              http500_retries++;
              const retryDelay = 800 + (http500_retries * 400); // 0.8s, 1.2s
              console.debug(`[POLLER] HTTP 500 retry ${http500_retries}/${maxRetries} after ${retryDelay}ms`);
              await sleep(retryDelay);
            } else {
              break;
            }
          } while (http500_retries <= maxRetries);

          console.debug('[POLLER] used default payload →', resp.status, http500_retries > 0 ? `(retries: ${http500_retries})` : '');
        }

        // 處理非OK狀態
        if (!resp.ok) {
          const txt = await resp.text().catch(() => '');
          console.debug('[POLLER] HTTP', resp.status, txt.slice(0, 300));
          state.consecutiveFails++;
          
          if (state.consecutiveFails >= 3) {
            console.warn('[POLLER] Too many consecutive failures, pausing...');
            await sleep(10000); // 暫停10秒
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
        
        const biz = json?.code ?? json?.data?.code;
        if (typeof biz !== 'undefined' && biz !== 200) {
          console.debug('[POLLER] api code=', biz, 'msg=', json?.msg || json?.message);
        }
        const list =
          (json?.data?.list) ||
          (json?.data?.rows) ||
          (json?.data?.data) ||   // 有些後端長這樣
          json?.records ||        // 你的 /api/latest 就是這個 key
          json?.list || [];

        const fresh = [];
        for (const r of list) {
          // 先檢查 round_id 是否夠新
          const ridNum = Number(r.round_id || r.roundId || 0) || 0;
          if (ridNum && ridNum <= lastMaxRound) continue;
          
          const k = uniqId(r);
          if (!k) continue;
          if (!state.seen.has(k)) {
            state.seen.add(k);
            fresh.push(r);
          }
        }
        
        // 更新 lastMaxRound
        if (fresh.length) {
          const newRounds = fresh.map(x => Number(x.round_id||x.roundId||0)||0).filter(n => n > 0);
          if (newRounds.length) {
            lastMaxRound = Math.max(lastMaxRound, ...newRounds);
          }
        }

        if (typeof window.ingest_results === 'function' && fresh.length > 0) {
          console.debug('[POLLER]', 'list=', list.length, 'fresh=', fresh.length);
          await window.ingest_results(fresh);
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

        # 等待輪詢器函數可用，然後啟動
        await asyncio.sleep(1)  # 等待腳本載入
        
        # ✅ 新版（fire-and-forget，不阻塞）
        await page.evaluate("""
          (opts) => {
            if (typeof window.__result_poller__ === 'function') {
              setTimeout(() => { window.__result_poller__(opts); }, 0);
              console.debug('[POLLER] fired');
            } else {
              console.error('Poller function not available');
            }
          }
        """, { "url": "https://i.t9live3.vip/api/game/result/search", "interval_ms": int(interval_ms) })

        logger.info(f"Started in-page poller with {interval_ms}ms interval")

    async def precise_search_worker(self):
        """精準搜尋工人：處理升級到精準搜尋的牌局"""
        logger.info(f"Starting precise search worker (interval: {PRECISE_WORKER_INTERVAL}s)")

        while self.is_running:
            try:
                await asyncio.sleep(PRECISE_WORKER_INTERVAL)

                if not self.page or not self.precise_queue:
                    continue

                # 取出一個待處理項目
                table, round_id = self.precise_queue.pop(0)
                logger.info(f"[PRECISE] Processing {table}:{round_id}")

                # 執行精準搜索
                success = await self.lookup_round_in_page(round_id)

                if success:
                    # 成功找到並處理，從待回填中移除
                    if round_id in self.pending[table]:
                        del self.pending[table][round_id]
                        logger.info(f"[PRECISE] Successfully found and removed {table}:{round_id}")
                else:
                    # 精準搜尋失敗，檢查是否需要重新排隊
                    if round_id in self.pending[table]:
                        meta = self.pending[table][round_id]
                        age = time.time() - meta["first_seen_ts"]

                        if age < ZOMBIE_TIME_SEC:
                            # 重新排隊，但降低優先級
                            if len(self.precise_queue) < 10:  # 避免隊列太長
                                self.precise_queue.append((table, round_id))
                                logger.debug(f"[PRECISE] Re-queued {table}:{round_id} for retry")
                        else:
                            # 超過僵屍線，放棄
                            del self.pending[table][round_id]
                            logger.info(f"[PRECISE] Dropping zombie round {table}:{round_id} (age: {age:.1f}s)")

                # 節流控制
                await asyncio.sleep(PRECISE_WORKER_INTERVAL)

            except Exception as e:
                logger.error(f"[PRECISE] Error in precise search worker: {e}")
                await asyncio.sleep(5)

    async def soft_refresh_every(self, base_seconds: int = 5):
        """
        智慧輕刷新：精準點擊上方工具列的「檢索」按鈕，避開左側選單
        增加節流控制，避免頻繁點擊「檢索」按鈕
        """
        logger.info(f"Starting smart soft refresh every ~{base_seconds}s with throttling")
        self._sr_fail = 0
        self._last_search_click = 0  # 記錄上次點擊檢索的時間
        self._min_search_interval = 8  # 最小檢索間隔（秒）
        
        while self.is_running:
            try:
                # 檢查頁籤可見性與防重入
                busy = await self.page.evaluate("""
(() => {
  // 頁籤不可見 → 跳過
  if (document.hidden) return 'HIDDEN';

  // 還在處理上一次點擊 → 跳過
  if (window.__softRefreshBusy__) return 'BUSY';

  // 標記進行中，並設一個保險超時（避免永遠卡著）
  window.__softRefreshBusy__ = true;
  window.__softRefreshStartedAt__ = Date.now();
  setTimeout(() => { window.__softRefreshBusy__ = false; }, 8000);
  return 'OK';
})()
""")
                if busy != "OK":
                    # HIDDEN 或 BUSY 都先睡一小會再回來
                    await asyncio.sleep(1.2)
                    continue
                
                # 節流控制：檢查是否距離上次點擊檢索按鈕太近
                current_time = time.time()
                time_since_last_click = current_time - self._last_search_click

                if time_since_last_click < self._min_search_interval:
                    logger.debug(f"[THROTTLE] Skipping search click, too soon ({time_since_last_click:.1f}s < {self._min_search_interval}s)")
                    # Wait for DOM to stabilize after refresh
                    await asyncio.sleep(random.uniform(0.4, 0.8))
                    continue

                clicked = False
                for frame in self.page.frames:
                    if "i.t9live3.vip" not in frame.url:
                        continue

                    # --- A. 用 Locator 精準點「檢索」 ---
                    try:
                        # 1) 首選：role=button, name=檢索/查询/搜尋/搜索
                        btn = frame.get_by_role(
                            "button",
                            name=re.compile(r"(檢索|检索|查询|查詢|搜尋|搜索|Search|Refresh)")
                        ).first
                        await btn.scroll_into_view_if_needed(timeout=1000)
                        # 避開左側選單：只點畫面中間偏右的按鈕
                        box = await btn.bounding_box()
                        if box and box["x"] > 220:    # 左邊選單大多 < 200px
                            await btn.click(timeout=1500)
                            self._last_search_click = current_time  # 更新上次點擊時間
                            logger.info(f"[SOFT_REFRESH] Locator clicked: 檢索 ({frame.url})")
                            clicked = True
                        else:
                            raise Exception("button too left")
                    except Exception:
                        # 2) 備援：找包含「檢索」文本的可點元素，且限制在上方工具列區域
                        try:
                            await frame.evaluate("""
                            (() => {
                              const byText = el => (el.innerText||el.textContent||'').trim();
                              const kw = /檢索|检索|查询|查詢|搜尋|搜索|Search|Refresh/;
                              const regionTop = 80, regionBottom = 220, regionLeft = 220; // 上方工具列大致區域
                              const cand = Array.from(document.querySelectorAll('button,[role="button"],.btn,.el-button,.ant-btn,a.button,a[role="button"]'))
                                .filter(el => {
                                  const r = el.getBoundingClientRect();
                                  return kw.test(byText(el)) && r.top>=regionTop && r.bottom<=regionBottom && r.left>=regionLeft;
                                });
                              const target = cand[0];
                              if (target){
                                target.click?.();
                                target.dispatchEvent?.(new MouseEvent('click',{bubbles:true,cancelable:true}));
                                console.debug('[SOFT_REFRESH] Clicked: 檢索 (fallback)');
                                return true;
                              }
                              return false;
                            })()
                            """)
                            self._last_search_click = current_time  # 更新上次點擊時間
                            clicked = True
                        except Exception:
                            pass

                    if not clicked:
                        continue

                    # B) 等 2 秒看是否有頁面自己發出的 XHR；沒有就做一次 fallback
                    xhr_observed = False
                    try:
                        await self.page.wait_for_response(
                            lambda r: ("i.t9live3.vip/api/game/result/search" in r.url)
                                      and (getattr(r.request, "resource_type", "") in ("xhr","fetch")),
                            timeout=2000
                        )
                        logger.info("[SOFT_REFRESH] Observed page XHR")
                        xhr_observed = True
                        self._sr_fail = 0
                    except Exception:
                        logger.warning("[SOFT_REFRESH] No XHR observed — falling back to direct pull")
                        
                        # 直接拉取作為 fallback
                        try:
                            data = await self.make_api_request()
                            if data and not data.get("_err"):
                                records = self.extract_records(data)
                                if records:
                                    # 去重 + 只取前 BATCH_SIZE 筆
                                    new_records = self._dedupe_new(records)[:BATCH_SIZE]
                                    if new_records:
                                        logger.info(f"[FALLBACK] pulled {len(new_records)} new records")
                                        success = await self.send_to_ingest(new_records)
                                        if success:
                                            self.stats["records_processed"] += len(new_records)
                                            self._sr_fail = 0
                                        else:
                                            self._sr_fail += 1
                                    else:
                                        logger.debug("[FALLBACK] no new records after dedup")
                                        self._sr_fail = 0  # 雖然沒新資料，但請求本身成功
                                else:
                                    self._sr_fail += 1
                            else:
                                self._sr_fail += 1
                        except Exception as fallback_error:
                            logger.debug(f"[FALLBACK] error: {fallback_error}")
                            self._sr_fail += 1
                    
                    # Wait for DOM to stabilize after refresh
                    await asyncio.sleep(random.uniform(0.4, 0.8))

                    break  # 這一輪已經成功處理；跳出 frame 迴圈

            except Exception as e:
                logger.debug(f"Smart refresh error: {e}")
                self._sr_fail += 1
            finally:
                # 清除防重入旗標
                try:
                    await self.page.evaluate("window.__softRefreshBusy__ = false")
                except:
                    pass

            # 帶一點抖動，避免節律痕跡，並加入退避邏輯
            base = base_seconds + random.uniform(0.4, 1.0)
            if self._sr_fail >= 3:
                base += random.uniform(10, 20)  # 退避一會，給站方喘息
            await asyncio.sleep(base)
                
    async def _handle_consecutive_failures(self):
        """处理连续101错误的自我修复"""
        self.consecutive_101_count += 1
        
        if self.consecutive_101_count == 1:
            # 第1次101 → 輕度刷新
            logger.warning(f"First 101 error, trying light refresh...")
            try:
                await self.page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Light refresh failed: {e}")
        elif self.consecutive_101_count == 2:
            # 连2次101 → 刷新页面
            logger.warning(f"Consecutive 101 errors: {self.consecutive_101_count}. Refreshing page...")
            try:
                await self.page.goto('https://i.t9live3.vip/', wait_until='domcontentloaded')
                await asyncio.sleep(3)
                await self.page.goto('https://i.t9live3.vip/#/gameResult', wait_until='networkidle')
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Page refresh failed: {e}")
        elif self.consecutive_101_count >= 3:
            # 连3次 → 暂停30-60s
            wait_time = 30 + random.uniform(0, 30)
            logger.warning(f"Consecutive 101 errors: {self.consecutive_101_count}. Pausing for {wait_time:.1f}s...")
            await asyncio.sleep(wait_time)
    
    async def start(self):
        """启动监控器"""
        logger.info("Starting Existing Browser Monitor...")
        if not await self.connect_to_existing_browser():
            raise RuntimeError("Failed to connect to existing browser")

        self.is_running = True
        
        if ACTIVE_PULL:
            try:
                ok = await self.discover_and_arm_template(timeout_ms=20000)
                logger.info(f"Template armed: {ok}")
            except Exception as e:
                logger.warning(f"Template discovery skipped: {e}")
            await self.start_poller(interval_ms=1000)
        else:
            logger.info("ACTIVE_PULL=False → 跳過模板偵測與 in-page poller（改走 soft-refresh-only）")

        # === 啟動三任務：軟刷新 + 滑動帶檢測 + 精準搜尋工人 ===
        soft_refresh_task = asyncio.create_task(self.soft_refresh_every(int(SOFT_REFRESH_SEC)))
        precise_worker_task = asyncio.create_task(self.precise_search_worker())

        logger.info("Started sliding band backfill system with precise search worker")

        try:
            # 簡單保活即可（主要工作由滑動帶檢測在鏡射中完成）
            while self.is_running:
                await asyncio.sleep(60)
        finally:
            soft_refresh_task.cancel()
            precise_worker_task.cancel()
            try:
                await soft_refresh_task
            except asyncio.CancelledError:
                pass
            try:
                await precise_worker_task
            except asyncio.CancelledError:
                pass
            await self.cleanup()
    
    async def stop(self):
        """停止监控器"""
        logger.info("Stopping monitor...")
        self.is_running = False
    
    async def cleanup(self):
        """清理资源"""
        try:
            # 不要关闭browser，因为那是用户的浏览器
            if self.playwright:
                await self.playwright.stop()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        uptime = time.time() - self.stats["start_time"]
        success_rate = (
            self.stats["successful_requests"] / max(1, self.stats["total_requests"]) * 100
        )
        
        # 統計滑動帶回填狀態
        total_pending = sum(len(rounds) for rounds in self.pending.values())
        pending_by_table = {table: len(rounds) for table, rounds in self.pending.items() if rounds}
        precise_queue_size = len(self.precise_queue)

        # 統計各狀態的牌局數量
        pending_stats = {"新發現": 0, "追蹤中": 0, "已升級": 0}
        now = time.time()

        for table, rounds in self.pending.items():
            for round_id, meta in rounds.items():
                age = now - meta["first_seen_ts"]
                if meta["stale"]:
                    pending_stats["已升級"] += 1
                elif age > UPGRADE_TIME_SEC or meta["attempts"] >= UPGRADE_ATTEMPTS:
                    pending_stats["追蹤中"] += 1
                else:
                    pending_stats["新發現"] += 1

        return {
            **self.stats,
            "uptime_seconds": uptime,
            "success_rate_percent": round(success_rate, 2),
            "is_running": self.is_running,
            "sliding_band_stats": {
                "total_pending_rounds": total_pending,
                "pending_by_table": pending_by_table,
                "pending_by_status": pending_stats,
                "precise_queue_size": precise_queue_size,
                "scan_results_size": len(self.current_scan_results),
                "config": {
                    "scan_all_rows": SCAN_ALL_ROWS,
                    "search_band": SEARCH_BAND,
                    "upgrade_time_sec": UPGRADE_TIME_SEC,
                    "upgrade_attempts": UPGRADE_ATTEMPTS,
                    "soft_refresh_sec": SOFT_REFRESH_SEC,
                    "precise_worker_interval": PRECISE_WORKER_INTERVAL
                }
            }
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

if __name__ == "__main__":
    asyncio.run(main())