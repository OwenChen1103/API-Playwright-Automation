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
from typing import Optional, Dict, Any, List

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
ACTIVE_PULL = False  # 先關掉主動 make_api_request

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
        
        logger.info("Existing Browser Monitor initialized")
    
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
                    self.req_template = {
                        "method": request.method,
                        "url": url,
                        "headers": dict(request.headers),
                        "body": body
                    }
                    logger.info(f"[TEMPLATE] captured {request.method} {url}")
                    await page.evaluate("window.__req_template__ = arguments[0]", self.req_template)
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
                    data = await response.json()
                    # 提取記錄並處理
                    records = self.extract_records(data)
                    if records:
                        logger.info(f"[MIRROR] got {len(records)} records from passive monitoring")
                        # 直接發送到 ingest
                        success = await self.send_to_ingest(records)
                        if success:
                            self.stats["records_processed"] += len(records)
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
                    # 只處理我們自己發的（poller 會加這個 header）
                    if request.headers.get("x-monitor", "") != "1":
                        await route.continue_()
                        return

                    headers = dict(request.headers)
                    headers.pop("authorization", None)
                    headers.pop("Authorization", None)

                    # 每次現讀頁面 JWT
                    jwt = await page.evaluate("() => window._jwt_current || ''")
                    if jwt:
                        headers["Authorization"] = f"Bearer {jwt}"

                    # 補 Referer/Origin，禁快取
                    try:
                        headers.setdefault("Referer", await page.evaluate("() => location.href"))
                        headers.setdefault("Origin", await page.evaluate("() => location.origin"))
                    except Exception:
                        pass
                    headers["Pragma"] = "no-cache"
                    headers["Cache-Control"] = "no-store"

                    await route.continue_(headers=headers)
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
            start_time = (now - datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
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
              return {{ success: res.ok, status: res.status, data, jwt_used: jwt.substring(0, 20) }};
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
            
            # 尝试多种数据结构
            if 'data' in data and isinstance(data['data'], dict):
                d = data['data']
                if isinstance(d.get('rows'), list):
                    return d['rows']
                if isinstance(d.get('list'), list):
                    return d['list']
                if isinstance(d.get('data'), list):
                    return d['data']
            
            if isinstance(data.get('data'), list):
                return data['data']
            
            # 通用提取
            for key in ("records", "items", "list", "result", "rows"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
            
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
        const startTime = ts(new Date(now.getTime() - 5*60*1000)); // 近 5 分鐘

        const token = (typeof window._jwt_current === 'string' && window._jwt_current) ? window._jwt_current : '';
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
          sp.set('pageSize',  '100');
          const targetUrl = urlObj.toString();

          try {
            // JSON body
            const obj = JSON.parse(body);
            obj.startTime = startTime;
            obj.endTime = endTime;
            if ('pageIndex' in obj) obj.pageIndex = 1;
            if ('pageSize'  in obj) obj.pageSize  = 100;
            body = JSON.stringify(obj);
            h.set('Content-Type', h.get('Content-Type') || 'application/json;charset=UTF-8');
          } catch {
            // x-www-form-urlencoded
            try {
              const p = new URLSearchParams(body);
              if (p.has('startTime')) p.set('startTime', startTime);
              if (p.has('endTime'))   p.set('endTime',   endTime);
              if (p.has('pageIndex')) p.set('pageIndex', '1');
              if (p.has('pageSize'))  p.set('pageSize',  '100');
              body = p.toString();
              h.set('Content-Type', h.get('Content-Type') || 'application/x-www-form-urlencoded; charset=UTF-8');
            } catch {}
          }

          if (method !== 'GET') opts.body = body;
          // ② 一律以改好的 targetUrl 發請求（GET/POST 都能用）
          resp = await fetch(targetUrl, opts);
          console.debug('[POLLER] used template', method, targetUrl, '→', resp.status);
        } else {
          // 沒模板就先略（或用你原本的保守 payload）
          resp = await fetch('https://i.t9live3.vip/api/game/result/search', {
            method:'POST',
            headers: (()=>{ const hh=new Headers({'X-Monitor':'1','Accept':'application/json, text/plain, */*','Content-Type':'application/json;charset=UTF-8'}); if(token) hh.set('Authorization','Bearer '+token); return hh; })(),
            credentials:'include', cache:'no-store',
            body: JSON.stringify({ startTime, endTime, pageIndex:1, pageSize:100 })
          });
          console.debug('[POLLER] used default payload →', resp.status);   // ← 新增這行
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

    async def soft_refresh_every(self, seconds: int = 8):
        """
        智慧輕刷新：模擬人真的按了「查詢/刷新」，讓站方的 JS 把快照重建、再發出 XHR
        現在針對所有同源 iframe 進行嘗試
        """
        logger.info(f"Starting smart soft refresh every {seconds} seconds")
        while self.is_running:
            try:
                logger.debug(f"[SOFT_REFRESH] Starting refresh cycle, is_running={self.is_running}")
                
                # 針對主頁面和所有同源 iframe 都執行軟刷新
                if not self.page:
                    logger.warning(f"[SOFT_REFRESH] Page is None, skipping refresh cycle")
                    await asyncio.sleep(seconds)
                    continue
                    
                frames = self.page.frames
                logger.debug(f"[SOFT_REFRESH] Found {len(frames)} frames to check")
                
                for i, frame in enumerate(frames):
                    try:
                        frame_url = frame.url
                        logger.debug(f"[SOFT_REFRESH] Checking frame {i}: {frame_url}")
                        
                        if not frame_url or "t9live3.vip" not in frame_url:
                            logger.debug(f"[SOFT_REFRESH] Skipping frame {i} - not t9live3 domain")
                            continue
                        
                        success = await frame.evaluate("""
                        (() => {
                          const T_NOW = Date.now();
                          const byText = (el) => (el.innerText || el.textContent || "").trim();

                          // 深度遍歷 (含 shadowRoot)
                          const all = [];
                          const walk = (root) => {
                            const iter = document.createNodeIterator(root, NodeFilter.SHOW_ELEMENT);
                            for (let n = iter.nextNode(); n; n = iter.nextNode()) {
                              all.push(n);
                              if (n.shadowRoot) walk(n.shadowRoot);
                            }
                            // 也掃一次常見的 open shadow 裏的 button
                            (root.querySelectorAll?.('slot') || []).forEach(slot => {
                              slot.assignedElements?.().forEach(el => { all.push(el); if (el.shadowRoot) walk(el.shadowRoot); });
                            });
                          };
                          walk(document);

                          const click = (el, why) => {
                            try {
                              el.click?.(); // 先用原生 click（許多框架綁 onClick）
                              el.dispatchEvent?.(new MouseEvent('click', {bubbles:true, cancelable:true}));
                              console.debug('[SOFT_REFRESH] Clicked:', why, '→', (byText(el) || el.className || el.id || el.tagName));
                              return true;
                            } catch (e) {
                              console.debug('[SOFT_REFRESH] click failed:', e.message);
                              return false;
                            }
                          };

                          const kw = /刷新|查詢|查询|搜尋|搜索|查找|更新|重新整理|Search|Refresh|Reload|Update/;

                          // 1) 直接看文字命中
                          let btn = all.find(el =>
                            (el.matches?.('button,[role="button"],.el-button,.ant-btn,.btn,a[role="button"],a.button')) &&
                            kw.test(byText(el))
                          );
                          if (btn && click(btn, 'byText')) return true;

                          // 2) 常見 UI 類名/圖示（Element/Ant/自定義 icon）
                          btn = all.find(el =>
                            el.matches?.('[class*="refresh"],[class*="reload"],[class*="search"],[data-action*="refresh"],[data-action*="search"],.el-button--primary,.ant-btn-primary')
                          );
                          if (btn && click(btn, 'byClass')) return true;

                          // 3) 沒命中就 bump hash 促使路由重取
                          try {
                            const hash = (location.hash || '#/').split('?')[0];
                            const p = new URLSearchParams((location.hash.split('?')[1]||''));
                            p.set('_ts', String(T_NOW));
                            const nh = hash + '?' + p.toString();
                            if (nh !== location.hash) { location.hash = nh; console.debug('[SOFT_REFRESH] Bumped hash'); return true; }
                          } catch (e) {}

                          // 4) 實在沒招，發自訂事件
                          try { (document.querySelector('#app') || document.body)
                                .dispatchEvent(new Event('force-fetch', {bubbles:true})); 
                                console.debug('[SOFT_REFRESH] Dispatched force-fetch'); 
                                return true; } catch(e){}

                          console.debug('[SOFT_REFRESH] No trigger');
                          return false;
                        })();
                        """)
                        
                        if success:
                            logger.info(f"[SOFT_REFRESH] Triggered successfully in frame: {frame_url}")

                            # 1) 先給頁面 3 秒時間，看它會不會真的發出 XHR
                            try:
                                await self.page.wait_for_response(
                                    lambda r: ("i.t9live3.vip/api/game/result/search" in r.url) and
                                              (getattr(r.request, "resource_type", "") in ("xhr", "fetch")),
                                    timeout=3_000
                                )
                                logger.info("[SOFT_REFRESH] Observed search XHR from page")
                            except Exception:
                                # 2) 如果 3 秒內完全沒有 XHR，就直接由我們自己拉一次（不等下一輪）
                                logger.warning("[SOFT_REFRESH] No XHR observed — falling back to direct pull")
                                try:
                                    data = await self.make_api_request()                # 直接在頁內 fetch
                                    if data and not data.get("_err"):
                                        records = self.extract_records(data)
                                        if records:
                                            ok = await self.send_to_ingest(records)
                                            if ok:
                                                self.stats["records_processed"] += len(records)
                                                logger.info(f"[FALLBACK] pulled {len(records)} records via make_api_request()")
                                except Exception as e:
                                    logger.error(f"[FALLBACK] direct pull failed: {e}")

                            # 3) 最後再催一次 poller（你已有 window.__poller_force_now__）
                            try:
                                await self.page.evaluate("window.__poller_force_now__ && window.__poller_force_now__()")
                            except Exception:
                                pass

                            break
                        else:
                            logger.debug(f"[SOFT_REFRESH] No suitable trigger found in frame: {frame_url}")
                            
                    except Exception as e:
                        logger.debug(f"[SOFT_REFRESH] Error in frame {frame.url}: {e}")
                        continue
                
                logger.debug(f"[SOFT_REFRESH] Refresh cycle complete, sleeping for {seconds} seconds")
                    
            except Exception as e:
                logger.error(f"[SOFT_REFRESH] General error: {e}")
                
            await asyncio.sleep(seconds)
                
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
        try:
            logger.info("Starting Existing Browser Monitor...")
            
            # 连接到现有浏览器
            if not await self.connect_to_existing_browser():
                raise RuntimeError("Failed to connect to existing browser")
            
            # 等待JWT準備好再啟動輪詢器（最長15秒）
            try:
                await self.page.wait_for_function(
                    "window._jwt_current && window._jwt_current.length > 10",
                    timeout=15_000
                )
                logger.info("JWT is ready, starting poller...")
            except Exception as e:
                logger.warning(f"JWT wait timeout: {e}. Starting poller anyway...")
            
            # 自動發現並捕獲模板（best-effort，不讓它阻斷啟動）
            try:
                ok = await self.discover_and_arm_template(timeout_ms=20000)
                logger.info(f"Template armed: {ok}")
            except Exception as e:
                logger.warning(f"Template discovery skipped: {e}")
            
            # ✅ 先標記正在跑
            self.is_running = True
            
            # 启动頁內輪詢器
            await self.start_poller(interval_ms=1000)
            
            # 啟動軟刷新（可選，背景執行）
            soft_refresh_task = asyncio.create_task(self.soft_refresh_every(8))
            
            try:
                await self.monitor_loop()
            finally:
                # 取消軟刷新任務
                soft_refresh_task.cancel()
                try:
                    await soft_refresh_task
                except asyncio.CancelledError:
                    pass
            
        except Exception as e:
            logger.error(f"Monitor startup failed: {e}")
            raise
        finally:
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
        
        return {
            **self.stats,
            "uptime_seconds": uptime,
            "success_rate_percent": round(success_rate, 2),
            "is_running": self.is_running
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