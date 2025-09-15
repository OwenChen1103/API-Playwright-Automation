#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
T9 Live Streaming Server - FastAPI 後端服務
提供 SSE 即時推送、資料攝取與監控 API
"""

import asyncio
import json
import time
import logging
import os
import hashlib
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import redis
from redis.exceptions import RedisError

# 載入環境變數
load_dotenv()

# 匯入核心模組（依你專案結構）
from core.event_system import get_global_publisher, EventType
from core.metrics import get_global_metrics, get_global_watchdog
from streaming.sse_server import get_global_sse_server

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("t9-server")

# 建立 FastAPI 應用
app = FastAPI(
    title="T9 Live Streaming API",
    description="T9 Live 即時資料流推送服務",
    version="2.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全域資料
latest_records: List[Dict[str, Any]] = []
# Upsert-based storage: table -> round_id(int) -> record(dict)
results_by_table: Dict[str, Dict[int, Dict[str, Any]]] = {}
# 狀態索引：key(table:round) -> {"phase": int, "hash": str, "last_update": ts}
state_index: Dict[str, Dict[str, Any]] = {}

metrics = get_global_metrics()
watchdog = get_global_watchdog()
event_publisher = get_global_publisher()
sse_server = get_global_sse_server()

# Redis 連線（可選）
redis_client: Optional[redis.Redis] = None
USE_REDIS = os.getenv("USE_REDIS", "false").lower() == "true"


def init_redis() -> None:
    """初始化 Redis 連線（若啟用）"""
    global redis_client
    if not USE_REDIS:
        return

    try:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        redis_db = int(os.getenv("REDIS_DB", "0"))
        redis_password = os.getenv("REDIS_PASSWORD", None)

        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={}
        )

        # 測試連線
        redis_client.ping()
        logger.info(f"Redis connected: {redis_host}:{redis_port}")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}, falling back to memory")
        redis_client = None


def get_state_from_store(key: str) -> Optional[Dict[str, Any]]:
    """從儲存中取得狀態"""
    if redis_client:
        try:
            data = redis_client.get(f"state:{key}")
            return json.loads(data) if data else None
        except RedisError as e:
            logger.warning(f"Redis get failed: {e}")
            return state_index.get(key)
    else:
        return state_index.get(key)


def set_state_to_store(key: str, state_data: Dict[str, Any]) -> None:
    """將狀態寫入儲存"""
    if redis_client:
        try:
            # 24 小時過期
            redis_client.setex(f"state:{key}", 86400, json.dumps(state_data))
        except RedisError as e:
            logger.warning(f"Redis set failed: {e}")
            state_index[key] = state_data
    else:
        state_index[key] = state_data


def extract_table_round_key(record: Dict[str, Any]) -> (Optional[str], Optional[int]):
    """Extract (table_id, round_id[int]) from record"""
    table_id = record.get("table_id") or record.get("tableId") or record.get("table")
    round_id = record.get("round_id") or record.get("roundId")

    if table_id and round_id:
        try:
            round_id_int = int(round_id)
            return str(table_id), round_id_int
        except (ValueError, TypeError):
            logger.debug(f"Invalid round_id format: {round_id}")
            return None, None
    return None, None


def upsert_record(record: Dict[str, Any]) -> bool:
    """Upsert record using (table_id, round_id) as primary key"""
    table_id, round_id = extract_table_round_key(record)
    if not table_id or round_id is None:
        return False

    if table_id not in results_by_table:
        results_by_table[table_id] = {}

    # 加上接收時間戳
    record["ingested_at"] = datetime.now().isoformat()

    # Upsert
    results_by_table[table_id][round_id] = record

    # 調試輸出
    try:
        status_name = record.get("game_payment_status_name", "unknown")
        result_code = (record.get("gameResult") or {}).get("result", -1) if isinstance(record.get("gameResult"), dict) else -1
        logger.debug(f"[UPSERT] {table_id}:{round_id} -> {status_name} (result: {result_code})")
    except Exception:
        pass

    return True


def get_latest_records_upsert(limit: int = 30) -> List[Dict[str, Any]]:
    """Get latest records from upsert storage, sorted by round_id (descending)"""
    all_records: List[Dict[str, Any]] = []

    # 收集全部桌台的資料
    for _, rounds in results_by_table.items():
        for _, rec in rounds.items():
            all_records.append(rec)

    # 依 round_id 由大到小（最新在前）
    all_records.sort(key=lambda r: int(r.get("round_id") or r.get("roundId") or 0), reverse=True)
    return all_records[:limit]


def get_table_records_upsert(table_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Get records for specific table, sorted by round_id (descending)"""
    if table_id not in results_by_table:
        return []

    table_rounds = results_by_table[table_id]
    # 依 round_id 由大到小
    sorted_rounds = sorted(table_rounds.items(), key=lambda x: x[0], reverse=True)
    return [rec for _, rec in sorted_rounds[:limit]]


# 狀態分類與去重相關
def classify_phase(record: Dict[str, Any]) -> int:
    """
    分類遊戲階段：
      1 = 投注中
      2 = 停止下注/派彩中
      3 = 已有最終結果
    """
    game_result = record.get("gameResult") or {}
    result = game_result.get("result", -1) if isinstance(game_result, dict) else -1
    status = (record.get("game_payment_status_name") or "").strip()

    # 修正可能的編碼問題並正規化
    try:
        if isinstance(status, bytes):
            status = status.decode("utf-8", errors="replace")
        import unicodedata
        status = unicodedata.normalize("NFKC", status)
    except Exception as e:
        logger.debug(f"Status normalization error: {e}, status: {repr(status)}")

    # 階段 3：有最終結果（包含取消/無效）
    if result in (0, 1, 2, 3):
        return 3

    # 階段 2：停止下注/派彩中/結算中（涵蓋多種字樣）
    phase_2_keywords = [
        "停止", "派彩", "結束", "完成", "開獎", "開局", "結算", "结算",
        "stop", "payout", "finish", "complete", "settle"
    ]
    if any(kw in status for kw in phase_2_keywords):
        return 2

    # 預設：投注中
    return 1


def important_hash(record: Dict[str, Any]) -> str:
    """計算重要欄位的哈希值，避免時間戳造成雜訊"""
    status = record.get("game_payment_status_name") or ""
    try:
        if isinstance(status, bytes):
            status = status.decode("utf-8", errors="replace")
        import unicodedata
        status = unicodedata.normalize("NFKC", status).strip()
    except Exception:
        status = str(status).strip()

    subset = {
        "table_id": record.get("table_id") or record.get("tableId") or record.get("table"),
        "round_id": record.get("round_id") or record.get("roundId"),
        "status": status,
        "result": (record.get("gameResult") or {}).get("result")
        if isinstance(record.get("gameResult"), dict) else None,
    }

    json_str = json.dumps(subset, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(json_str.encode("utf-8")).hexdigest()


def should_accept_record(record: Dict[str, Any]) -> bool:
    """決定是否接受這筆記錄（用於 SSE 去重/狀態推進判斷）"""
    table_id = record.get("table_id") or record.get("tableId") or record.get("table")
    round_id = record.get("round_id") or record.get("roundId")

    # 若缺 table/round，直接接受（作為寬鬆策略）
    if not (table_id and round_id):
        return True

    key = f"{table_id}:{round_id}"
    phase = classify_phase(record)
    record_hash = important_hash(record)

    prev = get_state_from_store(key)
    if not prev:
        # 新記錄，接受並記錄狀態
        set_state_to_store(
            key,
            {"phase": phase, "hash": record_hash, "last_update": datetime.now().isoformat()},
        )
        return True

    # 防止階段回退
    if phase < prev["phase"]:
        logger.debug(f"Phase regression detected for {key}: {phase} < {prev['phase']}, rejecting")
        return False

    # 同階段且內容相同 → 視為重複
    if phase == prev["phase"] and record_hash == prev["hash"]:
        logger.debug(f"Duplicate content detected for {key}: same phase {phase} and hash, rejecting")
        return False

    # 階段推進或同階段但內容有變 → 接受並更新狀態
    set_state_to_store(
        key,
        {"phase": phase, "hash": record_hash, "last_update": datetime.now().isoformat()},
    )
    logger.debug(f"Record accepted for {key}: phase {prev['phase']} -> {phase}")
    return True


# Pydantic models
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
        "timestamp": datetime.now().isoformat(),
    }


# 健康檢查
@app.get("/health")
async def health_check():
    """健康檢查端點"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "metrics": metrics.get_comprehensive_stats(),
    }


# SSE 流式推送
@app.get("/api/stream")
async def stream_events(
    request: Request,
    event_types: str = Query("result,error,heartbeat", description="事件類型過濾"),
    job_id: Optional[str] = Query(None, description="任務 ID 過濾"),
):
    """SSE 事件流端點"""
    parsed_event_types = event_types.split(",") if event_types else None
    return await sse_server.create_event_stream(
        request=request, event_types=parsed_event_types, job_id=job_id
    )


# WebSocket 端點（心跳）
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 心跳端點"""
    await websocket.accept()
    try:
        while True:
            await websocket.send_text(
                json.dumps({"type": "heartbeat", "timestamp": datetime.now().isoformat()})
            )
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")


# 資料攝取端點
@app.post("/ingest", response_model=IngestResponse)
async def ingest_data(
    request: IngestRequest, x_ingest_key: Optional[str] = Header(None, alias="x-ingest-key")
):
    """資料攝取端點"""
    expected_key = os.getenv("INGEST_KEY", "baccaratt9webapi")
    logger.info(f"Received x_ingest_key: '{x_ingest_key}', expected: '{expected_key}'")
    if x_ingest_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid ingest key")

    try:
        processed_count = 0
        accepted_count = 0
        upserted_count = 0

        for record in request.records:
            # 去重/狀態判斷（只影響是否推送 SSE）
            should_send_event = should_accept_record(record)

            # Upsert（永遠做，用於狀態管理與查詢）
            if upsert_record(record):
                upserted_count += 1

                # 兼容舊的 latest_records（僅保留近 1000 筆）
                global latest_records
                table_id, round_id = extract_table_round_key(record)
                if table_id and round_id is not None:
                    latest_records = [
                        r
                        for r in latest_records
                        if not (
                            (r.get("table_id") or r.get("tableId") or r.get("table")) == table_id
                            and int(r.get("round_id") or r.get("roundId") or 0) == round_id
                        )
                    ]
                    latest_records.insert(0, record)
                    if len(latest_records) > 1000:
                        latest_records = latest_records[:1000]

                # 只有在 should_send_event 時才發 SSE
                if should_send_event:
                    await sse_server.publish_result(
                        data={"record": record, "source": "ingest"},
                        jwt_version="unknown",
                        latency_ms=0,
                    )
                accepted_count += 1

            processed_count += 1

        metrics.increment_counter("records_ingested", processed_count)
        metrics.increment_counter("records_accepted", accepted_count)
        metrics.increment_counter("records_upserted", upserted_count)

        return IngestResponse(
            success=True,
            message=(
                f"Successfully processed {processed_count} records, "
                f"accepted {accepted_count} events, upserted {upserted_count} records"
            ),
            processed_count=accepted_count,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ingest error: {e}")
        metrics.record_error("ingest_error", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# 取得最新記錄（依 round_id 由大到小）
@app.get("/api/latest")
async def get_latest_records(limit: int = Query(30, ge=1, le=1000)):
    """取得最新記錄（upsert 儲存）"""
    records = get_latest_records_upsert(limit)
    return {
        "records": records,
        "total": sum(len(rounds) for rounds in results_by_table.values()),
        "timestamp": datetime.now().isoformat(),
    }


# 取得所有桌號（由 latest_records 統計）
@app.get("/api/tables")
async def get_table_list():
    """取得所有桌號列表"""
    tables = set()
    for record in latest_records:
        table_id = record.get("table_id") or record.get("tableId") or record.get("table")
        if table_id:
            tables.add(str(table_id))
    return {
        "tables": sorted(list(tables)),
        "count": len(tables),
        "timestamp": datetime.now().isoformat(),
    }


# 取得所有桌號摘要
@app.get("/api/tables/summary")
async def get_tables_summary():
    """取得所有桌號的摘要資訊"""
    tables_data: Dict[str, Dict[str, Any]] = {}

    for record in latest_records:
        table_id = record.get("table_id") or record.get("tableId") or record.get("table")
        if not table_id:
            continue
        table_id = str(table_id)

        if table_id not in tables_data:
            tables_data[table_id] = {
                "table_id": table_id,
                "total_games": 0,
                "latest_record": None,
                "status_counts": {},
                "last_update": None,
            }

        # 更新計數
        tables_data[table_id]["total_games"] += 1

        # 狀態統計
        status = record.get("game_payment_status_name", "未知")
        tables_data[table_id]["status_counts"].setdefault(status, 0)
        tables_data[table_id]["status_counts"][status] += 1

        # 更新最新記錄（依 ingested_at）
        current_time = record.get("ingested_at", "")
        if not tables_data[table_id]["latest_record"] or current_time > tables_data[table_id]["last_update"]:
            tables_data[table_id]["latest_record"] = record
            tables_data[table_id]["last_update"] = current_time

    # 為每個桌子補充摘要
    for data in tables_data.values():
        latest_record = data["latest_record"]
        if latest_record:
            # 最新狀態
            data["current_status"] = latest_record.get("game_payment_status_name", "未知")

            # 最新遊戲結果
            game_result = latest_record.get("gameResult", {})
            if isinstance(game_result, dict):
                result_code = game_result.get("result", -1)
                data["latest_result"] = (
                    "閒勝" if result_code == 1 else
                    "和局" if result_code == 2 else
                    "莊勝" if result_code == 0 else
                    "取消/無效" if result_code == 3 else
                    "進行中" if result_code == -1 else
                    f"結果{result_code}"
                )
            else:
                data["latest_result"] = "N/A"

            # 最新局號 & 開局時間
            data["latest_round_id"] = latest_record.get("round_id") or latest_record.get("roundId") or "N/A"
            data["game_start_time"] = latest_record.get("game_start_time", "N/A")
        else:
            data["current_status"] = "無資料"
            data["latest_result"] = "N/A"
            data["latest_round_id"] = "N/A"
            data["game_start_time"] = "N/A"

    sorted_tables = sorted(tables_data.values(), key=lambda x: x["table_id"])
    return {
        "tables": sorted_tables,
        "total_tables": len(tables_data),
        "timestamp": datetime.now().isoformat(),
    }


# 取得指定桌號的記錄（upsert 儲存）
@app.get("/api/tables/{table_id}")
async def get_table_records(
    table_id: str,
    limit: int = Query(50, ge=1, le=1000),
    include_all_status: bool = Query(True, description="是否包含所有狀態的遊戲"),
):
    """取得指定桌號的記錄（upsert 儲存）"""
    table_records = get_table_records_upsert(table_id, limit)
    limited_records = table_records

    # 狀態統計
    status_stats: Dict[str, int] = {}
    for record in table_records:
        status = record.get("game_payment_status_name", "未知")
        status_stats[status] = status_stats.get(status, 0) + 1

    # 最新狀態與結果
    latest_status = "無資料"
    latest_game_result = "N/A"
    if limited_records:
        latest_record = limited_records[0]
        latest_status = latest_record.get("game_payment_status_name", "未知")
        game_result = latest_record.get("gameResult", {})
        if isinstance(game_result, dict):
            result_code = game_result.get("result", -1)
            latest_game_result = (
                "閒勝" if result_code == 1 else
                "和局" if result_code == 2 else
                "莊勝" if result_code == 0 else
                "取消/無效" if result_code == 3 else
                "進行中" if result_code == -1 else
                f"結果{result_code}"
            )

    return {
        "table_id": table_id,
        "records": limited_records,
        "total": len(table_records),
        "latest_status": latest_status,
        "latest_game_result": latest_game_result,
        "status_stats": status_stats,
        "timestamp": datetime.now().isoformat(),
    }


# 系統指標
@app.get("/api/metrics")
async def get_metrics():
    """取得系統指標"""
    redis_status = "disabled"
    if USE_REDIS:
        if redis_client:
            try:
                redis_client.ping()
                redis_status = "connected"
            except Exception:
                redis_status = "disconnected"
        else:
            redis_status = "failed"

    return {
        "metrics": metrics.get_comprehensive_stats(),
        "watchdog": watchdog.get_stats(),
        "redis_status": redis_status,
        "use_redis": USE_REDIS,
        "state_index_size": len(state_index),
        "timestamp": datetime.now().isoformat(),
    }


# 事件統計
@app.get("/api/events/stats")
async def get_event_stats():
    """取得事件統計"""
    return {
        "event_stats": sse_server.get_stats(),
        "timestamp": datetime.now().isoformat(),
    }


# 啟動事件
@app.on_event("startup")
async def startup_event():
    """應用啟動事件"""
    logger.info("T9 Live Streaming Server starting up...")
    init_redis()
    logger.info("SSE server initialized")
    logger.info("Server startup completed")


# 關閉事件
@app.on_event("shutdown")
async def shutdown_event():
    """應用關閉事件"""
    logger.info("T9 Live Streaming Server shutting down...")

    if redis_client:
        try:
            redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.warning(f"Error closing Redis: {e}")

    await sse_server.shutdown()
    logger.info("Server shutdown completed")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
