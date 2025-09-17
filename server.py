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
import io
import csv
from typing import List, Optional, Dict, Any
from datetime import datetime, date

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Query, Header, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, Response
from pydantic import BaseModel
from dotenv import load_dotenv
import redis
from redis.exceptions import RedisError
import pandas as pd

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
            logger.warning(f"Invalid round_id format: {round_id} for table: {table_id}")
            return None, None

    if not table_id:
        logger.warning(f"Missing table_id in record: {record.keys()}")
    if not round_id:
        logger.warning(f"Missing round_id in record for table {table_id}")

    return None, None


def check_data_integrity():
    """檢查 results_by_table 的資料完整性"""
    total_records = 0
    for table_id, rounds in results_by_table.items():
        if not isinstance(rounds, dict):
            logger.error(f"[INTEGRITY] Table {table_id} has invalid rounds type: {type(rounds)}")
            return False

        table_count = len(rounds)
        total_records += table_count

        # 檢查 round_id 類型
        for round_id in rounds.keys():
            if not isinstance(round_id, int):
                logger.error(f"[INTEGRITY] Table {table_id} has invalid round_id type: {round_id} ({type(round_id)})")
                return False

    logger.debug(f"[INTEGRITY] Data check passed: {len(results_by_table)} tables, {total_records} total records")
    return True


def upsert_record(record: Dict[str, Any]) -> bool:
    """Upsert record using (table_id, round_id) as primary key"""
    table_id, round_id = extract_table_round_key(record)
    if not table_id or round_id is None:
        return False

    # 確保資料結構正確
    if table_id not in results_by_table:
        results_by_table[table_id] = {}

    # 額外檢查：確保 results_by_table[table_id] 是字典
    if not isinstance(results_by_table[table_id], dict):
        logger.error(f"[UPSERT] Table {table_id} rounds is not a dict: {type(results_by_table[table_id])}")
        results_by_table[table_id] = {}

    # 加上接收時間戳
    record["ingested_at"] = datetime.now().isoformat()

    # 檢查是否為新記錄
    is_new_record = round_id not in results_by_table[table_id]

    # Upsert
    results_by_table[table_id][round_id] = record

    # 調試輸出
    try:
        status_name = record.get("game_payment_status_name", "unknown")
        result_code = (record.get("gameResult") or {}).get("result", -1) if isinstance(record.get("gameResult"), dict) else -1
        action = "NEW" if is_new_record else "UPDATE"
        current_count = len(results_by_table[table_id])
        logger.debug(f"[UPSERT] {action} {table_id}:{round_id} -> {status_name} (result: {result_code}) [Total: {current_count}]")
    except Exception as e:
        logger.error(f"[UPSERT] Debug output failed: {e}")

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
    # 檢查資料完整性
    if not check_data_integrity():
        logger.error("[TABLE_SUMMARY] Data integrity check failed!")

    tables_data: Dict[str, Dict[str, Any]] = {}

    # 從 results_by_table 計算真實的總局數和莊閒和統計
    for table_id, rounds in results_by_table.items():
        table_id = str(table_id)

        # 雙重檢查：確保 rounds 是字典且不是 None
        if not isinstance(rounds, dict):
            logger.error(f"[TABLE_SUMMARY] Table {table_id} has invalid rounds: {type(rounds)}")
            continue

        game_count = len(rounds)

        # 計算莊閒和統計
        result_stats = {
            "banker_wins": 0,
            "player_wins": 0,
            "ties": 0,
            "other": 0
        }

        for round_id, record in rounds.items():
            # 提取遊戲結果 - 檢查多種可能的格式
            game_result = None

            # 方法1: gameResult 是字典，包含 result 數字碼
            if isinstance(record.get("gameResult"), dict):
                result_code = record["gameResult"].get("result")
                if result_code == 0:
                    game_result = "莊勝"
                elif result_code == 1:
                    game_result = "閒勝"
                elif result_code == 2:
                    game_result = "和局"
                elif result_code == 3:
                    game_result = "取消/無效"
                # 也檢查字典中的其他可能欄位
                elif "win_lose_result" in record["gameResult"]:
                    win_lose = record["gameResult"]["win_lose_result"]
                    if win_lose == "莊勝":
                        game_result = "莊勝"
                    elif win_lose == "閒勝":
                        game_result = "閒勝"
                    elif win_lose == "和局":
                        game_result = "和局"

            # 方法2: gameResult 是字串
            elif isinstance(record.get("gameResult"), str):
                game_result_str = record["gameResult"]
                if game_result_str in ["莊勝", "閒勝", "和局"]:
                    game_result = game_result_str

            # 方法3: 檢查其他可能的欄位名稱
            if not game_result:
                # 檢查是否有 win_lose_result 直接在 record 中
                if "win_lose_result" in record:
                    win_lose = record["win_lose_result"]
                    if win_lose in ["莊勝", "閒勝", "和局"]:
                        game_result = win_lose

            # 統計結果
            if game_result == "莊勝":
                result_stats["banker_wins"] += 1
            elif game_result == "閒勝":
                result_stats["player_wins"] += 1
            elif game_result == "和局":
                result_stats["ties"] += 1
            else:
                result_stats["other"] += 1

        # 驗算總局數
        calculated_total = sum(result_stats.values())
        if calculated_total != game_count:
            logger.warning(f"[TABLE_SUMMARY] {table_id}: Round count mismatch! Storage: {game_count}, Calculated: {calculated_total}")

        # 調試輸出
        logger.debug(f"[TABLE_SUMMARY] {table_id}: {game_count} games, 莊:{result_stats['banker_wins']} 閒:{result_stats['player_wins']} 和:{result_stats['ties']} 其他:{result_stats['other']}")

        if table_id not in tables_data:
            tables_data[table_id] = {
                "table_id": table_id,
                "total_games": game_count,
                "result_statistics": result_stats,
                "latest_record": None,
                "status_counts": {},
                "last_update": None,
            }
        else:
            tables_data[table_id]["total_games"] = game_count
            tables_data[table_id]["result_statistics"] = result_stats

    # 使用 latest_records 補充其他資訊（狀態、最新記錄等）
    for record in latest_records:
        table_id = record.get("table_id") or record.get("tableId") or record.get("table")
        if not table_id:
            continue
        table_id = str(table_id)

        if table_id not in tables_data:
            # 修正邏輯錯誤：正確計算該桌的總局數和莊閒和統計
            rounds = results_by_table.get(table_id, {})
            game_count = len(rounds)

            # 計算莊閒和統計
            result_stats = {
                "banker_wins": 0,
                "player_wins": 0,
                "ties": 0,
                "other": 0
            }

            for round_id, record in rounds.items():
                # 提取遊戲結果 - 檢查多種可能的格式
                game_result = None

                # 方法1: gameResult 是字典，包含 result 數字碼
                if isinstance(record.get("gameResult"), dict):
                    result_code = record["gameResult"].get("result")
                    if result_code == 0:
                        game_result = "莊勝"
                    elif result_code == 1:
                        game_result = "閒勝"
                    elif result_code == 2:
                        game_result = "和局"
                    elif result_code == 3:
                        game_result = "取消/無效"
                    # 也檢查字典中的其他可能欄位
                    elif "win_lose_result" in record["gameResult"]:
                        win_lose = record["gameResult"]["win_lose_result"]
                        if win_lose == "莊勝":
                            game_result = "莊勝"
                        elif win_lose == "閒勝":
                            game_result = "閒勝"
                        elif win_lose == "和局":
                            game_result = "和局"

                # 方法2: gameResult 是字串
                elif isinstance(record.get("gameResult"), str):
                    game_result_str = record["gameResult"]
                    if game_result_str in ["莊勝", "閒勝", "和局"]:
                        game_result = game_result_str

                # 方法3: 檢查其他可能的欄位名稱
                if not game_result:
                    # 檢查是否有 win_lose_result 直接在 record 中
                    if "win_lose_result" in record:
                        win_lose = record["win_lose_result"]
                        if win_lose in ["莊勝", "閒勝", "和局"]:
                            game_result = win_lose

                # 統計結果
                if game_result == "莊勝":
                    result_stats["banker_wins"] += 1
                elif game_result == "閒勝":
                    result_stats["player_wins"] += 1
                elif game_result == "和局":
                    result_stats["ties"] += 1
                else:
                    result_stats["other"] += 1

            tables_data[table_id] = {
                "table_id": table_id,
                "total_games": game_count,
                "result_statistics": result_stats,
                "latest_record": None,
                "status_counts": {},
                "last_update": None,
            }

        # 不再從 latest_records 計算總局數，只處理狀態統計

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


# 調試端點 - 檢查 results_by_table 狀態
@app.get("/api/debug/tables")
async def debug_tables():
    """調試端點：檢查 results_by_table 的詳細狀態"""
    # 執行完整性檢查
    integrity_status = check_data_integrity()

    debug_info = {
        "integrity_check": integrity_status,
        "results_by_table_summary": {},
        "latest_records_count": len(latest_records),
        "state_index_count": len(state_index),
        "total_games_across_all_tables": sum(len(rounds) for rounds in results_by_table.values()),
        "timestamp": datetime.now().isoformat(),
    }

    for table_id, rounds in results_by_table.items():
        if not isinstance(rounds, dict):
            debug_info["results_by_table_summary"][str(table_id)] = {
                "error": f"Invalid rounds type: {type(rounds)}"
            }
            continue

        round_ids = sorted(rounds.keys())
        debug_info["results_by_table_summary"][str(table_id)] = {
            "total_rounds": len(rounds),
            "min_round_id": min(round_ids) if round_ids else None,
            "max_round_id": max(round_ids) if round_ids else None,
            "latest_5_rounds": round_ids[-5:] if len(round_ids) >= 5 else round_ids,
            "earliest_5_rounds": round_ids[:5] if len(round_ids) >= 5 else round_ids,
            "rounds_type_check": all(isinstance(rid, int) for rid in round_ids),
        }

        # 特別檢查是否有重複的 round_id
        if len(round_ids) != len(set(round_ids)):
            debug_info["results_by_table_summary"][str(table_id)]["duplicate_round_ids"] = True

    return debug_info


# 調試端點 - 檢查特定桌號的詳細資料
@app.get("/api/debug/tables/{table_id}")
async def debug_specific_table(table_id: str):
    """調試端點：檢查特定桌號的詳細狀態"""
    if table_id not in results_by_table:
        return {"error": f"Table {table_id} not found in results_by_table"}

    rounds = results_by_table[table_id]
    round_ids = sorted(rounds.keys())

    debug_info = {
        "table_id": table_id,
        "total_rounds": len(rounds),
        "rounds_data_type": type(rounds).__name__,
        "all_round_ids": round_ids,
        "round_id_gaps": [],
        "sample_records": {},
        "timestamp": datetime.now().isoformat(),
    }

    # 檢查 round_id 間隙
    for i in range(1, len(round_ids)):
        gap = round_ids[i] - round_ids[i-1]
        if gap > 1:
            debug_info["round_id_gaps"].append({
                "from": round_ids[i-1],
                "to": round_ids[i],
                "gap_size": gap - 1
            })

    # 取樣幾個記錄
    sample_indices = [0, len(round_ids)//2, -1] if len(round_ids) > 2 else list(range(len(round_ids)))
    for idx in sample_indices:
        if 0 <= idx < len(round_ids):
            round_id = round_ids[idx]
            record = rounds[round_id]
            debug_info["sample_records"][f"round_{round_id}"] = {
                "game_payment_status_name": record.get("game_payment_status_name"),
                "ingested_at": record.get("ingested_at"),
                "keys": list(record.keys())
            }

    return debug_info


# ===== 資料導出功能 =====

def filter_records_by_criteria(
    table_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status_filter: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """根據條件篩選記錄"""
    all_records: List[Dict[str, Any]] = []

    # 從 results_by_table 收集資料
    target_tables = [table_id] if table_id else list(results_by_table.keys())

    for tid in target_tables:
        if tid not in results_by_table:
            continue

        table_rounds = results_by_table[tid]
        for _, record in table_rounds.items():
            all_records.append(record)

    # 篩選條件
    filtered_records = []

    for record in all_records:
        # 狀態篩選
        if status_filter:
            record_status = record.get("game_payment_status_name", "")
            if status_filter.lower() not in record_status.lower():
                continue

        # 日期篩選（基於 game_start_time 或 ingested_at）
        if start_date or end_date:
            record_date_str = record.get("game_start_time") or record.get("ingested_at", "")
            if record_date_str:
                try:
                    # 嘗試解析日期
                    if "T" in record_date_str:
                        record_date = datetime.fromisoformat(record_date_str.replace("Z", "+00:00")).date()
                    else:
                        record_date = datetime.strptime(record_date_str[:10], "%Y-%m-%d").date()

                    if start_date:
                        start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
                        if record_date < start_d:
                            continue

                    if end_date:
                        end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
                        if record_date > end_d:
                            continue
                except (ValueError, TypeError):
                    # 日期解析失敗，跳過此記錄
                    continue

        filtered_records.append(record)

    # 排序（按 round_id 降序）
    filtered_records.sort(
        key=lambda x: int(x.get("round_id") or x.get("roundId") or 0),
        reverse=True
    )

    # 限制數量
    if limit and limit > 0:
        filtered_records = filtered_records[:limit]

    return filtered_records


def prepare_export_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """準備導出資料，標準化欄位"""
    export_data = []

    for record in records:
        # 解析遊戲結果
        game_result = record.get("gameResult", {})
        if isinstance(game_result, dict):
            result_code = game_result.get("result", -1)
            result_text = {
                0: "莊勝",
                1: "閒勝",
                2: "和局",
                3: "取消/無效",
                -1: "進行中"
            }.get(result_code, f"結果{result_code}")
        else:
            result_text = str(game_result) if game_result else "N/A"

        # 標準化資料
        export_record = {
            "桌號": record.get("table_id") or record.get("tableId") or record.get("table", ""),
            "局號": record.get("round_id") or record.get("roundId", ""),
            "遊戲狀態": record.get("game_payment_status_name", "未知"),
            "遊戲結果": result_text,
            "開局時間": record.get("game_start_time", ""),
            "接收時間": record.get("ingested_at", ""),
            "結果代碼": game_result.get("result", -1) if isinstance(game_result, dict) else -1,
            "狀態代碼": record.get("game_payment_status", -1),
        }

        # 加入其他有用的欄位
        for key, value in record.items():
            if key not in ["table_id", "tableId", "table", "round_id", "roundId",
                          "game_payment_status_name", "gameResult", "game_start_time",
                          "ingested_at", "game_payment_status"]:
                if not isinstance(value, (dict, list)):
                    export_record[f"原始_{key}"] = value

        export_data.append(export_record)

    return export_data


async def export_to_json(records: List[Dict[str, Any]]) -> bytes:
    """導出為 JSON 格式"""
    export_data = prepare_export_data(records)

    result = {
        "export_info": {
            "timestamp": datetime.now().isoformat(),
            "total_records": len(export_data),
            "format": "json"
        },
        "data": export_data
    }

    return json.dumps(result, ensure_ascii=False, indent=2).encode('utf-8')


async def export_to_csv(records: List[Dict[str, Any]]) -> bytes:
    """導出為 CSV 格式"""
    export_data = prepare_export_data(records)

    if not export_data:
        return "沒有資料可導出\n".encode('utf-8-sig')

    # 使用 StringIO 創建 CSV
    output = io.StringIO()

    # 獲取所有欄位名稱
    fieldnames = list(export_data[0].keys())

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for record in export_data:
        writer.writerow(record)

    # 使用 UTF-8 BOM 以確保 Excel 正確顯示中文
    return output.getvalue().encode('utf-8-sig')


async def export_to_excel(records: List[Dict[str, Any]]) -> bytes:
    """導出為 Excel 格式"""
    export_data = prepare_export_data(records)

    if not export_data:
        # 創建空的 Excel 檔案
        df = pd.DataFrame([{"訊息": "沒有資料可導出"}])
    else:
        df = pd.DataFrame(export_data)

    # 使用 BytesIO 創建 Excel 檔案
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        if export_data:
            # 依桌號分組創建多個工作表
            tables = {}
            for record in export_data:
                table_id = record.get("桌號", "未知桌號")
                if table_id not in tables:
                    tables[table_id] = []
                tables[table_id].append(record)

            # 總覽工作表
            df.to_excel(writer, sheet_name='總覽', index=False)

            # 各桌工作表
            for table_id, table_records in tables.items():
                if len(table_records) > 0:
                    table_df = pd.DataFrame(table_records)
                    # Excel 工作表名稱不能超過31字符
                    sheet_name = f"桌號_{table_id}"[:31]
                    table_df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            df.to_excel(writer, sheet_name='空資料', index=False)

    output.seek(0)
    return output.read()


# API 端點：資料導出
@app.get("/api/export/{format}")
async def export_data(
    format: str = Path(..., regex="^(json|csv|excel)$"),
    table_id: Optional[str] = Query(None, description="指定桌號"),
    start_date: Optional[str] = Query(None, description="開始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="結束日期 (YYYY-MM-DD)"),
    status_filter: Optional[str] = Query(None, description="狀態篩選"),
    limit: Optional[int] = Query(None, ge=1, le=10000, description="限制筆數 (最多10000)"),
    x_ingest_key: Optional[str] = Header(None, alias="x-ingest-key")
):
    """
    資料導出 API

    支援格式：
    - json: JSON 格式
    - csv: CSV 格式（適合 Excel）
    - excel: Excel 格式（多工作表）

    篩選條件：
    - table_id: 指定桌號
    - start_date/end_date: 日期範圍
    - status_filter: 狀態篩選（模糊匹配）
    - limit: 限制筆數
    """
    # 驗證 API Key
    expected_key = os.getenv("INGEST_KEY", "baccaratt9webapi")
    if x_ingest_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid export key")

    try:
        # 篩選資料
        filtered_records = filter_records_by_criteria(
            table_id=table_id,
            start_date=start_date,
            end_date=end_date,
            status_filter=status_filter,
            limit=limit
        )

        logger.info(f"Export request: format={format}, table_id={table_id}, "
                   f"date_range={start_date} to {end_date}, status={status_filter}, "
                   f"limit={limit}, found={len(filtered_records)} records")

        # 根據格式導出
        if format == "json":
            content = await export_to_json(filtered_records)
            media_type = "application/json"
            filename = f"t9_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        elif format == "csv":
            content = await export_to_csv(filtered_records)
            media_type = "text/csv"
            filename = f"t9_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        elif format == "excel":
            content = await export_to_excel(filtered_records)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"t9_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        else:
            raise HTTPException(status_code=400, detail="Unsupported format")

        # 更新指標
        metrics.increment_counter("exports_total")
        metrics.increment_counter(f"exports_{format}")

        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "X-Total-Records": str(len(filtered_records)),
                "X-Export-Format": format,
                "X-Export-Timestamp": datetime.now().isoformat()
            }
        )

    except Exception as e:
        logger.error(f"Export error: {e}")
        metrics.record_error("export_error", str(e))
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


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
