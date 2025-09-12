#!/usr/bin/env python3
"""
負載測試腳本 - 測試系統在高並發下的表現
"""

import asyncio
import aiohttp
import json
import time
import random
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import statistics


@dataclass
class LoadTestResult:
    """負載測試結果"""
    total_requests: int
    successful_requests: int
    failed_requests: int
    avg_response_time_ms: float
    p95_response_time_ms: float
    requests_per_second: float
    error_rate: float
    test_duration_s: float
    concurrent_connections: int


class LoadTester:
    """負載測試器"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.response_times: List[float] = []
        self.errors: List[str] = []
    
    def generate_test_record(self) -> Dict[str, Any]:
        """生成測試用的遊戲記錄"""
        return {
            "table": f"T{random.randint(1, 20)}",
            "roundId": f"R{int(time.time())}-{random.randint(1000, 9999)}",
            "openTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "closeTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "result": random.choice(["莊家勝", "閒家勝", "和局"]),
            "playerPoints": random.randint(0, 9),
            "bankerPoints": random.randint(0, 9),
            "playerPair": random.choice([True, False]),
            "bankerPair": random.choice([True, False]),
            "status": "completed"
        }
    
    async def send_ingest_request(self, session: aiohttp.ClientSession, 
                                 record_count: int = 1) -> Dict[str, Any]:
        """發送 ingest 請求"""
        records = [self.generate_test_record() for _ in range(record_count)]
        
        payload = {"records": records}
        headers = {
            "X-INGEST-KEY": "baccaratt9webapi",  # 從 .env 獲取
            "Content-Type": "application/json"
        }
        
        start_time = time.time()
        
        try:
            async with session.post(
                f"{self.base_url}/ingest",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                response_time = (time.time() - start_time) * 1000
                self.response_times.append(response_time)
                
                if response.status == 200:
                    result = await response.json()
                    return {
                        "success": True,
                        "response_time_ms": response_time,
                        "records_processed": result.get("count", 0)
                    }
                else:
                    error_text = await response.text()
                    self.errors.append(f"HTTP {response.status}: {error_text}")
                    return {
                        "success": False,
                        "response_time_ms": response_time,
                        "error": f"HTTP {response.status}"
                    }
                    
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            self.response_times.append(response_time)
            self.errors.append(str(e))
            return {
                "success": False,
                "response_time_ms": response_time,
                "error": str(e)
            }
    
    async def run_load_test(self, 
                           concurrent_connections: int = 10,
                           requests_per_connection: int = 20,
                           records_per_request: int = 5) -> LoadTestResult:
        """
        運行負載測試
        
        Args:
            concurrent_connections: 並發連接數
            requests_per_connection: 每個連接發送的請求數
            records_per_request: 每個請求包含的記錄數
            
        Returns:
            LoadTestResult: 測試結果
        """
        print("=" * 60)
        print("🔥 LOAD TEST STARTING")
        print(f"   Concurrent Connections: {concurrent_connections}")
        print(f"   Requests per Connection: {requests_per_connection}")
        print(f"   Records per Request: {records_per_request}")
        print(f"   Total Requests: {concurrent_connections * requests_per_connection}")
        print(f"   Total Records: {concurrent_connections * requests_per_connection * records_per_request}")
        print("=" * 60)
        
        start_time = time.time()
        
        # 清空之前的結果
        self.response_times = []
        self.errors = []
        
        # 創建連接池
        connector = aiohttp.TCPConnector(
            limit=concurrent_connections * 2,
            limit_per_host=concurrent_connections * 2
        )
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # 創建所有任務
            tasks = []
            
            for conn_id in range(concurrent_connections):
                # 為每個連接創建多個請求任務
                for req_id in range(requests_per_connection):
                    task = asyncio.create_task(
                        self.send_ingest_request(session, records_per_request),
                        name=f"conn_{conn_id}_req_{req_id}"
                    )
                    tasks.append(task)
            
            # 執行所有任務
            print(f"🚀 Executing {len(tasks)} concurrent requests...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        test_duration = end_time - start_time
        
        # 統計結果
        successful_requests = 0
        failed_requests = 0
        
        for result in results:
            if isinstance(result, Exception):
                failed_requests += 1
                self.errors.append(str(result))
            elif isinstance(result, dict) and result.get("success"):
                successful_requests += 1
            else:
                failed_requests += 1
        
        total_requests = len(results)
        
        # 計算指標
        avg_response_time = statistics.mean(self.response_times) if self.response_times else 0
        p95_response_time = (
            statistics.quantiles(self.response_times, n=20)[18] 
            if len(self.response_times) > 20 
            else max(self.response_times) if self.response_times else 0
        )
        
        requests_per_second = total_requests / test_duration if test_duration > 0 else 0
        error_rate = failed_requests / total_requests if total_requests > 0 else 0
        
        return LoadTestResult(
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            avg_response_time_ms=avg_response_time,
            p95_response_time_ms=p95_response_time,
            requests_per_second=requests_per_second,
            error_rate=error_rate,
            test_duration_s=test_duration,
            concurrent_connections=concurrent_connections
        )
    
    def print_load_test_results(self, result: LoadTestResult):
        """打印負載測試結果"""
        print("\n" + "=" * 60)
        print("📊 LOAD TEST RESULTS")
        print("=" * 60)
        
        # 基本統計
        print(f"📈 Total Requests: {result.total_requests}")
        print(f"✅ Successful: {result.successful_requests}")
        print(f"❌ Failed: {result.failed_requests}")
        print(f"⏱️  Test Duration: {result.test_duration_s:.1f}s")
        
        # 性能指標
        print(f"\n🚀 Performance Metrics:")
        print(f"   Requests/Second: {result.requests_per_second:.1f}")
        print(f"   Avg Response Time: {result.avg_response_time_ms:.1f}ms")
        print(f"   P95 Response Time: {result.p95_response_time_ms:.1f}ms")
        
        # 錯誤率
        error_status = "✅" if result.error_rate < 0.01 else "❌"
        print(f"{error_status} Error Rate: {result.error_rate:.2%} (target: <1%)")
        
        # 錯誤詳情
        if self.errors:
            print(f"\n⚠️  Error Summary (showing first 5):")
            for i, error in enumerate(self.errors[:5]):
                print(f"   {i+1}. {error}")
            if len(self.errors) > 5:
                print(f"   ... and {len(self.errors) - 5} more errors")
        
        # 評估
        print("\n" + "=" * 60)
        if result.error_rate < 0.01 and result.requests_per_second > 50:
            print("🎉 LOAD TEST PASSED!")
        else:
            print("⚠️  LOAD TEST NEEDS IMPROVEMENT")
        print("=" * 60)


async def run_progressive_load_test(tester: LoadTester):
    """運行漸進式負載測試"""
    print("🔥 Progressive Load Testing")
    
    test_scenarios = [
        {"connections": 5, "requests": 10, "records": 3},
        {"connections": 10, "requests": 15, "records": 5},
        {"connections": 20, "requests": 20, "records": 10},
        {"connections": 50, "requests": 10, "records": 20},
    ]
    
    for i, scenario in enumerate(test_scenarios):
        print(f"\n🎯 Scenario {i+1}: {scenario['connections']}C × {scenario['requests']}R × {scenario['records']}Rec")
        
        result = await tester.run_load_test(
            concurrent_connections=scenario["connections"],
            requests_per_connection=scenario["requests"],
            records_per_request=scenario["records"]
        )
        
        tester.print_load_test_results(result)
        
        # 如果錯誤率過高，停止測試
        if result.error_rate > 0.1:
            print("❌ High error rate detected, stopping progressive test")
            break
        
        # 間隔等待
        if i < len(test_scenarios) - 1:
            print("\n⏳ Waiting 5s before next scenario...")
            await asyncio.sleep(5)


async def main():
    """主測試函數"""
    tester = LoadTester()
    
    print(f"🧪 Load Test Suite")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Target: Error Rate < 1%, RPS > 50")
    
    try:
        # 運行漸進式負載測試
        await run_progressive_load_test(tester)
        
        return True
        
    except Exception as e:
        print(f"❌ Load test failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)