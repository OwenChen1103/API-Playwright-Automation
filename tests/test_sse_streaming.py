#!/usr/bin/env python3
"""
SSE 流測試腳本 - 驗證 TTFD 和端到端延遲指標
"""

import asyncio
import json
import time
import aiohttp
import statistics
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TTFDResult:
    """TTFD 測試結果"""
    ttfd_ms: float
    total_events: int
    connection_time_ms: float
    first_event_time_ms: float
    test_duration_s: float


@dataclass
class PerformanceMetrics:
    """性能指標"""
    ttfd_p95: float
    ttfd_avg: float
    end_to_end_p95: float
    end_to_end_avg: float
    success_rate: float
    events_per_second: float
    connection_success_rate: float


class SSEStreamTester:
    """SSE 流測試器"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.results: List[TTFDResult] = []
        
    async def test_single_connection_ttfd(self, test_duration: int = 10) -> TTFDResult:
        """
        測試單個 SSE 連接的 TTFD
        
        Args:
            test_duration: 測試持續時間（秒）
            
        Returns:
            TTFDResult: TTFD 測試結果
        """
        start_time = time.time()
        connection_established = False
        first_event_received = False
        connection_time = 0
        first_event_time = 0
        event_count = 0
        
        try:
            async with aiohttp.ClientSession() as session:
                # 記錄連接開始時間
                connect_start = time.time()
                
                async with session.get(
                    f"{self.base_url}/api/stream",
                    headers={"Accept": "text/event-stream"},
                    timeout=aiohttp.ClientTimeout(total=test_duration + 5)
                ) as response:
                    
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
                    
                    # 記錄連接建立時間
                    connection_time = (time.time() - connect_start) * 1000
                    connection_established = True
                    
                    # 讀取 SSE 流
                    async for line in response.content:
                        line_str = line.decode('utf-8').strip()
                        
                        if line_str.startswith('data:'):
                            if not first_event_received:
                                # 記錄首個事件時間 (TTFD)
                                first_event_time = (time.time() - start_time) * 1000
                                first_event_received = True
                            
                            event_count += 1
                            
                            # 檢查測試時間
                            if time.time() - start_time >= test_duration:
                                break
                    
        except Exception as e:
            print(f"⚠️ Connection failed: {e}")
        
        total_time = time.time() - start_time
        
        return TTFDResult(
            ttfd_ms=first_event_time if first_event_received else -1,
            total_events=event_count,
            connection_time_ms=connection_time if connection_established else -1,
            first_event_time_ms=first_event_time,
            test_duration_s=total_time
        )
    
    async def test_multiple_connections(self, num_connections: int = 5, 
                                      test_duration: int = 10) -> List[TTFDResult]:
        """
        測試多個並發 SSE 連接
        
        Args:
            num_connections: 並發連接數
            test_duration: 每個連接的測試時間
            
        Returns:
            List[TTFDResult]: 測試結果列表
        """
        print(f"Starting {num_connections} concurrent SSE connections...")
        
        # 並發執行多個連接測試
        tasks = []
        for i in range(num_connections):
            task = asyncio.create_task(
                self.test_single_connection_ttfd(test_duration),
                name=f"sse_test_{i}"
            )
            tasks.append(task)
        
        # 等待所有測試完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 過濾異常結果
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ Connection {i} failed: {result}")
            else:
                valid_results.append(result)
        
        return valid_results
    
    async def run_performance_test(self, iterations: int = 20, 
                                  concurrent_connections: int = 3) -> PerformanceMetrics:
        """
        運行完整的性能測試
        
        Args:
            iterations: 測試迭代次數
            concurrent_connections: 並發連接數
            
        Returns:
            PerformanceMetrics: 性能指標
        """
        print("=" * 60)
        print("SSE Performance Test Starting")
        print(f"   Iterations: {iterations}")
        print(f"   Concurrent Connections: {concurrent_connections}")
        print("=" * 60)
        
        all_results = []
        successful_connections = 0
        total_connections = 0
        
        for iteration in range(iterations):
            print(f"\nIteration {iteration + 1}/{iterations}")
            
            # 測試多個並發連接
            batch_results = await self.test_multiple_connections(
                concurrent_connections, 
                test_duration=5
            )
            
            # 統計結果
            valid_batch = [r for r in batch_results if r.ttfd_ms > 0]
            all_results.extend(valid_batch)
            
            successful_connections += len(valid_batch)
            total_connections += concurrent_connections
            
            # 顯示批次結果
            if valid_batch:
                batch_ttfd = [r.ttfd_ms for r in valid_batch]
                avg_ttfd = statistics.mean(batch_ttfd)
                print(f"   ✅ {len(valid_batch)}/{concurrent_connections} connections successful")
                print(f"   📊 Batch TTFD avg: {avg_ttfd:.1f}ms")
            else:
                print(f"   ❌ All connections failed in this batch")
            
            # 間隔等待
            if iteration < iterations - 1:
                await asyncio.sleep(1)
        
        # 計算最終指標
        if not all_results:
            raise Exception("No successful connections in performance test")
        
        # TTFD 指標
        ttfd_values = [r.ttfd_ms for r in all_results]
        ttfd_avg = statistics.mean(ttfd_values)
        ttfd_p95 = statistics.quantiles(ttfd_values, n=20)[18]  # 95th percentile
        
        # 端到端延遲（近似）
        end_to_end_values = [(r.connection_time_ms + r.first_event_time_ms) 
                            for r in all_results if r.connection_time_ms > 0]
        
        if end_to_end_values:
            end_to_end_avg = statistics.mean(end_to_end_values)
            end_to_end_p95 = statistics.quantiles(end_to_end_values, n=20)[18]
        else:
            end_to_end_avg = end_to_end_p95 = 0
        
        # 成功率
        connection_success_rate = successful_connections / total_connections
        success_rate = len([r for r in all_results if r.total_events > 0]) / len(all_results)
        
        # 事件速率
        total_events = sum(r.total_events for r in all_results)
        total_time = sum(r.test_duration_s for r in all_results)
        events_per_second = total_events / total_time if total_time > 0 else 0
        
        return PerformanceMetrics(
            ttfd_p95=ttfd_p95,
            ttfd_avg=ttfd_avg,
            end_to_end_p95=end_to_end_p95,
            end_to_end_avg=end_to_end_avg,
            success_rate=success_rate,
            events_per_second=events_per_second,
            connection_success_rate=connection_success_rate
        )
    
    def print_results(self, metrics: PerformanceMetrics):
        """打印測試結果"""
        print("\n" + "=" * 60)
        print("📊 PERFORMANCE TEST RESULTS")
        print("=" * 60)
        
        # TTFD 指標
        ttfd_status = "✅" if metrics.ttfd_p95 <= 300 else "❌"
        print(f"{ttfd_status} TTFD P95: {metrics.ttfd_p95:.1f}ms (target: ≤300ms)")
        print(f"   TTFD Average: {metrics.ttfd_avg:.1f}ms")
        
        # 端到端延遲
        e2e_status = "✅" if metrics.end_to_end_p95 <= 800 else "❌"
        print(f"{e2e_status} End-to-End P95: {metrics.end_to_end_p95:.1f}ms (target: ≤800ms)")
        print(f"   End-to-End Average: {metrics.end_to_end_avg:.1f}ms")
        
        # 成功率
        success_status = "✅" if metrics.success_rate >= 0.99 else "❌"
        print(f"{success_status} Success Rate: {metrics.success_rate:.2%} (target: ≥99%)")
        
        connection_status = "✅" if metrics.connection_success_rate >= 0.95 else "❌"
        print(f"{connection_status} Connection Success Rate: {metrics.connection_success_rate:.2%}")
        
        # 吞吐量
        print(f"📈 Events per Second: {metrics.events_per_second:.1f}")
        
        # 總體評估
        all_passed = (
            metrics.ttfd_p95 <= 300 and
            metrics.end_to_end_p95 <= 800 and
            metrics.success_rate >= 0.99
        )
        
        print("\n" + "=" * 60)
        if all_passed:
            print("🎉 ALL PERFORMANCE TARGETS MET!")
        else:
            print("⚠️  SOME PERFORMANCE TARGETS NOT MET")
        print("=" * 60)


async def main():
    """主測試函數"""
    tester = SSEStreamTester()
    
    try:
        # 運行性能測試
        metrics = await tester.run_performance_test(
            iterations=10,  # 可以調整測試強度
            concurrent_connections=5
        )
        
        # 打印結果
        tester.print_results(metrics)
        
        return metrics
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        return None


if __name__ == "__main__":
    print(f"🧪 SSE Streaming Performance Test")
    print(f"   Target: TTFD ≤ 300ms, E2E P95 ≤ 800ms, Success Rate ≥ 99%")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    result = asyncio.run(main())
    
    if result:
        # 返回測試狀態碼
        exit_code = 0 if (
            result.ttfd_p95 <= 300 and 
            result.end_to_end_p95 <= 800 and 
            result.success_rate >= 0.99
        ) else 1
        
        exit(exit_code)
    else:
        exit(1)