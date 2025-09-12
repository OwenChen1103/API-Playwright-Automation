#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome 調試模式設定助手
幫助你啟動 Chrome 瀏覽器並啟用調試模式
"""

import subprocess
import sys
import os
import time
import webbrowser
from pathlib import Path

TARGET_URL = os.getenv("TARGET_URL", "https://i.t9live3.vip/")

def find_chrome_path():
    """尋找 Chrome 瀏覽器路徑"""
    possible_paths = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Users\{}\AppData\Local\Google\Chrome\Application\chrome.exe".format(os.getenv('USERNAME')),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    return None

def start_chrome_with_debug():
    """啟動 Chrome 並啟用調試模式"""
    chrome_path = find_chrome_path()
    
    if not chrome_path:
        print("❌ 找不到 Chrome 瀏覽器")
        print("請手動啟動 Chrome 並添加以下參數：")
        print("--remote-debugging-port=9222 --user-data-dir=C:\\temp\\chrome-debug")
        return False
    
    print(f"✅ 找到 Chrome: {chrome_path}")
    
    # Chrome 調試參數
    debug_args = [
        chrome_path,
        "--remote-debugging-port=9222",
        "--user-data-dir=C:\\temp\\chrome-debug",
        "--disable-web-security",
        "--disable-features=VizDisplayCompositor",
        TARGET_URL,  # 直接在調試視窗開啟目標網址
    ]
    
    try:
        print("🚀 正在啟動 Chrome 調試模式...")
        subprocess.Popen(debug_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # 等待 Chrome 啟動
        time.sleep(3)
        
        print("✅ Chrome 調試模式已啟動")
        print(f"🌐 已自動開啟: {TARGET_URL}")
        print("🌐 正在開啟調試頁面…")
        
        # 開啟調試頁面（非必須，可忽略）
        webbrowser.open("http://localhost:9222")
        
        print("\n📝 接下來的步驟：")
        print("1. 在剛剛開啟的 9222 視窗確認已登入該站台")
        print("2. 保持該視窗開啟")
        print("3. 運行 worker：python monitor_worker.py 或 python final_baccarat_monitor.py")
        
        return True
        
    except Exception as e:
        print(f"❌ 啟動 Chrome 失敗: {e}")
        return False

def main():
    """主程式"""
    print("🔧 Chrome 調試模式設定助手")
    print("="*50)
    
    # 檢查是否已有 Chrome 在調試模式運行
    try:
        import requests
        response = requests.get("http://localhost:9222", timeout=2)
        if response.status_code == 200:
            print("✅ Chrome 調試模式已在運行")
            print("🌐 調試頁面: http://localhost:9222")
            return
    except:
        pass
    
    # 啟動 Chrome 調試模式
    if start_chrome_with_debug():
        print("\n🎉 設定完成！")
    else:
        print("\n❌ 設定失敗，請手動操作")

if __name__ == "__main__":
    main()
