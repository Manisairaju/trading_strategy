import json
import random
import threading
import time
import os
import sys
import requests
import pandas as pd
import numpy as np
from websocket import create_connection

# =====================================================================
# 🛠️ SYSTEM CONFIGURATION (GitHub Secrets Enabled)
# =====================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("❌ Critical Error: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID Secrets are missing!")
    sys.exit(1)

# We use FX_IDC feeds via TradingView's public websocket (No login required)
PAIRS = {
    "USDJPY": "FX_IDC:USDJPY",
    "AUDJPY": "FX_IDC:AUDJPY",
    "NZDJPY": "FX_IDC:NZDJPY",
    "CADJPY": "FX_IDC:CADJPY",
    "EURJPY": "FX_IDC:EURJPY",
    "GBPJPY": "FX_IDC:GBPJPY"
}

# In-memory database to store real-time tick ticks
market_data = {pair: [] for pair in PAIRS.keys()}
active_tracks = set()
lock = threading.Lock()

def send_telegram_signal(message):
    """Sends immediate alert to your Telegram Channel via GitHub Secrets."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=4)
        if response.status_code != 200:
            print(f"❌ Telegram API Error: {response.text}")
    except Exception as e:
        print(f"❌ Telegram Connection Error: {e}")

def generate_session_id():
    string_set = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(string_set) for _ in range(12))

def prepend_header(st):
    return f"~m~{len(st)}~m~{st}"

def create_message(func, param_list):
    return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

def live_websocket_worker():
    """Connects to public TradingView servers and streams ticks instantly."""
    while True:
        try:
            ws = create_connection(
                "wss://data.tradingview.com/socket.io/1/websocket/xhr",
                headers={"Origin": "https://www.tradingview.com"}
            )
            session = "qs_" + generate_session_id()
            ws.send(prepend_header(create_message("set_auth_token", ["unauthorized_user_token"])))
            ws.send(prepend_header(create_message("chart_create_session", [session, ""])))
            
            for internal_name, tv_symbol in PAIRS.items():
                ws.send(prepend_header(create_message("quote_add_symbols", [session, tv_symbol, {"qty": 1}])))
            
            print("🟩 Live Stream WebSocket Established (No-Login Engine Connected)")
            
            while True:
                result = ws.recv()
                if result.startswith("~h~"):
                    ws.send(result)
                    continue
                
                if '"m":"q_sd"' in result:
                    payloads = result.split("~m~")
                    for p in payloads:
                        if "{" in p:
                            try:
                                data_json = json.loads(p)
                                for item in data_json.get("p", []):
                                    if isinstance(item, dict) and "v" in item:
                                        symbol_raw = item.get("n")
                                        values = item.get("v")
                                        price = values.get("lp")
                                        
                                        if price:
                                            for name, tv_name in PAIRS.items():
                                                if tv_name == symbol_raw:
                                                    market_data[name].append({
                                                        "timestamp": time.time(),
                                                        "price": float(price)
                                                    })
                            except:
                                pass
        except Exception as e:
            print(f"⚠️ Connection dropped: {e}. Reconnecting in 3 seconds...")
            time.sleep(3)

# =====================================================================
# 📊 MATHEMATICAL ANALYSIS WORKER
# =====================================================================
def calculate_rsi_live(prices, period=14):
    if len(prices) < period + 1:
        return 50
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = np.zeros_like(prices)
    rsi[:period+1] = 100. - 100. / (1. + rs)

    for i in range(period + 1, len(prices)):
        delta = deltas[i - 1]
        upval = delta if delta > 0 else 0.
        downval = -delta if delta < 0 else 0.
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)
    return rsi[-1]

def process_signal(pair, direction, rsi_val):
    print(f"🎯 [SIGNAL MATCHED] sending alert for {pair}...")
    
    msg = f"⏳ *[20-SECOND PRE-SIGNAL]* ⏳\n" \
          f"🏆 *PAIR:* {pair[:3]}/{pair[3:]}\n" \
          f"🎯 *DIRECTION:* {direction}\n" \
          f"📊 Live RSI: {rsi_val:.1f}\n" \
          f"⏱️ *EXPIRY:* 1 MINUTE\n\n" \
          f"🚀 _Open asset on Quotex now! Place trade exactly at the start of the next minute candle!_"
          
    send_telegram_signal(msg)
    time.sleep(25)  # Lock execution for safety
    with lock:
        active_tracks.remove(pair)

def execute_analysis_cycle():
    print("🔎 Scanning live feeds for setups...")
    last_processed_minute = -1
    
    while True:
        now = time.localtime()
        # Triggers exactly at the 40th second mark (20 seconds before new candle)
        if now.tm_sec == 40 and now.tm_min != last_processed_minute:
            last_processed_minute = now.tm_min
            
            for pair, ticks in market_data.items():
                with lock:
                    if pair in active_tracks or len(ticks) < 15:
                        continue
                
                df_ticks = pd.DataFrame(ticks)
                current_cutoff = time.time()
                one_minute_ago = current_cutoff - 40
                
                live_ticks = df_ticks[df_ticks['timestamp'] >= one_minute_ago]['price'].tolist()
                historical_ticks = df_ticks[df_ticks['timestamp'] < one_minute_ago]['price'].tolist()
                
                if not live_ticks or len(historical_ticks) < 20:
                    continue
                
                open_p = live_ticks[0]
                close_p = live_ticks[-1]
                high_p = max(live_ticks)
                low_p = min(live_ticks)
                
                total_range = high_p - low_p
                all_prices_sampled = historical_ticks[-50:] + [close_p]
                rsi_value = calculate_rsi_live(all_prices_sampled)
                
                direction = None
                if total_range > 0:
                    lower_wick = min(open_p, close_p) - low_p
                    upper_wick = high_p - max(open_p, close_p)
                    
                    if lower_wick / total_range > 0.45 and close_p > open_p and 45 < rsi_value < 58:
                        direction = "🟢 CALL (UP)"
                    elif upper_wick / total_range > 0.45 and close_p < open_p and 42 < rsi_value < 55:
                        direction = "🔴 PUT (DOWN)"
                
                if direction:
                    with lock:
                        active_tracks.add(pair)
                    t = threading.Thread(target=process_signal, args=(pair, direction, rsi_value))
                    t.daemon = True
                    t.start()
                
                # Housekeeping memory array size optimization
                market_data[pair] = ticks[-500:]
                
        time.sleep(0.2)

if __name__ == "__main__":
    ws_thread = threading.Thread(target=live_websocket_worker, daemon=True)
    ws_thread.start()
    execute_analysis_cycle()
