import json
import random
import threading
import time
import os
import sys
import requests
import numpy as np
from websocket import create_connection

# =====================================================================
# 🛠️ SYSTEM CONFIGURATION
# =====================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("❌ Critical Error: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID Secrets are missing!")
    sys.exit(1)

PAIRS = {
    "USDJPY": "FX_IDC:USDJPY",
    "AUDJPY": "FX_IDC:AUDJPY",
    "NZDJPY": "FX_IDC:NZDJPY",
    "CADJPY": "FX_IDC:CADJPY",
    "EURJPY": "FX_IDC:EURJPY",
    "GBPJPY": "FX_IDC:GBPJPY"
}

raw_tick_storage = {pair: {} for pair in PAIRS.keys()}
historical_candles = {pair: [] for pair in PAIRS.keys()}
active_tracks = set()
lock = threading.Lock()

def send_telegram_signal(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=4)
        return response.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"❌ Telegram Error: {e}")
        return None

def edit_telegram_message(message_id, updated_text):
    """Updates the original pre-signal message in real-time to avoid chat clutter."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    payload = {
        "chat_id": CHAT_ID,
        "message_id": message_id,
        "text": updated_text,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=4)
    except Exception as e:
        print(f"❌ Telegram Edit Error: {e}")

def generate_session_id():
    return "".join(random.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(12))

def prepend_header(st):
    return f"~m~{len(st)}~m~{st}"

def create_message(func, param_list):
    return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

def live_websocket_worker():
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
            
            print("🟩 Dual-Stage Confirmation Engine Live.")
            
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
                                                    now_sec = time.time()
                                                    minute_floor = int(now_sec // 60) * 60
                                                    
                                                    with lock:
                                                        if minute_floor not in raw_tick_storage[name]:
                                                            raw_tick_storage[name][minute_floor] = []
                                                        raw_tick_storage[name][minute_floor].append(float(price))
                            except:
                                pass
        except Exception as e:
            time.sleep(3)

def calculate_true_rsi(candle_closes, period=14):
    if len(candle_closes) < period + 1:
        return 50.0
    deltas = np.diff(candle_closes)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    if down == 0: return 100.0
    return 100. - (100. / (1. + (up / down)))

# =====================================================================
# 🔍 DUAL-STAGE VERIFICATION LOOP
# =====================================================================
def verify_at_second_55(pair, direction, rsi_val, msg_id, minute_floor):
    """Runs a hard checkpoint 5 seconds before candle close to confirm or abort."""
    time.sleep(10) # Wait from second 45 to second 55
    
    with lock:
        live_ticks = list(raw_tick_storage[pair].get(minute_floor, []))
        
    if len(live_ticks) < 5:
        # Data stream dropped, abort for safety
        abort_text = f"❌ *[SIGNAL CANCELLED]* ❌\n\n🏆 *PAIR:* {pair[:3]}/{pair[3:]}\n⚠️ *REASON:* Feed lag detected at second 55. *DO NOT TAKE THIS TRADE!*"
        edit_telegram_message(msg_id, abort_text)
        with lock: active_tracks.remove(pair)
        return

    c_open = live_ticks[0]
    c_close = live_ticks[-1]
    c_high = max(live_ticks)
    c_low = min(live_ticks)
    c_range = c_high - c_low

    is_valid = False
    
    # Check if the rejection wick structural ratio is still intact at second 55
    if c_range > 0:
        if direction == "🟢 CALL (UP)":
            lower_wick = c_open - c_low
            # Reversal criteria check: Wick must maintain structural dominance
            if (lower_wick / c_range) >= 0.45 and c_close >= c_open:
                is_valid = True
        elif direction == "🔴 PUT (DOWN)":
            upper_wick = c_high - c_open
            if (upper_wick / c_range) >= 0.45 and c_close <= c_open:
                is_valid = True

    if is_valid:
        confirm_text = f"✅ *[TAKE TRADE NOW!]* ✅\n\n" \
                       f"🏆 *PAIR:* {pair[:3]}/{pair[3:]}\n" \
                       f"🎯 *ACTION:* {direction}\n" \
                       f"📊 True RSI: {rsi_val:.1f}\n" \
                       f"⏱️ *EXPIRY:* 1 MINUTE\n\n" \
                       f"🚀 _The rejection shape held at second 55. Execute your entry on Quotex exactly at the 00:00 countdown transition!_"
        edit_telegram_message(msg_id, confirm_text)
    else:
        reject_text = f"❌ *[DO NOT TAKE - CANCELLED]* ❌\n\n" \
                      f"🏆 *PAIR:* {pair[:3]}/{pair[3:]}\n" \
                      f"⚠️ *REASON:* Candle reversal occurred between seconds 45 and 55. The rejection pattern collapsed. *SKIP THIS TRADE!*"
                      
        edit_telegram_message(msg_id, reject_text)

    time.sleep(15)
    with lock:
        active_tracks.remove(pair)

def execute_analysis_cycle():
    print("🔎 Scanning order flow matrix for high-probability setups...")
    last_processed_minute = -1
    
    while True:
        now = time.localtime()
        if now.tm_sec == 45 and now.tm_min != last_processed_minute:
            last_processed_minute = now.tm_min
            current_time_sec = time.time()
            minute_floor = int(current_time_sec // 60) * 60
            
            for pair in PAIRS.keys():
                with lock:
                    if pair in active_tracks: continue
                        
                    for past_min in list(raw_tick_storage[pair].keys()):
                        if past_min < minute_floor:
                            ticks = raw_tick_storage[pair].pop(past_min)
                            if len(ticks) >= 5:
                                historical_candles[pair].append([ticks[0], max(ticks), min(ticks), ticks[-1]])
                                if len(historical_candles[pair]) > 30: historical_candles[pair].pop(0)
                
                with lock:
                    live_ticks = list(raw_tick_storage[pair].get(minute_floor, []))
                    history = list(historical_candles[pair])
                
                if len(live_ticks) < 10 or len(history) < 15: continue
                
                c_open, c_close = live_ticks[0], live_ticks[-1]
                c_high, c_low = max(live_ticks), min(live_ticks)
                c_range = c_high - c_low
                if c_range == 0: continue
                
                rsi_value = calculate_true_rsi([candle[3] for candle in history] + [c_close])
                liquidity_pool_high = max([candle[1] for candle in history[-5:]])
                liquidity_pool_low = min([candle[2] for candle in history[-5:]])
                
                direction = None
                if c_high >= liquidity_pool_high and c_close < c_open:
                    if ((c_high - c_open) / c_range) > 0.50 and rsi_value > 68:
                        direction = "🔴 PUT (DOWN)"
                elif c_low <= liquidity_pool_low and c_close > c_open:
                    if ((c_open - c_low) / c_range) > 0.50 and rsi_value < 32:
                        direction = "🟢 CALL (UP)"
                
                if direction:
                    with lock: active_tracks.add(pair)
                    
                    # Stage 1 Message: Pre-Signal Sent at Second 45
                    pre_msg = f"⏳ *[15-SECOND PRE-SIGNAL]* ⏳\n\n" \
                              f"🏆 *PAIR:* {pair[:3]}/{pair[3:]}\n" \
                              f"🎯 *POTENTIAL DIRECTION:* {direction}\n" \
                              f"⏱️ *EXPIRY:* 1 MINUTE\n\n" \
                              f"⚠️ *ACTION:* Prepare your pair and trade amount on Quotex now. *DO NOT entry yet.* Wait for second 55 automated check updates below..."
                    
                    msg_id = send_telegram_signal(pre_msg)
                    
                    if msg_id:
                        # Spin up tracking thread to evaluate live candle changes at second 55
                        t = threading.Thread(target=verify_at_second_55, args=(pair, direction, rsi_value, msg_id, minute_floor))
                        t.daemon = True
                        t.start()
                    else:
                        with lock: active_tracks.remove(pair)
                        
        time.sleep(0.1)

if __name__ == "__main__":
    ws_thread = threading.Thread(target=live_websocket_worker, daemon=True)
    ws_thread.start()
    execute_analysis_cycle()
