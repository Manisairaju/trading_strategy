import json
import os
import random
import string
import sys
import time
import threading
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import requests
from websocket import create_connection

# ================================================================
# CONFIGURATION
# ================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("ERROR: TELEGRAM_TOKEN and TELEGRAM_CHAT_ID are required.")
    sys.exit(1)

PAIRS = {
    "USDJPY": "FX_IDC:USDJPY",
    "AUDJPY": "FX_IDC:AUDJPY",
    "NZDJPY": "FX_IDC:NZDJPY",
    "CADJPY": "FX_IDC:CADJPY",
    "EURJPY": "FX_IDC:EURJPY",
    "GBPJPY": "FX_IDC:GBPJPY",
}

TIMEFRAMES = {"M1": 1, "M3": 3, "M5": 5}
EXPIRY_MINUTES = {"M1": 1, "M3": 3, "M5": 5}

# --- INDICATOR SETTINGS ---
# 1. SMA 50
SMA_PERIOD = 50

# 2. ENVELOPES (Period 14, Deviation 0.1%)
ENV_PERIOD = 14
ENV_DEV = 0.1  # Percentage deviation

# 3. STOCHASTIC OSCILLATOR (14, 3, 3)
STOCH_K_PERIOD = 14
STOCH_SMOOTH = 3
STOCH_D_PERIOD = 3
STOCH_OVERSOLD = 20.0
STOCH_OVERBOUGHT = 80.0

# General App Settings
HISTORY_M1_BARS = 3000
lock = threading.RLock()

# Data Storage
candles_m1 = {pair: {} for pair in PAIRS}
last_signal = set()

# ================================================================
# TRADINGVIEW MESSAGE HELPERS
# ================================================================

def generate_session(prefix):
    chars = string.ascii_lowercase
    return prefix + "".join(random.choice(chars) for _ in range(12))

def frame_message(message):
    return f"~m~{len(message)}~m~{message}"

def send_tv_message(ws, function_name, params):
    body = json.dumps({"m": function_name, "p": params}, separators=(",", ":"))
    ws.send(frame_message(body))

def iter_frames(raw):
    if not isinstance(raw, str):
        return
    position = 0
    total = len(raw)
    while position < total:
        if raw.startswith("~m~", position):
            separator = raw.find("~m~", position + 3)
            if separator == -1: return
            try:
                length = int(raw[position + 3: separator])
            except ValueError: return
            start = separator + 3
            payload = raw[start:start + length]
            yield payload
            position = start + length
        else:
            yield raw[position:]
            return

def parse_frames(raw):
    for payload in iter_frames(raw):
        if payload.startswith("~h~"):
            yield "heartbeat", payload
            continue
        try:
            yield ("json", json.loads(payload))
        except Exception:
            continue

# ================================================================
# TELEGRAM ALERTS
# ================================================================

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=8)
        if response.ok:
            return response.json().get("result", {}).get("message_id")
        print("Telegram error:", response.status_code, response.text[:300])
    except Exception as exc:
        print("Telegram exception:", exc)
    return None

# ================================================================
# DATA MANAGEMENT
# ================================================================

def store_m1_candle(pair, candle):
    timestamp = int(candle["time"])
    with lock:
        candles_m1[pair][timestamp] = {
            "time": timestamp,
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
        }
        if len(candles_m1[pair]) > HISTORY_M1_BARS + 200:
            old_keys = sorted(candles_m1[pair])[:-HISTORY_M1_BARS]
            for key in old_keys:
                candles_m1[pair].pop(key, None)

def get_m1_candles(pair, include_current=True):
    current_minute = int(time.time() // 60) * 60
    with lock:
        rows = [candles_m1[pair][key] for key in sorted(candles_m1[pair])]
    if not include_current:
        rows = [row for row in rows if row["time"] < current_minute]
    return rows

def resample_candles(rows, minutes, completed_only=True):
    if not rows: return []
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.sort_values("datetime").drop_duplicates("datetime").set_index("datetime")
    
    if completed_only:
        current_minute = int(time.time() // 60) * 60
        df = df[df["time"] < current_minute]

    result = df.resample(f"{minutes}min", origin="epoch").agg({
        "open": "first", "high": "max", "low": "min", "close": "last"
    }).dropna()

    if result.empty: return []
    result["time"] = result.index.astype("int64") // 10**9
    return result.to_dict("records")

# ================================================================
# STRATEGY & INDICATORS (Highly Efficient Vectorized Pandas)
# ================================================================

def apply_strategy_and_check(df):
    """Calculates SMA50, Envelopes, Stochastic and returns signals (1 for CALL, -1 for PUT, 0 for None)"""
    if len(df) < SMA_PERIOD:
        return 0

    # 1. SMA 50
    df['sma_50'] = df['close'].rolling(SMA_PERIOD).mean()

    # 2. Envelopes (Basis = SMA 14, Upper/Lower = Dev %)
    df['env_basis'] = df['close'].rolling(ENV_PERIOD).mean()
    df['env_upper'] = df['env_basis'] * (1 + (ENV_DEV / 100))
    df['env_lower'] = df['env_basis'] * (1 - (ENV_DEV / 100))

    # 3. Stochastic Oscillator (14, 3, 3)
    low_min = df['low'].rolling(STOCH_K_PERIOD).min()
    high_max = df['high'].rolling(STOCH_K_PERIOD).max()
    
    # Fast %K
    df['stoch_k_fast'] = 100 * ((df['close'] - low_min) / (high_max - low_min))
    # Slow %K
    df['stoch_k'] = df['stoch_k_fast'].rolling(STOCH_SMOOTH).mean()
    # Slow %D
    df['stoch_d'] = df['stoch_k'].rolling(STOCH_D_PERIOD).mean()

    # Evaluate ONLY the most recently CLOSED candle (last row)
    last_idx = df.index[-1]
    prev_idx = df.index[-2]

    # Current values
    c_close = df.at[last_idx, 'close']
    c_high = df.at[last_idx, 'high']
    c_low = df.at[last_idx, 'low']
    c_sma = df.at[last_idx, 'sma_50']
    c_env_up = df.at[last_idx, 'env_upper']
    c_env_low = df.at[last_idx, 'env_lower']
    c_k = df.at[last_idx, 'stoch_k']
    c_d = df.at[last_idx, 'stoch_d']
    
    # Previous values (for crossover check)
    p_k = df.at[prev_idx, 'stoch_k']
    p_d = df.at[prev_idx, 'stoch_d']

    # --- CALL LOGIC ---
    # 1. Uptrend (Close > SMA50)
    # 2. Touches lower envelope (Low <= Env_Lower)
    # 3. Stoch is oversold and K crosses above D
    if (c_close > c_sma and 
        c_low <= c_env_low and 
        c_k < STOCH_OVERSOLD and 
        c_k > c_d and p_k <= p_d):
        return 1

    # --- PUT LOGIC ---
    # 1. Downtrend (Close < SMA50)
    # 2. Touches upper envelope (High >= Env_Upper)
    # 3. Stoch is overbought and K crosses below D
    elif (c_close < c_sma and 
          c_high >= c_env_up and 
          c_k > STOCH_OVERBOUGHT and 
          c_k < c_d and p_k >= p_d):
        return -1

    return 0

# ================================================================
# WEBSOCKET CONNECTIONS (TradingView)
# ================================================================
# (Merged Historical Load and Realtime stream logic into workers)

def tv_realtime_worker():
    while True:
        ws = None
        try:
            ws = create_connection("wss://data.tradingview.com/socket.io/websocket", 
                                   origin="https://data.tradingview.com", timeout=30)
            chart_session = generate_session("cs_")
            quote_session = generate_session("qs_")

            send_tv_message(ws, "set_auth_token", ["unauthorized_user_token"])
            send_tv_message(ws, "chart_create_session", [chart_session, ""])
            send_tv_message(ws, "quote_create_session", [quote_session])
            send_tv_message(ws, "switch_timezone", [chart_session, "Etc/UTC"])

            for index, (pair, symbol) in enumerate(PAIRS.items(), start=1):
                symbol_config = json.dumps({"symbol": symbol, "adjustment": "splits"}, separators=(",", ":"))
                send_tv_message(ws, "resolve_symbol", [chart_session, f"symbol_{index}", "=" + symbol_config])
                # Request history to prime the pump, then switch to realtime
                send_tv_message(ws, "create_series", [chart_session, f"s{index}", f"s{index}", f"symbol_{index}", "1", HISTORY_M1_BARS])

            print("Realtime TV stream connected & fetching initial history...")

            while True:
                raw = ws.recv()
                for kind, message in parse_frames(raw):
                    if kind == "heartbeat":
                        ws.send(message)
                        continue
                    
                    if message.get("m") not in ("timescale_update", "du"):
                        continue
                    
                    params = message.get("p", [])
                    if len(params) < 2: continue
                    data = params[1]
                    if not isinstance(data, dict): continue

                    for index, pair in enumerate(PAIRS, start=1):
                        series = data.get(f"s{index}")
                        if not isinstance(series, dict): continue
                        
                        for item in series.get("s", []):
                            vals = item.get("v")
                            if isinstance(vals, list) and len(vals) >= 5:
                                try:
                                    ts = float(vals[0])
                                    if ts < 1_000_000_000: continue
                                    store_m1_candle(pair, {
                                        "time": int(ts),
                                        "open": float(vals[1]),
                                        "high": float(vals[2]),
                                        "low": float(vals[3]),
                                        "close": float(vals[4]),
                                    })
                                except Exception:
                                    pass

        except Exception as exc:
            print("TV stream disconnected:", exc)
            time.sleep(3)
        finally:
            if ws:
                try: ws.close()
                except Exception: pass

# ================================================================
# SIGNAL SCANNER WORKER
# ================================================================

def signal_scanner_worker():
    last_checked_minute = 0
    print("Scanner active, waiting for candle closures...")

    while True:
        current_time = int(time.time())
        current_minute = current_time // 60

        # Run scan only at the very start of a new minute (when previous candle closes)
        if current_minute > last_checked_minute and current_time % 60 == 1:
            last_checked_minute = current_minute
            
            for pair in PAIRS:
                m1_data = get_m1_candles(pair, include_current=False)
                if not m1_data: continue

                for tf_name, tf_minutes in TIMEFRAMES.items():
                    # Check if this specific timeframe candle just closed (e.g. M5 closes at :00, :05, :10)
                    if current_minute % tf_minutes != 0:
                        continue 

                    resampled = resample_candles(m1_data, tf_minutes, completed_only=True)
                    if len(resampled) < SMA_PERIOD: 
                        continue

                    df = pd.DataFrame(resampled)
                    signal = apply_strategy_and_check(df)

                    if signal != 0:
                        # Prevent duplicate alerts for the exact same timeframe and timestamp
                        signal_id = f"{pair}_{tf_name}_{current_minute}"
                        if signal_id not in last_signal:
                            last_signal.add(signal_id)
                            
                            direction = "🟢 CALL (UP)" if signal == 1 else "🔴 PUT (DOWN)"
                            expiry = EXPIRY_MINUTES[tf_name]
                            price = df.iloc[-1]['close']
                            
                            msg = (
                                f"🔥 *QUOTEX TRIPLE CONFLUENCE SIGNAL* 🔥\n\n"
                                f"💱 *Asset:* {pair} (OTC/Live)\n"
                                f"⏱ *Timeframe:* {tf_name}\n"
                                f"🎯 *Action:* {direction}\n"
                                f"⏳ *Expiry:* {expiry} Minute(s)\n"
                                f"💲 *Close Price:* {price}\n\n"
                                f"✅ _SMA 50 + Envelopes + Stochastic Confirmed_"
                            )
                            send_telegram(msg)
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Signal sent: {pair} {tf_name} {direction}")

        # Sleep a bit to prevent CPU max out
        time.sleep(0.5)

# ================================================================
# MAIN ENTRY
# ================================================================

if __name__ == "__main__":
    print("Starting Quotex Trading Bot...")
    
    # 1. Start TV Websocket Thread
    tv_thread = threading.Thread(target=tv_realtime_worker, daemon=True)
    tv_thread.start()

    # Wait a few seconds to let historical data populate
    print("Waiting 10 seconds for initial historical data to load...")
    time.sleep(10)

    # 2. Start Scanner Thread
    scanner_thread = threading.Thread(target=signal_scanner_worker, daemon=True)
    scanner_thread.start()

    # Keep Main Thread Alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting...")
        sys.exit(0)
        
