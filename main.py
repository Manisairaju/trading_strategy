import time
import requests
import pandas as pd
import threading
import os  # Added to read secrets
import sys # Added for graceful shutdown
from tvDatafeed import TvDatafeed, Interval

# =====================================================================
# 🛠️ CONFIGURATION (SECURED WITH ENVIRONMENT VARIABLES)
# =====================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

AUTO_PAIRS = ["USDINR", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY", "EURJPY", "GBPJPY", "USDJPY"]

# Thread-safe set to track pairs currently processing
active_tracks = set()
lock = threading.Lock()

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("❌ Critical Error: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID environment variables are missing!")
    sys.exit(1)

print("🚀 1-Min Continuous Scanner Engine Online!")
tv = TvDatafeed() 

def send_telegram_signal(message):
    """Sends a direct message to your Telegram channel."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
            print(f"❌ Telegram API Error: {response.text}")
    except Exception as e:
        print(f"❌ Telegram Connection Error: {e}")

def fetch_data_fast(symbol):
    global tv
    try:
        df = tv.get_hist(symbol=symbol, exchange='FX_IDC', interval=Interval.in_1_minute, n_bars=70)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"⚠️ TV Fetch Error for {symbol}: {e}. Attempting re-auth...")
        try: 
            tv = TvDatafeed()
        except: 
            pass
    return None 

def calculate_rsi(series, period=14):
    delta = series.diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def evaluate_setup(df, raw_symbol):
    formatted_name = f"{raw_symbol[:3]}/{raw_symbol[3:]}"
    if df is None or len(df) < 55:  
        return {"is_valid": False, "score": 0, "pair_name": formatted_name}

    df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['RSI_14'] = calculate_rsi(df['close'])
    
    closed_candle = df.iloc[-2]
    close_p = closed_candle['close']
    open_p = closed_candle['open']
    high_p = closed_candle['high']
    low_p = closed_candle['low']
    rsi = closed_candle['RSI_14']
    
    body_size = abs(close_p - open_p)
    total_range = high_p - low_p
    
    df['range'] = df['high'] - df['low']
    avg_range = df['range'].iloc[-12:-2].mean()
    
    momentum_score = body_size / avg_range if avg_range > 0 else 0
    
    is_bullish = (close_p > open_p) and (close_p > closed_candle['EMA_50'])
    is_bearish = (close_p < open_p) and (close_p < closed_candle['EMA_50'])
    
    clean_close = False
    if is_bullish and total_range > 0:
        clean_close = (high_p - close_p) / total_range < 0.35
    elif is_bearish and total_range > 0:
        clean_close = (close_p - low_p) / total_range < 0.35
        
    direction = "🟢 CALL (UP)" if is_bullish else ("🔴 PUT (DOWN)" if is_bearish else "⚪ NEUTRAL")
    is_valid = (is_bullish or is_bearish) and momentum_score > 0.8 and clean_close
    
    return {
        "raw_symbol": raw_symbol,
        "pair_name": formatted_name,
        "score": momentum_score,
        "direction": direction,
        "is_valid": is_valid,
        "close": close_p,
        "rsi": rsi
    }

def process_signal(target):
    pair_name = target['pair_name']
    raw_symbol = target['raw_symbol']
    direction = target['direction']
    
    print(f"🎯 [MATCH FOUND] {pair_name} | Score: {target['score']:.2f} | Sending Alerts...")
    
    msg = f"⏳ *[SIGNAL ALERT]* ⏳\n" \
          f"🏆 *PAIR:* {pair_name}\n" \
          f"🎯 *DIRECTION:* {direction}\n" \
          f"📊 RSI: {target['rsi']:.1f} | Score: {target['score']:.2f}\n" \
          f"⏱️ *EXPIRY:* 1 MINUTE\n\n" \
          f"🚀 _Execute trade immediately at the opening of the new candle on Quotex!_"
    
    send_telegram_signal(msg)
    time.sleep(10)
    with lock:
        active_tracks.remove(raw_symbol)

def live_market_runner():
    # Record the startup time to calculate 6 hours max runtime
    start_time = time.time()
    max_runtime_seconds = 5.5 * 3600  # Run safely for 5.5 hours to avoid GitHub Actions timing out abruptly
    
    print("⏳ Synchronizing to the next clean 1-minute candle block...")
    while True:
        if time.localtime().tm_sec == 0:
            break
        time.sleep(0.2)
        
    print("🟩 Continuous 1-Minute Engine Active!")

    while True:
        # Check if 5.5 hours have passed, then shut down cleanly
        if (time.time() - start_time) > max_runtime_seconds:
            print("⏰ Reached end of daily trading window. Shutting down system down gracefully.")
            sys.exit(0)

        current_time = time.localtime()
        print(f"🔎 [SCANNING] Time: {current_time.tm_hour:02d}:{current_time.tm_min:02d}:00")
        
        for symbol in AUTO_PAIRS:
            with lock:
                if symbol in active_tracks:
                    continue
            
            df = fetch_data_fast(symbol)
            metrics = evaluate_setup(df, symbol)
            
            if metrics['is_valid']:
                with lock:
                    active_tracks.add(symbol)
                
                t = threading.Thread(target=process_signal, args=(metrics,))
                t.daemon = True
                t.start()
        
        now = time.time()
        time_to_next_minute = 60 - (now % 60)
        time.sleep(time_to_next_minute)

if __name__ == "__main__":
    live_market_runner()
