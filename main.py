import time
import requests
import pandas as pd
import threading
import os
import sys
from tvDatafeed import TvDatafeed, Interval

# =====================================================================
# 🛠️ CONFIGURATION (SECURED WITH ENVIRONMENT VARIABLES)
# =====================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

AUTO_PAIRS = ["USDINR", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY", "EURJPY", "GBPJPY", "USDJPY"]

active_tracks = set()
lock = threading.Lock()

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("❌ Critical Error: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID environment variables are missing!")
    sys.exit(1)

print("🚀 High-Accuracy 1-Min Continuous Scanner Engine Online!")
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
        # Increased to 220 bars to perfectly calculate EMA 200 without accuracy decay
        df = tv.get_hist(symbol=symbol, exchange='FX_IDC', interval=Interval.in_1_minute, n_bars=220)
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

def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    return true_range.rolling(window=period).mean()

def evaluate_setup(df, raw_symbol):
    formatted_name = f"{raw_symbol[:3]}/{raw_symbol[3:]}"
    if df is None or len(df) < 210:  
        return {"is_valid": False, "score": 0, "pair_name": formatted_name}

    # Technical Indicators
    df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['EMA_200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['RSI_14'] = calculate_rsi(df['close'])
    df['ATR_14'] = calculate_atr(df)
    
    # Analyze the LAST CLOSED candle (Index -2)
    closed_candle = df.iloc[-2]
    close_p = closed_candle['close']
    open_p = closed_candle['open']
    high_p = closed_candle['high']
    low_p = closed_candle['low']
    rsi = closed_candle['RSI_14']
    atr = closed_candle['ATR_14']
    
    body_size = abs(close_p - open_p)
    total_range = high_p - low_p
    
    df['range'] = df['high'] - df['low']
    avg_range = df['range'].iloc[-12:-2].mean()
    
    # 1. Momentum Score Check
    momentum_score = body_size / avg_range if avg_range > 0 else 0
    
    # 2. Strict Trend-Following Filter (Aligning with Major Trend)
    is_bullish_trend = (close_p > closed_candle['EMA_50']) and (closed_candle['EMA_50'] > closed_candle['EMA_200'])
    is_bearish_trend = (close_p < closed_candle['EMA_50']) and (closed_candle['EMA_50'] < closed_candle['EMA_200'])
    
    is_bullish_candle = (close_p > open_p) and is_bullish_trend
    is_bearish_candle = (close_p < open_p) and is_bearish_trend
    
    # 3. Clean Close Filter (Avoid long reversal shadows)
    clean_close = False
    if is_bullish_candle and total_range > 0:
        clean_close = (high_p - close_p) / total_range < 0.25  # Tightened from 0.35 to 0.25
    elif is_bearish_candle and total_range > 0:
        clean_close = (close_p - low_p) / total_range < 0.25  # Tightened from 0.35 to 0.25
        
    # 4. Math-Driven RSI Anti-Reversal Guard
    rsi_safe = False
    if is_bullish_candle and rsi < 65:        # Do not buy overbought tops
        rsi_safe = True
    elif is_bearish_candle and rsi > 35:      # Do not sell oversold bottoms
        rsi_safe = True

    # 5. Volatility Filter
    has_volume = total_range > (0.7 * atr) if atr > 0 else False

    direction = "🟢 CALL (UP)" if is_bullish_candle else ("🔴 PUT (DOWN)" if is_bearish_candle else "⚪ NEUTRAL")
    
    # Master Accuracy Checklist
    is_valid = is_bullish_candle or is_bearish_candle
    is_valid = is_valid and momentum_score > 1.15            # Raised requirement for stronger breakouts
    is_valid = is_valid and clean_close 
    is_valid = is_valid and rsi_safe
    is_valid = is_valid and has_volume
    
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
    
    print(f"🎯 [HIGH ACCURACY MATCH] {pair_name} | Score: {target['score']:.2f} | Sending Alerts...")
    
    msg = f"🔥 *[HIGH ACCURACY SIGNAL]* 🔥\n" \
          f"🏆 *PAIR:* {pair_name}\n" \
          f"🎯 *DIRECTION:* {direction}\n" \
          f"📊 RSI: {target['rsi']:.1f} | Breakout Strength: {target['score']:.2f}\n" \
          f"⏱️ *EXPIRY:* 1 MINUTE\n\n" \
          f"🚀 _Execute trade swiftly at the opening of the new candle on Quotex!_"
    
    send_telegram_signal(msg)
    time.sleep(10)
    with lock:
        active_tracks.remove(raw_symbol)

def live_market_runner():
    start_time = time.time()
    max_runtime_seconds = 5.5 * 3600  # 5.5 Hours clean execution
    
    print("⏳ Synchronizing to the next clean 1-minute candle block...")
    while True:
        if time.localtime().tm_sec == 0:
            break
        time.sleep(0.2)
        
    print("🟩 Continuous High-Accuracy 1-Minute Engine Active!")

    while True:
        if (time.time() - start_time) > max_runtime_seconds:
            print("⏰ Session window closed. Shutting down system down gracefully.")
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
