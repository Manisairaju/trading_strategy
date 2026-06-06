import os
import time
import requests
import pandas as pd
import threading
from tvDatafeed import TvDatafeed, Interval

# =====================================================================
# 🛠️ CONFIGURATION & SECURITY
# =====================================================================
# These lines securely fetch the hidden keys you saved in GitHub Secrets!
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")          

AUTO_PAIRS = ["USDINR", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY", "EURJPY", "GBPJPY", "USDJPY"]

# Thread-safe tracking system
active_tracks = set()
lock = threading.Lock()

print("🚀 High-Accuracy 1-Min Multi-Indicator Scanner Online!")
tv = TvDatafeed() 

def send_telegram_signal(message):
    """Sends a formatted notification to your Telegram channel."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
            print(f"❌ Telegram API Error: {response.text}")
    except Exception as e:
        print(f"❌ Telegram Connection Error: {e}")

def fetch_data_fast(symbol):
    """Fetches historical bars securely from TradingView."""
    global tv
    try:
        # Request 100 bars to smoothly calculate SMA 50 and dynamic volatility boundaries
        df = tv.get_hist(symbol=symbol, exchange='FX_IDC', interval=Interval.in_1_minute, n_bars=100)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"⚠️ TV Fetch Error for {symbol}: {e}. Re-authenticating...")
        try: 
            tv = TvDatafeed()
        except: 
            pass
    return None 

def calculate_rsi(series, period=14):
    """Calculates Wilder's RSI using Exponential Smoothing to fix calculation lag."""
    delta = series.diff(1)
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))

def calculate_stochastic(df, k_period=14, d_period=3):
    """Calculates Fast Stochastic Oscillator (%K and %D momentum lines)."""
    low_min = df['low'].rolling(window=k_period).min()
    high_max = df['high'].rolling(window=k_period).max()
    df['%K'] = 100 * ((df['close'] - low_min) / (high_max - low_min).replace(0, 1e-9))
    df['%D'] = df['%K'].rolling(window=d_period).mean()
    return df

def evaluate_setup(df, raw_symbol):
    """Applies strict rule-based criteria to identify high-accuracy setups."""
    formatted_name = f"{raw_symbol[:3]}/{raw_symbol[3:]}"
    if df is None or len(df) < 60:
        return {"is_valid": False, "pair_name": formatted_name}

    # 1. Indicator Math Core
    df['SMA_10'] = df['close'].rolling(window=10).mean()
    df['SMA_50'] = df['close'].rolling(window=50).mean()
    
    # Envelopes Engine (SMA 50 with a highly refined 0.10% dynamic shift band)
    deviation = 0.0010 
    df['Env_Upper'] = df['SMA_50'] * (1 + deviation)
    df['Env_Lower'] = df['SMA_50'] * (1 - deviation)
    
    df = calculate_stochastic(df)
    df['RSI_14'] = calculate_rsi(df['close'])
    
    # 2. Extract Last Completely Formed Candle Metrics (Index -2 prevents repainting errors)
    closed_candle = df.iloc[-2]
    close_p = closed_candle['close']
    open_p = closed_candle['open']
    high_p = closed_candle['high']
    low_p = closed_candle['low']
    
    sma10 = closed_candle['SMA_10']
    sma50 = closed_candle['SMA_50']
    stoch_k = closed_candle['%K']
    stoch_d = closed_candle['%D']
    rsi = closed_candle['RSI_14']
    env_upper = closed_candle['Env_Upper']
    env_lower = closed_candle['Env_Lower']
    
    # 3. Dynamic Institutional Momentum Calculation
    body_size = abs(close_p - open_p)
    total_range = high_p - low_p
    df['range'] = df['high'] - df['low']
    avg_range = df['range'].iloc[-12:-2].mean()
    momentum_score = body_size / avg_range if avg_range > 0 else 0

    # 4. Multilateral Filter Checks
    trend_bullish = sma10 > sma50
    trend_bearish = sma10 < sma50
    
    # Ensuring candle doesn't have a massive exhausting upper or lower wick
    clean_close_bull = total_range > 0 and ((high_p - close_p) / total_range < 0.25)
    clean_close_bear = total_range > 0 and ((close_p - low_p) / total_range < 0.25)

    # 🟢 High Accuracy CALL Condition
    signal_call = (
        trend_bullish and 
        close_p > open_p and 
        close_p >= env_upper and        # Price breaches/rides the upper structural envelope boundary
        stoch_k > stoch_d and           # Stochastic bullish cross validation
        stoch_k < 80 and                # Filters out exhausted assets that are already overbought
        clean_close_bull and
        momentum_score > 1.1            # Confirms institutional volume pump
    )

    # 🔴 High Accuracy PUT Condition
    signal_put = (
        trend_bearish and 
        close_p < open_p and 
        close_p <= env_lower and        # Price breaches/rides the lower structural envelope boundary
        stoch_k < stoch_d and           # Stochastic bearish cross validation
        stoch_k > 20 and                # Filters out exhausted assets that are already oversold
        clean_close_bear and
        momentum_score > 1.1            # Confirms institutional volume pump
    )
    
    direction = "⚪ NEUTRAL"
    is_valid = False
    
    if signal_call:
        direction = "🟢 CALL (UP)"
        is_valid = True
    elif signal_put:
        direction = "🔴 PUT (DOWN)"
        is_valid = True
        
    return {
        "raw_symbol": raw_symbol,
        "pair_name": formatted_name,
        "score": momentum_score,
        "direction": direction,
        "is_valid": is_valid,
        "close": close_p,
        "rsi": rsi,
        "stoch_k": stoch_k
    }

def process_signal(target):
    """Dispatches background alerts instantly to minimize network lag latency."""
    pair_name = target['pair_name']
    raw_symbol = target['raw_symbol']
    direction = target['direction']
    
    print(f"🎯 [MATCH FOUND] {pair_name} | Sending Alerts...")
    
    msg = f"🚨 *[HIGH WIN-RATE SIGNAL]* 🚨\n\n"
    msg += f"🏆 *PAIR:* {pair_name}\n"
    msg += f"🎯 *ACTION:* {direction}\n"
    msg += f"📊 *RSI:* {target['rsi']:.1f} | *Stoch %K:* {target['stoch_k']:.1f}\n"
    msg += f"🔥 *Momentum Force:* {target['score']:.2f}x\n"
    msg += f"⏱️ *EXPIRY:* 1 MINUTE\n\n"
    msg += f"⚡ _Execute immediately at the dynamic opening of the new Quotex candlestick!_"
    
    send_telegram_signal(msg)
    
    # Keep the thread locked for 15 seconds to safely transition past the candle change boundary
    time.sleep(15)
    with lock:
        active_tracks.remove(raw_symbol)

def live_market_runner():
    """Initializes synchronization loop framework."""
    print("⏳ Synchronizing to the next clean clock minute-block...")
    while True:
        if time.localtime().tm_sec == 0:
            break
        time.sleep(0.1)
        
    print("🟩 Continuous 1-Minute Aggressive Engine Active!")

    while True:
        current_time = time.localtime()
        print(f"🔎 [SCANNING] Time: {current_time.tm_hour:02d}:{current_time.tm_min:02d}:00")
        
        for symbol in AUTO_PAIRS:
            with lock:
                if symbol in active_tracks:
                    continue
            
            df = fetch_data_fast(symbol)
            metrics = evaluate_setup(df, symbol)
            
            if metrics.get('is_valid', False):
                with lock:
                    active_tracks.add(symbol)
                
                t = threading.Thread(target=process_signal, args=(metrics,))
                t.daemon = True
                t.start()
        
        # Drift-free internal scheduling calculation loop
        now = time.time()
        time_to_next_minute = 60 - (now % 60)
        time.sleep(time_to_next_minute)

if __name__ == "__main__":
    live_market_runner()
