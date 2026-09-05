 
import json
import random
import threading
import time
import os
import sys
import requests
import numpy as np
import pandas as pd
from websocket import create_connection

# ================================================================
# CONFIGURATION
# ================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("ERROR: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID missing")
    sys.exit(1)

PAIRS = {
    "USDJPY": "FX_IDC:USDJPY",
    "AUDJPY": "FX_IDC:AUDJPY",
    "NZDJPY": "FX_IDC:NZDJPY",
    "CADJPY": "FX_IDC:CADJPY",
    "EURJPY": "FX_IDC:EURJPY",
    "GBPJPY": "FX_IDC:GBPJPY",
}

TIMEFRAMES = {
    "M1": 1,
    "M3": 3,
    "M5": 5,
}

RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0

RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

MIN_SCORE = 8

# Number of historical M1 candles retained
MAX_M1_CANDLES = 500

# Avoid repeatedly signalling same pair/timeframe
signal_lock = set()

lock = threading.Lock()

raw_ticks = {pair: {} for pair in PAIRS}
m1_history = {pair: [] for pair in PAIRS}

# ================================================================
# TELEGRAM
# ================================================================

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }

    try:
        r = requests.post(url, json=payload, timeout=5)

        if r.status_code == 200:
            return r.json().get("result", {}).get("message_id")

        print("Telegram error:", r.text)

    except Exception as e:
        print("Telegram exception:", e)

    return None


# ================================================================
# HELPERS
# ================================================================

def generate_session_id():
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(chars) for _ in range(12))


def prepend_header(text):
    return f"~m~{len(text)}~m~{text}"


def create_message(func, params):
    return json.dumps(
        {"m": func, "p": params},
        separators=(",", ":")
    )


# ================================================================
# TRADINGVIEW WEBSOCKET
# ================================================================

def websocket_worker():

    while True:

        try:

            ws = create_connection(
                "wss://data.tradingview.com/socket.io/1/websocket/xhr",
                origin="https://www.tradingview.com",
                timeout=15
            )

            session = "qs_" + generate_session_id()

            ws.send(
                prepend_header(
                    create_message(
                        "set_auth_token",
                        ["unauthorized_user_token"]
                    )
                )
            )

            ws.send(
                prepend_header(
                    create_message(
                        "chart_create_session",
                        [session, ""]
                    )
                )
            )

            for name, symbol in PAIRS.items():

                ws.send(
                    prepend_header(
                        create_message(
                            "quote_add_symbols",
                            [
                                session,
                                symbol,
                                {"flags": ["force_permission"]}
                            ]
                        )
                    )
                )

            print("TradingView feed connected")

            while True:

                result = ws.recv()

                if not result:
                    continue

                if result.startswith("~h~"):
                    ws.send(result)
                    continue

                parts = result.split("~m~")

                for part in parts:

                    if "{" not in part:
                        continue

                    try:

                        data = json.loads(part)

                        if data.get("m") != "q_sd":
                            continue

                        params = data.get("p", [])

                        for item in params:

                            if not isinstance(item, dict):
                                continue

                            values = item.get("v")

                            if not isinstance(values, dict):
                                continue

                            symbol = item.get("n")
                            price = values.get("lp")

                            if price is None:
                                continue

                            pair = None

                            for name, tv_symbol in PAIRS.items():

                                if tv_symbol == symbol:
                                    pair = name
                                    break

                            if pair is None:
                                continue

                            now = time.time()

                            minute = int(now // 60) * 60

                            with lock:

                                if minute not in raw_ticks[pair]:
                                    raw_ticks[pair][minute] = []

                                raw_ticks[pair][minute].append(
                                    float(price)
                                )

                    except Exception:
                        continue

        except Exception as e:

            print("WebSocket disconnected:", e)

            time.sleep(3)


# ================================================================
# CANDLE CREATION
# ================================================================

def build_closed_m1_candles():

    current_minute = int(time.time() // 60) * 60

    for pair in PAIRS:

        with lock:

            old_minutes = [
                m for m in raw_ticks[pair]
                if m < current_minute
            ]

            for minute in sorted(old_minutes):

                ticks = raw_ticks[pair].pop(minute)

                if len(ticks) < 3:
                    continue

                candle = {
                    "time": minute,
                    "open": ticks[0],
                    "high": max(ticks),
                    "low": min(ticks),
                    "close": ticks[-1],
                }

                m1_history[pair].append(candle)

                if len(m1_history[pair]) > MAX_M1_CANDLES:
                    m1_history[pair].pop(0)


# ================================================================
# RESAMPLE M1 -> M3 / M5 / H1
# ================================================================

def resample_candles(candles, minutes):

    if not candles:
        return []

    df = pd.DataFrame(candles)

    df["datetime"] = pd.to_datetime(
        df["time"],
        unit="s"
    )

    df = df.set_index("datetime")

    rule = f"{minutes}min"

    result = df.resample(rule).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last"
    })

    result = result.dropna()

    result["time"] = (
        result.index.astype("int64") // 10**9
    )

    return result.reset_index(drop=True).to_dict("records")


# ================================================================
# RSI - WILDER
# ================================================================

def calculate_rsi(closes, period=14):

    if len(closes) < period + 1:
        return np.nan

    closes = np.asarray(closes, dtype=float)

    delta = np.diff(closes)

    gains = np.where(delta > 0, delta, 0)
    losses = np.where(delta < 0, -delta, 0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    if avg_loss == 0:
        return 100.0

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1)) +
            gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) +
            losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# ================================================================
# BOLLINGER BANDS
# ================================================================

def calculate_bollinger(closes, period=20, std_mult=2):

    if len(closes) < period:
        return None

    series = pd.Series(closes)

    middle = series.rolling(period).mean().iloc[-1]

    std = series.rolling(period).std(ddof=0).iloc[-1]

    upper = middle + std_mult * std

    lower = middle - std_mult * std

    return {
        "middle": float(middle),
        "upper": float(upper),
        "lower": float(lower),
    }


# ================================================================
# PIN BAR DETECTION
# ================================================================

def bullish_pin_bar(candle):

    o = candle["open"]
    h = candle["high"]
    l = candle["low"]
    c = candle["close"]

    candle_range = h - l

    if candle_range <= 0:
        return False

    body = abs(c - o)

    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l

    # Body must be relatively small
    if body > candle_range * 0.35:
        return False

    # Lower wick must dominate
    if lower_wick < body * 2:
        return False

    if lower_wick < upper_wick * 1.5:
        return False

    # Close should be in upper portion
    if c < l + candle_range * 0.55:
        return False

    return True


def bearish_pin_bar(candle):

    o = candle["open"]
    h = candle["high"]
    l = candle["low"]
    c = candle["close"]

    candle_range = h - l

    if candle_range <= 0:
        return False

    body = abs(c - o)

    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l

    if body > candle_range * 0.35:
        return False

    if upper_wick < body * 2:
        return False

    if upper_wick < lower_wick * 1.5:
        return False

    if c > h - candle_range * 0.55:
        return False

    return True


# ================================================================
# ENGULFING
# ================================================================

def bullish_engulfing(previous, current):

    prev_o = previous["open"]
    prev_c = previous["close"]

    cur_o = current["open"]
    cur_c = current["close"]

    previous_bearish = prev_c < prev_o
    current_bullish = cur_c > cur_o

    if not (previous_bearish and current_bullish):
        return False

    return (
        cur_o <= prev_c and
        cur_c >= prev_o
    )


def bearish_engulfing(previous, current):

    prev_o = previous["open"]
    prev_c = previous["close"]

    cur_o = current["open"]
    cur_c = current["close"]

    previous_bullish = prev_c > prev_o
    current_bearish = cur_c < cur_o

    if not (previous_bullish and current_bearish):
        return False

    return (
        cur_o >= prev_c and
        cur_c <= prev_o
    )


# ================================================================
# SUPPORT / RESISTANCE
# ================================================================

def support_resistance(candles, lookback=30):

    if len(candles) < lookback:
        return None, None

    recent = candles[-lookback:]

    support = min(
        candle["low"]
        for candle in recent
    )

    resistance = max(
        candle["high"]
        for candle in recent
    )

    return support, resistance


def near_level(price, level, tolerance):

    if level is None:
        return False

    return abs(price - level) <= tolerance


# ================================================================
# H1 TREND
# ================================================================

def get_h1_trend(m1_candles):

    h1 = resample_candles(m1_candles, 60)

    if len(h1) < 30:
        return "UNKNOWN"

    closes = [
        c["close"]
        for c in h1
    ]

    fast = np.mean(closes[-10:])
    slow = np.mean(closes[-25:])

    recent = h1[-3:]

    bullish_structure = (
        recent[-1]["high"] >= recent[-2]["high"] and
        recent[-1]["low"] >= recent[-2]["low"]
    )

    bearish_structure = (
        recent[-1]["high"] <= recent[-2]["high"] and
        recent[-1]["low"] <= recent[-2]["low"]
    )

    if fast > slow and bullish_structure:
        return "BULLISH"

    if fast < slow and bearish_structure:
        return "BEARISH"

    return "NEUTRAL"


# ================================================================
# ANALYZE ONE TIMEFRAME
# ================================================================

def analyze_timeframe(pair, timeframe):

    with lock:
        candles = list(m1_history[pair])

    minutes = TIMEFRAMES[timeframe]

    tf_candles = resample_candles(
        candles,
        minutes
    )

    # Need enough completed candles
    if len(tf_candles) < 40:
        return None

    # IMPORTANT:
    # We only use completed candles.
    # The last currently-forming candle is not included
    # because m1_history only contains closed M1 candles.

    current = tf_candles[-1]

    previous = tf_candles[-2]

    closes = [
        c["close"]
        for c in tf_candles
    ]

    rsi = calculate_rsi(
        closes,
        RSI_PERIOD
    )

    bb = calculate_bollinger(
        closes,
        BB_PERIOD,
        BB_STD
    )

    if np.isnan(rsi) or bb is None:
        return None

    h1_trend = get_h1_trend(candles)

    support, resistance = support_resistance(
        tf_candles,
        min(30, len(tf_candles))
    )

    candle_range = current["high"] - current["low"]

    if candle_range <= 0:
        return None

    tolerance = candle_range * 0.50

    score_call = 0
    score_put = 0

    reasons_call = []
    reasons_put = []

    # ------------------------------------------------------------
    # CALL
    # ------------------------------------------------------------

    if h1_trend == "BULLISH":

        score_call += 2
        reasons_call.append("H1 bullish")

    if rsi < RSI_OVERSOLD:

        score_call += 2
        reasons_call.append(
            f"RSI oversold ({rsi:.1f})"
        )

    if current["low"] <= bb["lower"]:

        score_call += 2
        reasons_call.append("Lower BB touch")

    bullish_pin = bullish_pin_bar(current)

    bullish_engulf = bullish_engulfing(
        previous,
        current
    )

    if bullish_pin:

        score_call += 2
        reasons_call.append("Bullish pin bar")

    elif bullish_engulf:

        score_call += 2
        reasons_call.append("Bullish engulfing")

    if near_level(
        current["low"],
        support,
        tolerance
    ):

        score_call += 1
        reasons_call.append("Near support")

    if current["close"] > current["open"]:

        score_call += 1
        reasons_call.append("Bullish close")

    # ------------------------------------------------------------
    # PUT
    # ------------------------------------------------------------

    if h1_trend == "BEARISH":

        score_put += 2
        reasons_put.append("H1 bearish")

    if rsi > RSI_OVERBOUGHT:

        score_put += 2
        reasons_put.append(
            f"RSI overbought ({rsi:.1f})"
        )

    if current["high"] >= bb["upper"]:

        score_put += 2
        reasons_put.append("Upper BB touch")

    bearish_pin = bearish_pin_bar(current)

    bearish_engulf = bearish_engulfing(
        previous,
        current
    )

    if bearish_pin:

        score_put += 2
        reasons_put.append("Bearish pin bar")

    elif bearish_engulf:

        score_put += 2
        reasons_put.append("Bearish engulfing")

    if near_level(
        current["high"],
        resistance,
        tolerance
    ):

        score_put += 1
        reasons_put.append("Near resistance")

    if current["close"] < current["open"]:

        score_put += 1
        reasons_put.append("Bearish close")

    # ------------------------------------------------------------
    # FINAL DECISION
    # ------------------------------------------------------------

    direction = None
    score = 0
    reasons = []

    if score_call >= MIN_SCORE and score_call > score_put:

        direction = "CALL"
        score = score_call
        reasons = reasons_call

    elif score_put >= MIN_SCORE and score_put > score_call:

        direction = "PUT"
        score = score_put
        reasons = reasons_put

    if direction is None:
        return None

    # The signal is for the NEXT candle.
    next_candle_start = (
        int(current["time"]) +
        minutes * 60
    )

    return {
        "pair": pair,
        "timeframe": timeframe,
        "direction": direction,
        "score": score,
        "rsi": rsi,
        "bb": bb,
        "trend": h1_trend,
        "candle_time": current["time"],
        "next_candle": next_candle_start,
        "reasons": reasons,
    }


# ================================================================
# TELEGRAM FORMAT
# ================================================================

def format_signal(signal):

    direction = signal["direction"]

    emoji = "🟢" if direction == "CALL" else "🔴"

    pair = signal["pair"]

    pair_display = (
        pair[:3] +
        "/" +
        pair[3:]
    )

    timeframe = signal["timeframe"]

    expiry = TIMEFRAMES[timeframe]

    entry_time = time.strftime(
        "%H:%M:%S",
        time.localtime(
            signal["next_candle"]
        )
    )

    reasons = "\n".join(
        "• " + reason
        for reason in signal["reasons"]
    )

    return (
        "━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} *HIGH-CONFLUENCE SIGNAL*\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        f"🏆 *PAIR:* {pair_display}\n"
        f"📊 *TIMEFRAME:* {timeframe}\n"
        f"🎯 *ACTION:* {direction}\n"
        f"⏱️ *EXPIRY:* {expiry} MINUTE"
        f"{'' if expiry == 1 else 'S'}\n\n"

        f"📈 *RSI(14):* {signal['rsi']:.1f}\n"
        f"📊 *H1 TREND:* {signal['trend']}\n\n"

        "*CONFIRMATIONS:*\n"
        f"{reasons}\n\n"

        f"⭐ *SCORE:* {signal['score']}/10\n\n"

        f"🕐 *NEXT CANDLE:* {entry_time}\n\n"

        "⚠️ Signal is calculated from the "
        "*completed candle* and is intended "
        "for the upcoming candle.\n\n"

        "━━━━━━━━━━━━━━━━━━"
    )


# ================================================================
# MAIN SCANNER
# ================================================================

def analysis_engine():

    last_processed_minute = -1

    print("Strategy engine started")

    while True:

        try:

            current_minute = int(
                time.time() // 60
            )

            if current_minute != last_processed_minute:

                last_processed_minute = current_minute

                # Build newly completed M1 candles
                build_closed_m1_candles()

                # Give candle storage a moment to settle
                time.sleep(0.2)

                for pair in PAIRS:

                    for timeframe in TIMEFRAMES:

                        try:

                            signal = analyze_timeframe(
                                pair,
                                timeframe
                            )

                            if signal is None:
                                continue

                            signal_key = (
                                pair,
                                timeframe,
                                signal["next_candle"]
                            )

                            with lock:

                                if signal_key in signal_lock:
                                    continue

                                signal_lock.add(
                                    signal_key
                                )

                                # Keep lock from growing forever
                                if len(signal_lock) > 500:
                                    signal_lock.clear()

                            message = format_signal(
                                signal
                            )

                            msg_id = send_telegram(
                                message
                            )

                            if msg_id:
                                print(
                                    f"SIGNAL: "
                                    f"{pair} "
                                    f"{timeframe} "
                                    f"{signal['direction']} "
                                    f"{signal['score']}/10"
                                )

                        except Exception as e:

                            print(
                                "Analysis error:",
                                pair,
                                timeframe,
                                e
                            )

            time.sleep(0.2)

        except Exception as e:

            print(
                "Engine error:",
                e
            )

            time.sleep(2)


# ================================================================
# HEALTH SERVER FOR RENDER
# ================================================================

def health_server():

    from http.server import (
        BaseHTTPRequestHandler,
        HTTPServer
    )

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    class Handler(BaseHTTPRequestHandler):

        def do_GET(self):

            self.send_response(200)

            self.send_header(
                "Content-type",
                "text/plain"
            )

            self.end_headers()

            self.wfile.write(
                b"RSI BB Pin Bar Scanner Live"
            )

        def log_message(
            self,
            format,
            *args
        ):
            return

    server = HTTPServer(
        ("0.0.0.0", port),
        Handler
    )

    server.serve_forever()


# ================================================================
# START
# ================================================================

if __name__ == "__main__":

    print("======================================")
    print(" RSI + BOLLINGER + PIN BAR SCANNER")
    print("======================================")
    print("Timeframes: M1 / M3 / M5")
    print("RSI: 14")
    print("Bollinger: 20 / 2")
    print("Minimum score:", MIN_SCORE)
    print("======================================")

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    threading.Thread(
        target=websocket_worker,
        daemon=True
    ).start()

    analysis_engine()
