import json
import os
import random
import string
import sys
import time
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import numpy as np
import pandas as pd
import requests
from websocket import create_connection


# ================================================================
# CONFIGURATION
# ================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.getenv("PORT", "10000"))

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

TIMEFRAMES = {
    "M1": 1,
    "M3": 3,
    "M5": 5,
}

EXPIRY_MINUTES = {
    "M1": 1,
    "M3": 3,
    "M5": 5,
}

# RSI + Bollinger settings
RSI_PERIOD = 14
RSI_OVERSOLD = 30.0
RSI_OVERBOUGHT = 70.0

BB_PERIOD = 20
BB_STD = 2.0

# Strict final filter
MIN_SCORE = 9

# Early preparation alert.
# This is NOT a final signal.
PREPARE_SECONDS = 38
PREPARE_MIN_SCORE = 6

# Historical M1 candles retained
HISTORY_M1_BARS = 3000

# Support / resistance lookback
SR_LOOKBACK = 50

# Local result file
RESULT_LOG = "signal_results.csv"

lock = threading.RLock()

# pair -> {unix_minute: candle}
candles_m1 = {
    pair: {}
    for pair in PAIRS
}

# Prevent duplicate final alerts
last_signal = set()

# Prevent duplicate preparation alerts
prepare_sent = set()

# Signals waiting for expiry result
pending_results = {}


# ================================================================
# TRADINGVIEW MESSAGE HELPERS
# ================================================================

def generate_session(prefix):
    chars = string.ascii_lowercase

    return prefix + "".join(
        random.choice(chars)
        for _ in range(12)
    )


def frame_message(message):
    return f"~m~{len(message)}~m~{message}"


def send_tv_message(
    ws,
    function_name,
    params
):
    body = json.dumps(
        {
            "m": function_name,
            "p": params,
        },
        separators=(",", ":"),
    )

    ws.send(
        frame_message(body)
    )


def iter_frames(raw):
    """
    TradingView can concatenate multiple
    ~m~length~m~payload frames in one
    websocket message.
    """

    if not isinstance(raw, str):
        return

    position = 0
    total = len(raw)

    while position < total:

        if raw.startswith(
            "~m~",
            position
        ):

            separator = raw.find(
                "~m~",
                position + 3
            )

            if separator == -1:
                return

            try:

                length = int(
                    raw[
                        position + 3:
                        separator
                    ]
                )

            except ValueError:
                return

            start = separator + 3

            payload = raw[
                start:start + length
            ]

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

            yield (
                "json",
                json.loads(payload)
            )

        except Exception:
            continue


# ================================================================
# TELEGRAM
# ================================================================

def send_telegram(text):

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=8,
        )

        if response.ok:

            return (
                response.json()
                .get("result", {})
                .get("message_id")
            )

        print(
            "Telegram error:",
            response.status_code,
            response.text[:300],
        )

    except Exception as exc:

        print(
            "Telegram exception:",
            exc,
        )

    return None


# ================================================================
# TIME
# ================================================================

def local_time_text(timestamp):

    return time.strftime(
        "%H:%M:%S",
        time.localtime(timestamp),
    )


def utc_time_text(timestamp):

    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


# ================================================================
# CANDLE STORAGE
# ================================================================

def store_m1_candle(
    pair,
    candle
):

    timestamp = int(
        candle["time"]
    )

    with lock:

        candles_m1[pair][timestamp] = {
            "time": timestamp,
            "open": float(
                candle["open"]
            ),
            "high": float(
                candle["high"]
            ),
            "low": float(
                candle["low"]
            ),
            "close": float(
                candle["close"]
            ),
        }

        # Prevent unlimited memory growth.
        if (
            len(candles_m1[pair])
            > HISTORY_M1_BARS + 200
        ):

            old_keys = sorted(
                candles_m1[pair]
            )[:-HISTORY_M1_BARS]

            for key in old_keys:

                candles_m1[pair].pop(
                    key,
                    None
                )


def get_m1_candles(
    pair,
    include_current=True,
):

    current_minute = (
        int(time.time() // 60)
        * 60
    )

    with lock:

        rows = [
            candles_m1[pair][key]
            for key in sorted(
                candles_m1[pair]
            )
        ]

    if not include_current:

        rows = [
            row
            for row in rows
            if row["time"]
            < current_minute
        ]

    return rows


# ================================================================
# HISTORICAL DATA
# ================================================================

def load_history_for_pair(
    pair,
    symbol,
):

    ws = None

    try:

        ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket",
            origin="https://data.tradingview.com",
            timeout=15,
        )

        chart_session = generate_session(
            "cs_"
        )

        quote_session = generate_session(
            "qs_"
        )

        send_tv_message(
            ws,
            "set_auth_token",
            ["unauthorized_user_token"],
        )

        send_tv_message(
            ws,
            "chart_create_session",
            [
                chart_session,
                "",
            ],
        )

        send_tv_message(
            ws,
            "quote_create_session",
            [
                quote_session,
            ],
        )

        send_tv_message(
            ws,
            "switch_timezone",
            [
                chart_session,
                "Etc/UTC",
            ],
        )

        symbol_config = json.dumps(
            {
                "symbol": symbol,
                "adjustment": "splits",
            },
            separators=(",", ":"),
        )

        send_tv_message(
            ws,
            "resolve_symbol",
            [
                chart_session,
                "symbol_1",
                "=" + symbol_config,
            ],
        )

        send_tv_message(
            ws,
            "create_series",
            [
                chart_session,
                "s1",
                "s1",
                "symbol_1",
                "1",
                HISTORY_M1_BARS,
            ],
        )

        deadline = time.time() + 20

        loaded = 0

        while time.time() < deadline:

            raw = ws.recv()

            for kind, message in parse_frames(raw):

                if kind == "heartbeat":

                    ws.send(message)

                    continue

                if message.get("m") not in (
                    "timescale_update",
                    "du",
                ):

                    continue

                params = message.get(
                    "p",
                    []
                )

                if len(params) < 2:
                    continue

                data = params[1]

                if not isinstance(
                    data,
                    dict
                ):

                    continue

                series = (
                    data.get("sds_1")
                    or data.get("s1")
                )

                if not isinstance(
                    series,
                    dict
                ):

                    continue

                for item in series.get(
                    "s",
                    []
                ):

                    values = (
                        item.get("v")
                        if isinstance(
                            item,
                            dict
                        )
                        else None
                    )

                    if not isinstance(
                        values,
                        list
                    ):

                        continue

                    if len(values) < 5:
                        continue

                    try:

                        timestamp = float(
                            values[0]
                        )

                        open_price = float(
                            values[1]
                        )

                        high_price = float(
                            values[2]
                        )

                        low_price = float(
                            values[3]
                        )

                        close_price = float(
                            values[4]
                        )

                        values_to_check = [
                            timestamp,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                        ]

                        if not np.isfinite(
                            values_to_check
                        ).all():

                            continue

                        if timestamp < 1_000_000_000:
                            continue

                        store_m1_candle(
                            pair,
                            {
                                "time": int(
                                    timestamp
                                ),
                                "open": open_price,
                                "high": high_price,
                                "low": low_price,
                                "close": close_price,
                            },
                        )

                        loaded += 1

                    except Exception:
                        continue

                if message.get(
                    "m"
                ) == "series_completed":

                    if len(
                        get_m1_candles(pair)
                    ) >= 100:

                        print(
                            f"{pair}: "
                            f"loaded "
                            f"{len(get_m1_candles(pair))} "
                            f"M1 candles"
                        )

                        return True

        print(
            f"{pair}: historical load timeout"
        )

        return len(
            get_m1_candles(pair)
        ) >= 100

    except Exception as exc:

        print(
            f"{pair}: history error:",
            exc,
        )

        return False

    finally:

        if ws:

            try:
                ws.close()
            except Exception:
                pass
# ================================================================
# REAL-TIME TRADINGVIEW STREAM
# ================================================================

def realtime_chart_worker():

    while True:

        ws = None

        try:

            ws = create_connection(
                "wss://data.tradingview.com/socket.io/websocket",
                origin="https://data.tradingview.com",
                timeout=30,
            )

            chart_session = generate_session(
                "cs_"
            )

            quote_session = generate_session(
                "qs_"
            )

            send_tv_message(
                ws,
                "set_auth_token",
                ["unauthorized_user_token"],
            )

            send_tv_message(
                ws,
                "chart_create_session",
                [
                    chart_session,
                    "",
                ],
            )

            send_tv_message(
                ws,
                "quote_create_session",
                [
                    quote_session,
                ],
            )

            send_tv_message(
                ws,
                "switch_timezone",
                [
                    chart_session,
                    "Etc/UTC",
                ],
            )

            for index, (
                pair,
                symbol,
            ) in enumerate(
                PAIRS.items(),
                start=1,
            ):

                symbol_config = json.dumps(
                    {
                        "symbol": symbol,
                        "adjustment": "splits",
                    },
                    separators=(",", ":"),
                )

                alias = (
                    f"symbol_{index}"
                )

                series = (
                    f"s{index}"
                )

                send_tv_message(
                    ws,
                    "resolve_symbol",
                    [
                        chart_session,
                        alias,
                        "=" + symbol_config,
                    ],
                )

                send_tv_message(
                    ws,
                    "create_series",
                    [
                        chart_session,
                        series,
                        series,
                        alias,
                        "1",
                        5,
                    ],
                )

            print(
                "Realtime TradingView stream connected."
            )

            while True:

                raw = ws.recv()

                for kind, message in parse_frames(raw):

                    if kind == "heartbeat":

                        ws.send(message)

                        continue

                    if message.get(
                        "m"
                    ) != "timescale_update":

                        continue

                    params = message.get(
                        "p",
                        []
                    )

                    if len(params) < 2:
                        continue

                    data = params[1]

                    if not isinstance(
                        data,
                        dict
                    ):

                        continue

                    for index, pair in enumerate(
                        PAIRS,
                        start=1
                    ):

                        series = data.get(
                            f"s{index}"
                        )

                        if not isinstance(
                            series,
                            dict
                        ):

                            continue

                        for item in series.get(
                            "s",
                            []
                        ):

                            values = (
                                item.get("v")
                                if isinstance(
                                    item,
                                    dict
                                )
                                else None
                            )

                            if not isinstance(
                                values,
                                list
                            ):

                                continue

                            if len(values) < 5:
                                continue

                            try:

                                timestamp = float(
                                    values[0]
                                )

                                if (
                                    timestamp
                                    < 1_000_000_000
                                ):

                                    continue

                                store_m1_candle(
                                    pair,
                                    {
                                        "time": int(
                                            timestamp
                                        ),
                                        "open": float(
                                            values[1]
                                        ),
                                        "high": float(
                                            values[2]
                                        ),
                                        "low": float(
                                            values[3]
                                        ),
                                        "close": float(
                                            values[4]
                                        ),
                                    },
                                )

                            except Exception:
                                continue

        except Exception as exc:

            print(
                "Realtime stream disconnected:",
                exc,
            )

            time.sleep(3)

        finally:

            if ws:

                try:
                    ws.close()
                except Exception:
                    pass


# ================================================================
# TIMEFRAME RESAMPLING
# ================================================================

def resample_candles(
    rows,
    minutes,
    completed_only=True,
):

    if not rows:
        return []

    df = pd.DataFrame(rows)

    df["datetime"] = pd.to_datetime(
        df["time"],
        unit="s",
        utc=True,
    )

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .set_index("datetime")
    )

    if completed_only:

        current_minute = (
            int(time.time() // 60)
            * 60
        )

        df = df[
            df["time"] < current_minute
        ]

    result = (
        df.resample(
            f"{minutes}min",
            origin="epoch",
        )
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
            }
        )
        .dropna()
    )

    if result.empty:
        return []

    result["time"] = (
        result.index.astype("int64")
        // 10**9
    )

    return (
        result.reset_index(
            drop=True
        )
        .to_dict("records")
    )


# ================================================================
# RSI — WILDER METHOD
# ================================================================

def calculate_rsi(
    closes,
    period=14,
):

    if len(closes) < period + 1:
        return np.nan

    values = np.asarray(
        closes,
        dtype=float,
    )

    changes = np.diff(values)

    gains = np.where(
        changes > 0,
        changes,
        0.0,
    )

    losses = np.where(
        changes < 0,
        -changes,
        0.0,
    )

    average_gain = (
        gains[:period].mean()
    )

    average_loss = (
        losses[:period].mean()
    )

    for index in range(
        period,
        len(changes),
    ):

        average_gain = (
            (
                (period - 1)
                * average_gain
            )
            + gains[index]
        ) / period

        average_loss = (
            (
                (period - 1)
                * average_loss
            )
            + losses[index]
        ) / period

    if average_loss == 0:
        return 100.0

    relative_strength = (
        average_gain
        / average_loss
    )

    return float(
        100.0
        - (
            100.0
            / (
                1.0
                + relative_strength
            )
        )
    )


# ================================================================
# BOLLINGER BANDS
# ================================================================

def calculate_bollinger(
    closes,
    period=20,
    std_multiplier=2.0,
):

    if len(closes) < period:
        return None

    series = pd.Series(
        closes,
        dtype=float,
    )

    middle = float(
        series
        .rolling(period)
        .mean()
        .iloc[-1]
    )

    standard_deviation = float(
        series
        .rolling(period)
        .std(ddof=0)
        .iloc[-1]
    )

    return {
        "middle": middle,
        "upper": (
            middle
            + std_multiplier
            * standard_deviation
        ),
        "lower": (
            middle
            - std_multiplier
            * standard_deviation
        ),
    }


# ================================================================
# CANDLE STRUCTURE
# ================================================================

def candle_parts(candle):

    open_price = candle["open"]
    high_price = candle["high"]
    low_price = candle["low"]
    close_price = candle["close"]

    candle_range = (
        high_price
        - low_price
    )

    body = abs(
        close_price
        - open_price
    )

    upper_wick = (
        high_price
        - max(
            open_price,
            close_price,
        )
    )

    lower_wick = (
        min(
            open_price,
            close_price,
        )
        - low_price
    )

    return (
        candle_range,
        body,
        upper_wick,
        lower_wick,
    )


# ================================================================
# BULLISH PIN BAR
# ================================================================

def bullish_pin_bar(candle):

    (
        candle_range,
        body,
        upper_wick,
        lower_wick,
    ) = candle_parts(candle)

    if candle_range <= 0:
        return False

    return (
        body
        <= candle_range * 0.35

        and lower_wick
        >= max(
            body * 2.0,
            candle_range * 0.45,
        )

        and lower_wick
        > upper_wick * 1.5

        and candle["close"]
        >= (
            candle["low"]
            + candle_range * 0.60
        )
    )


# ================================================================
# BEARISH PIN BAR
# ================================================================

def bearish_pin_bar(candle):

    (
        candle_range,
        body,
        upper_wick,
        lower_wick,
    ) = candle_parts(candle)

    if candle_range <= 0:
        return False

    return (
        body
        <= candle_range * 0.35

        and upper_wick
        >= max(
            body * 2.0,
            candle_range * 0.45,
        )

        and upper_wick
        > lower_wick * 1.5

        and candle["close"]
        <= (
            candle["high"]
            - candle_range * 0.60
        )
    )


# ================================================================
# BULLISH ENGULFING
# ================================================================

def bullish_engulfing(
    previous,
    current,
):

    return (
        previous["close"]
        < previous["open"]

        and current["close"]
        > current["open"]

        and current["open"]
        <= previous["close"]

        and current["close"]
        >= previous["open"]
    )


# ================================================================
# BEARISH ENGULFING
# ================================================================

def bearish_engulfing(
    previous,
    current,
):

    return (
        previous["close"]
        > previous["open"]

        and current["close"]
        < current["open"]

        and current["open"]
        >= previous["close"]

        and current["close"]
        <= previous["open"]
    )


# ================================================================
# SUPPORT / RESISTANCE
# ================================================================

def get_support_resistance(
    timeframe_candles,
):

    if len(timeframe_candles) < 20:
        return None, None

    recent = timeframe_candles[
        -SR_LOOKBACK:
    ]

    support = min(
        candle["low"]
        for candle in recent
    )

    resistance = max(
        candle["high"]
        for candle in recent
    )

    return (
        support,
        resistance,
    )


def near_level(
    price,
    level,
    tolerance,
):

    if level is None:
        return False

    return (
        abs(price - level)
        <= tolerance
    )


# ================================================================
# H1 TREND
# ================================================================

def get_h1_trend(
    m1_candles,
):

    h1 = resample_candles(
        m1_candles,
        60,
        completed_only=True,
    )

    if len(h1) < 30:
        return "UNKNOWN"

    closes = np.asarray(
        [
            candle["close"]
            for candle in h1
        ],
        dtype=float,
    )

    fast_ema = (
        pd.Series(closes)
        .ewm(
            span=10,
            adjust=False,
        )
        .mean()
        .iloc[-1]
    )

    slow_ema = (
        pd.Series(closes)
        .ewm(
            span=25,
            adjust=False,
        )
        .mean()
        .iloc[-1]
    )

    first = h1[-3]
    second = h1[-2]
    third = h1[-1]

    bullish_structure = (
        second["high"]
        >= first["high"]

        and second["low"]
        >= first["low"]

        and third["high"]
        > second["high"]

        and third["low"]
        > second["low"]
    )

    bearish_structure = (
        second["high"]
        <= first["high"]

        and second["low"]
        <= first["low"]

        and third["high"]
        < second["high"]

        and third["low"]
        < second["low"]
    )

    if (
        fast_ema > slow_ema
        and bullish_structure
    ):

        return "BULLISH"

    if (
        fast_ema < slow_ema
        and bearish_structure
    ):

        return "BEARISH"

    return "NEUTRAL"

# ================================================================
# STRATEGY ANALYSIS
# ================================================================

def analyze_timeframe(
    pair,
    timeframe,
    allow_partial=False,
):

    with lock:

        m1 = get_m1_candles(
            pair,
            include_current=True,
        )

    minutes = TIMEFRAMES[
        timeframe
    ]

    timeframe_candles = resample_candles(
        m1,
        minutes,
        completed_only=(
            not allow_partial
        ),
    )

    if len(timeframe_candles) < 40:
        return None

    current = timeframe_candles[-1]

    previous = timeframe_candles[-2]

    closes = [
        candle["close"]
        for candle in timeframe_candles
    ]

    rsi = calculate_rsi(
        closes,
        RSI_PERIOD,
    )

    bands = calculate_bollinger(
        closes,
        BB_PERIOD,
        BB_STD,
    )

    if (
        np.isnan(rsi)
        or bands is None
    ):
        return None

    h1_trend = get_h1_trend(
        m1
    )

    support, resistance = (
        get_support_resistance(
            timeframe_candles
        )
    )

    candle_range = (
        current["high"]
        - current["low"]
    )

    if candle_range <= 0:
        return None

    # Dynamic level tolerance.
    # This avoids using an identical price distance
    # for every pair.
    tolerance = max(
        candle_range * 0.60,
        current["close"] * 0.00015,
    )

    call_score = 0
    put_score = 0

    call_reasons = []
    put_reasons = []

    # ============================================================
    # CALL CONDITIONS
    # ============================================================

    if h1_trend == "BULLISH":

        call_score += 2

        call_reasons.append(
            "H1 bullish"
        )

    if rsi < RSI_OVERSOLD:

        call_score += 2

        call_reasons.append(
            f"RSI oversold {rsi:.1f}"
        )

    if current["low"] <= bands["lower"]:

        call_score += 2

        call_reasons.append(
            "Lower Bollinger touch"
        )

    bullish_pin = bullish_pin_bar(
        current
    )

    bullish_engulf = bullish_engulfing(
        previous,
        current,
    )

    if bullish_pin:

        call_score += 2

        call_reasons.append(
            "Bullish pin bar"
        )

    elif bullish_engulf:

        call_score += 2

        call_reasons.append(
            "Bullish engulfing"
        )

    near_support = near_level(
        current["low"],
        support,
        tolerance,
    )

    if near_support:

        call_score += 1

        call_reasons.append(
            "Near support"
        )

    if current["close"] > current["open"]:

        call_score += 1

        call_reasons.append(
            "Bullish close"
        )

    # ============================================================
    # PUT CONDITIONS
    # ============================================================

    if h1_trend == "BEARISH":

        put_score += 2

        put_reasons.append(
            "H1 bearish"
        )

    if rsi > RSI_OVERBOUGHT:

        put_score += 2

        put_reasons.append(
            f"RSI overbought {rsi:.1f}"
        )

    if current["high"] >= bands["upper"]:

        put_score += 2

        put_reasons.append(
            "Upper Bollinger touch"
        )

    bearish_pin = bearish_pin_bar(
        current
    )

    bearish_engulf = bearish_engulfing(
        previous,
        current,
    )

    if bearish_pin:

        put_score += 2

        put_reasons.append(
            "Bearish pin bar"
        )

    elif bearish_engulf:

        put_score += 2

        put_reasons.append(
            "Bearish engulfing"
        )

    near_resistance = near_level(
        current["high"],
        resistance,
        tolerance,
    )

    if near_resistance:

        put_score += 1

        put_reasons.append(
            "Near resistance"
        )

    if current["close"] < current["open"]:

        put_score += 1

        put_reasons.append(
            "Bearish close"
        )

    # ============================================================
    # HARD CORE FILTER
    #
    # The score by itself is NOT enough.
    # The important strategy conditions must actually exist.
    # ============================================================

    call_core = (
        h1_trend == "BULLISH"

        and rsi < RSI_OVERSOLD

        and current["low"]
        <= bands["lower"]

        and (
            bullish_pin
            or bullish_engulf
        )

        and (
            near_support
            or current["close"]
            > current["open"]
        )
    )

    put_core = (
        h1_trend == "BEARISH"

        and rsi > RSI_OVERBOUGHT

        and current["high"]
        >= bands["upper"]

        and (
            bearish_pin
            or bearish_engulf
        )

        and (
            near_resistance
            or current["close"]
            < current["open"]
        )
    )

    direction = None
    score = 0
    reasons = []

    if (
        call_core
        and call_score >= MIN_SCORE
        and call_score > put_score
    ):

        direction = "CALL"

        score = call_score

        reasons = call_reasons

    elif (
        put_core
        and put_score >= MIN_SCORE
        and put_score > call_score
    ):

        direction = "PUT"

        score = put_score

        reasons = put_reasons

    # ============================================================
    # UPCOMING CANDLE
    # ============================================================

    next_candle = (
        int(current["time"])
        + minutes * 60
    )

    return {
        "pair": pair,
        "timeframe": timeframe,
        "direction": direction,

        "score": score,

        "call_score": call_score,
        "put_score": put_score,

        "rsi": float(rsi),

        "trend": h1_trend,

        "reasons": reasons,

        "candle_time": int(
            current["time"]
        ),

        "next_candle": next_candle,
    }


# ================================================================
# TELEGRAM — PREPARE MESSAGE
# ================================================================

def format_prepare_message(
    pair,
    timeframe,
    result,
):

    direction = result["direction"]

    emoji = (
        "🟢"
        if direction == "CALL"
        else "🔴"
    )

    score = max(
        result["call_score"],
        result["put_score"],
    )

    reasons = result.get(
        "reasons",
        []
    )

    reason_text = "\n".join(
        "• " + reason
        for reason in reasons
    )

    return (
        "━━━━━━━━━━━━━━━━━━\n"
        "⚠️ *PREPARE — POSSIBLE SIGNAL*\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        f"🏆 *PAIR:* "
        f"{pair[:3]}/{pair[3:]}\n"

        f"📊 *TIMEFRAME:* "
        f"{timeframe}\n"

        f"🎯 *POSSIBLE:* "
        f"{emoji} {direction}\n\n"

        f"📈 *RSI:* "
        f"{result['rsi']:.1f}\n"

        f"📊 *H1 TREND:* "
        f"{result['trend']}\n"

        f"⭐ *CURRENT SCORE:* "
        f"{score}/10\n\n"

        "*CURRENT CONDITIONS:*\n"

        f"{reason_text}\n\n"

        "⚠️ *WAIT — THIS IS NOT THE FINAL SIGNAL.*\n"

        "The candle is still forming. "
        "The setup can disappear before "
        "the candle closes.\n\n"

        "━━━━━━━━━━━━━━━━━━"
    )


# ================================================================
# TELEGRAM — FINAL MESSAGE
# ================================================================

def format_final_message(
    pair,
    timeframe,
    result,
):

    direction = result["direction"]

    emoji = (
        "🟢"
        if direction == "CALL"
        else "🔴"
    )

    expiry = EXPIRY_MINUTES[
        timeframe
    ]

    reasons = "\n".join(
        "• " + reason
        for reason in result["reasons"]
    )

    entry_timestamp = (
        result["next_candle"]
    )

    return (
        "━━━━━━━━━━━━━━━━━━\n"
        "✅ *FINAL SIGNAL*\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        f"🏆 *PAIR:* "
        f"{pair[:3]}/{pair[3:]}\n"

        f"📊 *TIMEFRAME:* "
        f"{timeframe}\n"

        f"🎯 *ACTION:* "
        f"{emoji} *{direction}*\n"

        f"⏱️ *EXPIRY:* "
        f"{expiry} MINUTE"
        f"{'S' if expiry != 1 else ''}\n\n"

        f"📈 *RSI(14):* "
        f"{result['rsi']:.1f}\n"

        f"📊 *H1 TREND:* "
        f"{result['trend']}\n"

        f"⭐ *SCORE:* "
        f"{result['score']}/10\n\n"

        "*CONFIRMATIONS:*\n"

        f"{reasons}\n\n"

        f"🕐 *ENTRY:* "
        f"{local_time_text(entry_timestamp)}\n"

        f"🌍 *UTC:* "
        f"{utc_time_text(entry_timestamp)}\n\n"

        "⚠️ Signal uses completed-candle "
        "confirmation and is intended "
        "for the upcoming candle.\n\n"

        "━━━━━━━━━━━━━━━━━━"
    )


# ================================================================
# RESULT TRACKING
# ================================================================

def initialize_result_file():

    if not os.path.exists(
        RESULT_LOG
    ):

        with open(
            RESULT_LOG,
            "w",
            encoding="utf-8",
        ) as file:

            file.write(
                "signal_time,"
                "pair,"
                "timeframe,"
                "direction,"
                "entry_price,"
                "expiry_price,"
                "result,"
                "score\n"
            )


def record_result(
    signal_time,
    pair,
    timeframe,
    direction,
    entry_price,
    expiry_price,
    result,
    score,
):

    with open(
        RESULT_LOG,
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            f"{signal_time},"
            f"{pair},"
            f"{timeframe},"
            f"{direction},"
            f"{entry_price:.8f},"
            f"{expiry_price:.8f},"
            f"{result},"
            f"{score}\n"
        )


def get_current_price(pair):

    rows = get_m1_candles(
        pair,
        include_current=True,
    )

    if not rows:
        return None

    return float(
        rows[-1]["close"]
    )


# ================================================================
# EVALUATE PENDING SIGNALS
# ================================================================

def evaluate_pending_results():

    now = time.time()

    finished = []

    with lock:

        for key, item in list(
            pending_results.items()
        ):

            if now < item["expiry_time"]:
                continue

            expiry_price = (
                get_current_price(
                    item["pair"]
                )
            )

            if expiry_price is None:
                continue

            if item["direction"] == "CALL":

                result = (
                    "WIN"
                    if expiry_price
                    > item["entry_price"]
                    else "LOSS"
                )

            else:

                result = (
                    "WIN"
                    if expiry_price
                    < item["entry_price"]
                    else "LOSS"
                )

            record_result(
                item["signal_time"],
                item["pair"],
                item["timeframe"],
                item["direction"],
                item["entry_price"],
                expiry_price,
                result,
                item["score"],
            )

            finished.append(
                (
                    key,
                    item,
                    expiry_price,
                    result,
                )
            )

        for key, _, _, _ in finished:

            pending_results.pop(
                key,
                None
            )

    # Send result notifications outside
    # the storage lock.

    for (
        key,
        item,
        expiry_price,
        result,
    ) in finished:

        emoji = (
            "✅"
            if result == "WIN"
            else "❌"
        )

        send_telegram(
            f"{emoji} *RESULT*\n\n"

            f"🏆 *PAIR:* "
            f"{item['pair'][:3]}/"
            f"{item['pair'][3:]}\n"

            f"📊 *TIMEFRAME:* "
            f"{item['timeframe']}\n"

            f"🎯 *ACTION:* "
            f"{item['direction']}\n"

            f"⭐ *SCORE:* "
            f"{item['score']}/10\n"

            f"💰 *ENTRY:* "
            f"{item['entry_price']}\n"

            f"🏁 *EXPIRY:* "
            f"{expiry_price}\n\n"

            f"*{result}*"
    )

# ================================================================
# SCANNER
# ================================================================

def scanner():

    print(
        "Strategy scanner started."
    )

    last_second = -1

    while True:

        try:

            current_time = time.time()

            current_second = int(
                current_time
            )

            if current_second == last_second:

                time.sleep(0.05)
                continue

            last_second = current_second

            # Check signals whose expiry time has arrived.
            evaluate_pending_results()

            # Scan every pair.
            for pair in PAIRS:

                # Scan M1, M3 and M5.
                for timeframe, minutes in (
                    TIMEFRAMES.items()
                ):

                    # ------------------------------------------------
                    # FIND CURRENT CANDLE BUCKET
                    # ------------------------------------------------

                    bucket_start = (
                        int(
                            current_time
                            / (minutes * 60)
                        )
                        * (minutes * 60)
                    )

                    bucket_end = (
                        bucket_start
                        + minutes * 60
                    )

                    seconds_to_close = (
                        bucket_end
                        - current_time
                    )

                    # ------------------------------------------------
                    # PREPARE ALERT
                    # ------------------------------------------------
                    #
                    # This is NOT the final signal.
                    #
                    # It checks the candle while it is still forming.
                    # The setup can disappear before candle close.
                    # ------------------------------------------------

                    if (
                        0
                        <= seconds_to_close
                        <= PREPARE_SECONDS
                    ):

                        prepare_key = (
                            pair,
                            timeframe,
                            bucket_start,
                        )

                        if (
                            prepare_key
                            not in prepare_sent
                        ):

                            result = (
                                analyze_timeframe(
                                    pair,
                                    timeframe,
                                    allow_partial=True,
                                )
                            )

                            if (
                                result
                                and result["direction"]
                                and max(
                                    result[
                                        "call_score"
                                    ],
                                    result[
                                        "put_score"
                                    ],
                                )
                                >= PREPARE_MIN_SCORE
                            ):

                                prepare_sent.add(
                                    prepare_key
                                )

                                send_telegram(
                                    format_prepare_message(
                                        pair,
                                        timeframe,
                                        result,
                                    )
                                )

                    # ------------------------------------------------
                    # FINAL SIGNAL
                    # ------------------------------------------------
                    #
                    # When the candle closes, analyze ONLY the
                    # completed candle.
                    #
                    # The signal is intended for the NEXT candle.
                    # ------------------------------------------------

                    if (
                        -0.50
                        <= seconds_to_close
                        <= 0.50
                    ):

                        result = (
                            analyze_timeframe(
                                pair,
                                timeframe,
                                allow_partial=False,
                            )
                        )

                        # No valid setup.
                        if (
                            not result
                            or not result["direction"]
                        ):
                            continue

                        # Unique key for this signal.
                        signal_key = (
                            pair,
                            timeframe,
                            result[
                                "next_candle"
                            ],
                        )

                        # Prevent duplicate Telegram signals.
                        if (
                            signal_key
                            in last_signal
                        ):
                            continue

                        # ------------------------------------------------
                        # ENTRY PRICE
                        # ------------------------------------------------
                        #
                        # This is the theoretical entry price at the
                        # candle boundary.
                        # ------------------------------------------------

                        entry_price = (
                            get_current_price(
                                pair
                            )
                        )

                        if entry_price is None:
                            continue

                        last_signal.add(
                            signal_key
                        )

                        # Prevent unlimited memory growth.
                        if len(last_signal) > 5000:

                            last_signal.clear()

                        # ------------------------------------------------
                        # SEND FINAL TELEGRAM SIGNAL
                        # ------------------------------------------------

                        send_telegram(
                            format_final_message(
                                pair,
                                timeframe,
                                result,
                            )
                        )

                        # ------------------------------------------------
                        # RESULT TRACKING
                        # ------------------------------------------------
                        #
                        # This does NOT represent the actual Quotex
                        # settlement.
                        #
                        # It is only a market-price direction proxy:
                        #
                        # CALL = expiry price > entry
                        # PUT  = expiry price < entry
                        # ------------------------------------------------

                        pending_results[
                            signal_key
                        ] = {
                            "signal_time": int(
                                time.time()
                            ),

                            "pair": pair,

                            "timeframe": timeframe,

                            "direction": result[
                                "direction"
                            ],

                            "score": result[
                                "score"
                            ],

                            "entry_price": float(
                                entry_price
                            ),

                            "expiry_time": (
                                result[
                                    "next_candle"
                                ]
                                + minutes * 60
                            ),
                        }

            time.sleep(0.05)

        except Exception as exc:

            print(
                "Scanner error:",
                exc,
            )

            time.sleep(1)


# ================================================================
# RENDER HEALTH SERVER
# ================================================================

def health_server():

    class HealthHandler(
        BaseHTTPRequestHandler
    ):

        def do_GET(self):

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "text/plain",
            )

            self.end_headers()

            self.wfile.write(
                b"RSI BB Pin Bar Scanner is running."
            )

        def log_message(
            self,
            format_string,
            *args,
        ):
            return

    server = HTTPServer(
        ("0.0.0.0", PORT),
        HealthHandler,
    )

    server.serve_forever()


# ================================================================
# START
# ================================================================

def main():

    print("=" * 60)

    print(
        "RSI + BOLLINGER + PIN BAR SIGNAL ENGINE"
    )

    print("=" * 60)

    print(
        "Pairs:",
        ", ".join(PAIRS),
    )

    print(
        "Timeframes: M1 / M3 / M5"
    )

    print(
        "RSI:",
        RSI_PERIOD,
        "| BB:",
        BB_PERIOD,
        "/",
        BB_STD,
    )

    print(
        "Minimum final score:",
        MIN_SCORE,
        "/10",
    )

    print(
        "Prepare alert:",
        PREPARE_SECONDS,
        "seconds before candle close",
    )

    print("=" * 60)

    # Create result CSV if it does not already exist.
        # Create result CSV if it does not already exist.
    initialize_result_file()

    send_telegram(
        "✅ *Telegram connection test successful!*\n\n"
        "Your trading signal bot is connected and ready."
    )

    # ------------------------------------------------------------
    # LOAD HISTORICAL M1 DATA
    # ------------------------------------------------------------

    # ------------------------------------------------------------
    # LOAD HISTORICAL M1 DATA
    # ------------------------------------------------------------
    #
    # This is important because H1 trend analysis requires enough
    # historical candles after the bot starts.
    # ------------------------------------------------------------

    for pair, symbol in PAIRS.items():

        load_history_for_pair(
            pair,
            symbol,
        )

    # Display loaded candle counts.
    counts = {
        pair: len(
            get_m1_candles(pair)
        )
        for pair in PAIRS
    }

    print(
        "Historical candle counts:",
        counts,
    )

    # ------------------------------------------------------------
    # START REAL-TIME TRADINGVIEW DATA
    # ------------------------------------------------------------

    threading.Thread(
        target=realtime_chart_worker,
        daemon=True,
    ).start()

    # ------------------------------------------------------------
    # START RENDER HEALTH SERVER
    # ------------------------------------------------------------

    threading.Thread(
        target=health_server,
        daemon=True,
    ).start()

    # ------------------------------------------------------------
    # START SIGNAL SCANNER
    # ------------------------------------------------------------

    scanner()


# ================================================================
# PYTHON ENTRY POINT
# ================================================================

if __name__ == "__main__":

    main()
                
