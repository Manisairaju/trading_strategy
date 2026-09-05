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

# RSI + Bollinger settings from the strategy
RSI_PERIOD = 14
RSI_OVERSOLD = 30.0
RSI_OVERBOUGHT = 70.0

BB_PERIOD = 20
BB_STD = 2.0

# 9/10 is intentionally strict.
# The four core conditions are mandatory:
# H1 trend + RSI extreme + Bollinger touch + rejection pattern.
MIN_SCORE = 9

# Early preparation alert. It is NOT a final signal.
PREPARE_SECONDS = 38
PREPARE_MIN_SCORE = 6

# Enough M1 history for H1 trend + indicators + S/R.
HISTORY_M1_BARS = 3000
SR_LOOKBACK = 50

# Local CSV result log.
RESULT_LOG = "signal_results.csv"

lock = threading.RLock()

# pair -> {unix_minute: candle}
candles_m1 = {
    pair: {}
    for pair in PAIRS
}

# Prevent duplicate final alerts.
last_signal = set()

# Prevent duplicate preparation alerts.
prepare_sent = set()

# Signals waiting for expiry result.
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


def send_tv_message(ws, function_name, params):
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
    TradingView can concatenate several ~m~length~m~payload
    frames in one websocket message.
    """

    if not isinstance(raw, str):
        return

    position = 0
    total = len(raw)

    while position < total:

        if raw.startswith("~m~", position):

            separator = raw.find(
                "~m~",
                position + 3
            )

            if separator == -1:
                return

            try:
                length = int(
                    raw[position + 3:separator]
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
            yield "json", json.loads(payload)
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

def store_m1_candle(pair, candle):

    timestamp = int(
        candle["time"]
    )

    with lock:

        candles_m1[pair][timestamp] = {
            "time": timestamp,
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
        }

        # Prevent unlimited memory growth.
        if len(candles_m1[pair]) > HISTORY_M1_BARS + 200:

            old_keys = sorted(
                candles_m1[pair]
            )[:-HISTORY_M1_BARS]

            for key in old_keys:
                candles_m1[pair].pop(
                    key,
                    None,
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
            if row["time"] < current_minute
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
                    [],
                )

                if len(params) < 2:
                    continue

                data = params[1]

                if not isinstance(data, dict):
                    continue

                series = (
                    data.get("sds_1")
                    or data.get("s1")
                )

                if not isinstance(series, dict):
                    continue

                for item in series.get(
                    "s",
                    [],
                ):

                    values = (
                        item.get("v")
                        if isinstance(item, dict)
                        else None
                    )

                    if not isinstance(
                        values,
                        list,
                    ):
                        continue

                    # TradingView candle data is:
                    # [timestamp, open, high, low, close, ...]
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
                                "time": int(timestamp),
                                "open": open_price,
                                "high": high_price,
                                "low": low_price,
                                "close": close_price,
                            },
                        )

                        loaded += 1

                    except Exception:
                        continue

                if message.get("m") == "series_completed":

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

                    if message.get("m") != "timescale_update":
                        continue

                    params = message.get(
                        "p",
                        [],
                    )

                    if len(params) < 2:
                        continue

                    data = params[1]

                    if not isinstance(
                        data,
                        dict,
                    ):
                        continue

                    for index, pair in enumerate(
                        PAIRS,
                        start=1,
                    ):

                        series = data.get(
                            f"s{index}"
                        )

                        if not isinstance(
                            series,
                            dict,
                        ):
                            continue

                        for item in series.get(
                            "s",
                            [],
                        ):

                            values = (
                                item.get("v")
                                if isinstance(
                                    item,
                                    dict,
                                )
                                else None
                            )

                            if not isinstance(
                                values,
                                list,
                            ):
                                continue

                            if len(values) < 5:
                                continue

                            try:

                                timestamp = float(
                                    values[0]
                                )

                                if timestamp < 1_000_000_000:
                                    continue

                                store_m1_candle(
                                    pair,
                                    {
                                        "time": int(timestamp),
                                        "open": float(values[1]),
                                        "high": float(values[2]),
                                        "low": float(values[3]),
                                        "close": float(values[4]),
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
# RSI
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
                                        
