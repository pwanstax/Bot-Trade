import os
import time
import sqlite3
import threading
import traceback
from decimal import Decimal, ROUND_DOWN

from flask import Flask, request, jsonify
from binance.client import Client

# =========================
# ENV / CONFIG
# =========================
PORT = int(os.environ.get("PORT", "5000"))

API_KEY = os.environ["API_KEY"]
API_SECRET = os.environ["API_SECRET"]
SECRET = os.environ["SECRET"]

DB_PATH = os.environ.get("DB_PATH", "data/trades.db")

USD_PER_TRADE = float(os.environ.get("USD_PER_TRADE", "105"))
MIN_NOTIONAL = float(os.environ.get("MIN_NOTIONAL", "100"))
MIN_NOTIONAL_BUFFER = float(os.environ.get("MIN_NOTIONAL_BUFFER", "1.02"))
LEVERAGE = int(os.environ.get("LEVERAGE", "5"))

TP_SPLITS = [0.20, 0.20, 0.20, 0.20, 0.20]

USE_TESTNET = os.environ.get("USE_TESTNET", "true").lower() == "true"

# =========================
# APP / CLIENT
# =========================
app = Flask(__name__)

_client = None

def get_client():
    global _client
    if _client is None:
        c = Client(API_KEY, API_SECRET, testnet=USE_TESTNET)
        if USE_TESTNET:
            c.FUTURES_URL = "https://demo-fapi.binance.com"
        _client = c
    return _client

db_lock = threading.Lock()

# =========================
# DB
# =========================
def get_db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with db_lock:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            symbol TEXT PRIMARY KEY,
            side TEXT NOT NULL,
            entry REAL NOT NULL,
            initial_sl REAL NOT NULL,
            current_sl REAL NOT NULL,
            tp1_price REAL NOT NULL,
            tp1_order_id TEXT NOT NULL,
            sl_order_id TEXT NOT NULL,
            breakeven_armed INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        conn.close()


def db_execute(query, params=()):
    with db_lock:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        conn.close()


def db_fetchone(query, params=()):
    with db_lock:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(query, params)
        row = cur.fetchone()
        conn.close()
        return row


def db_fetchall(query, params=()):
    with db_lock:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        return rows


def save_trade(symbol, side, entry, initial_sl, tp1_price, tp1_order_id, sl_order_id):
    db_execute("""
    INSERT INTO trades (
        symbol, side, entry, initial_sl, current_sl,
        tp1_price, tp1_order_id, sl_order_id, breakeven_armed, is_active, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 1, CURRENT_TIMESTAMP)
    ON CONFLICT(symbol) DO UPDATE SET
        side = excluded.side,
        entry = excluded.entry,
        initial_sl = excluded.initial_sl,
        current_sl = excluded.current_sl,
        tp1_price = excluded.tp1_price,
        tp1_order_id = excluded.tp1_order_id,
        sl_order_id = excluded.sl_order_id,
        breakeven_armed = 0,
        is_active = 1,
        updated_at = CURRENT_TIMESTAMP
    """, (
        symbol,
        side,
        entry,
        initial_sl,
        initial_sl,
        tp1_price,
        str(tp1_order_id),
        str(sl_order_id),
    ))


def mark_trade_inactive(symbol):
    db_execute("""
    UPDATE trades
    SET is_active = 0, updated_at = CURRENT_TIMESTAMP
    WHERE symbol = ?
    """, (symbol,))


def update_trade_sl(symbol, new_sl, new_sl_order_id, breakeven_armed=True):
    db_execute("""
    UPDATE trades
    SET current_sl = ?, sl_order_id = ?, breakeven_armed = ?, updated_at = CURRENT_TIMESTAMP
    WHERE symbol = ?
    """, (new_sl, str(new_sl_order_id), 1 if breakeven_armed else 0, symbol))


# =========================
# BINANCE HELPERS
# =========================
def get_symbol_filters(symbol: str):
    info = get_client().futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            step_size = None
            tick_size = None
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step_size = f["stepSize"]
                elif f["filterType"] == "PRICE_FILTER":
                    tick_size = f["tickSize"]
            if step_size is None or tick_size is None:
                raise ValueError(f"Missing filters for symbol: {symbol}")
            return {
                "step_size": Decimal(step_size),
                "tick_size": Decimal(tick_size),
            }
    raise ValueError(f"Symbol not found: {symbol}")


def round_to_step(value: float, step: Decimal) -> float:
    d = Decimal(str(value))
    rounded = (d / step).quantize(Decimal("1"), rounding=ROUND_DOWN) * step
    return float(rounded)


def round_price(value: float, tick: Decimal) -> float:
    d = Decimal(str(value))
    rounded = (d / tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * tick
    return float(rounded)


def get_mark_price(symbol: str) -> float:
    return float(get_client().futures_mark_price(symbol=symbol)["markPrice"])


def get_position_amt(symbol: str) -> float:
    positions = get_client().futures_position_information(symbol=symbol)
    for pos in positions:
        amt = float(pos["positionAmt"])
        if amt != 0:
            return amt
    return 0.0


def set_leverage(symbol: str, leverage: int):
    return get_client().futures_change_leverage(symbol=symbol, leverage=leverage)


def calc_entry_qty(symbol: str, usd_amount: float) -> float:
    mark_price = get_mark_price(symbol)
    filters = get_symbol_filters(symbol)

    target_notional = max(usd_amount, MIN_NOTIONAL * MIN_NOTIONAL_BUFFER)
    raw_qty = target_notional / mark_price
    qty = round_to_step(raw_qty, filters["step_size"])
    return qty


def open_market_position(symbol: str, action: str, qty: float):
    side = "BUY" if action == "buy" else "SELL"
    return get_client().futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty
    )


def close_position_market(symbol: str):
    amt = get_position_amt(symbol)
    if amt == 0:
        return None

    side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)

    filters = get_symbol_filters(symbol)
    qty = round_to_step(qty, filters["step_size"])
    if qty <= 0:
        return None

    return get_client().futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty,
        reduceOnly=True
    )


def cancel_open_orders(symbol: str):
    try:
        return get_client().futures_cancel_all_open_orders(symbol=symbol)
    except Exception:
        return None


def cancel_order(symbol: str, order_id: str):
    try:
        return get_client().futures_cancel_order(symbol=symbol, orderId=order_id)
    except Exception:
        return None


def get_order(symbol: str, order_id: str):
    return get_client().futures_get_order(symbol=symbol, orderId=order_id)


def place_stop_loss(symbol: str, side: str, sl_price: float):
    exit_side = "SELL" if side == "LONG" else "BUY"
    return get_client().futures_create_order(
        symbol=symbol,
        side=exit_side,
        type="STOP_MARKET",
        stopPrice=sl_price,
        closePosition=True,
        workingType="MARK_PRICE"
    )


def place_native_tp_limits(symbol: str, side: str, tp_prices: list[float], tp_qtys: list[float]):
    exit_side = "SELL" if side == "LONG" else "BUY"
    orders = []

    for price, qty in zip(tp_prices, tp_qtys):
        if qty <= 0:
            continue

        orders.append(
            get_client().futures_create_order(
                symbol=symbol,
                side=exit_side,
                type="LIMIT",
                price=price,
                quantity=qty,
                timeInForce="GTC",
                reduceOnly=True
            )
        )

    return orders


# =========================
# BREAKEVEN LOGIC
# =========================
def move_sl_to_breakeven(symbol: str):
    row = db_fetchone("""
        SELECT side, entry, sl_order_id, breakeven_armed, is_active
        FROM trades
        WHERE symbol = ?
    """, (symbol,))

    if not row:
        return

    side = row["side"]
    entry = float(row["entry"])
    old_sl_order_id = row["sl_order_id"]
    breakeven_armed = int(row["breakeven_armed"])
    is_active = int(row["is_active"])

    if not is_active or breakeven_armed:
        return

    cancel_order(symbol, old_sl_order_id)
    time.sleep(0.3)

    new_sl_order = place_stop_loss(symbol, side, entry)
    update_trade_sl(symbol, entry, new_sl_order["orderId"], breakeven_armed=True)

    print(f"[breakeven] {symbol} moved SL to entry {entry}")


def breakeven_worker():
    while True:
        try:
            rows = db_fetchall("""
                SELECT symbol, tp1_order_id, breakeven_armed, is_active
                FROM trades
                WHERE is_active = 1
            """)

            for row in rows:
                symbol = row["symbol"]
                tp1_order_id = row["tp1_order_id"]
                breakeven_armed = int(row["breakeven_armed"])

                if breakeven_armed:
                    continue

                # Position already fully closed
                pos_amt = get_position_amt(symbol)
                if pos_amt == 0:
                    mark_trade_inactive(symbol)
                    continue

                try:
                    order = get_order(symbol, tp1_order_id)
                    if order["status"] == "FILLED":
                        move_sl_to_breakeven(symbol)
                except Exception as e:
                    print(f"[breakeven_worker] failed checking TP1 for {symbol}: {e}")

        except Exception as e:
            print(f"[breakeven_worker] error: {e}")

        time.sleep(2)


def start_breakeven_worker():
    t = threading.Thread(target=breakeven_worker, daemon=True)
    t.start()
    return t


# =========================
# ROUTES
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json or {}

    if data.get("secret") != SECRET:
        return jsonify({"error": "unauthorized"}), 403

    try:
        action = data["action"].lower()
        symbol = data["symbol"]

        entry = float(data["entry"])
        sl = float(data["sl"])
        tp1 = float(data["tp1"])
        tp2 = float(data["tp2"])
        tp3 = float(data["tp3"])
        tp4 = float(data["tp4"])
        tp5 = float(data["tp5"])

        if action not in ("buy", "sell"):
            return jsonify({"error": "invalid action"}), 400

        filters = get_symbol_filters(symbol)

        entry = round_price(entry, filters["tick_size"])
        sl = round_price(sl, filters["tick_size"])
        tps = [
            round_price(tp1, filters["tick_size"]),
            round_price(tp2, filters["tick_size"]),
            round_price(tp3, filters["tick_size"]),
            round_price(tp4, filters["tick_size"]),
            round_price(tp5, filters["tick_size"]),
        ]

        set_leverage(symbol, LEVERAGE)

        # Always close old position and cancel old orders first
        cancel_open_orders(symbol)
        close_position_market(symbol)
        time.sleep(0.8)

        remaining = abs(get_position_amt(symbol))
        if remaining != 0:
            return jsonify({"error": f"old position not fully closed: {remaining}"}), 400

        qty = calc_entry_qty(symbol, USD_PER_TRADE)
        if qty <= 0:
            return jsonify({"error": "quantity rounded to zero"}), 400

        opened_order = open_market_position(symbol, action, qty)
        time.sleep(0.5)

        actual_position = abs(get_position_amt(symbol))
        if actual_position <= 0:
            return jsonify({"error": "position did not open"}), 400

        side = "LONG" if action == "buy" else "SHORT"

        tp_qtys = [actual_position * p for p in TP_SPLITS]
        tp_qtys = [round_to_step(x, filters["step_size"]) for x in tp_qtys]

        rounded_sum = sum(tp_qtys)
        leftover = round_to_step(actual_position - rounded_sum, filters["step_size"])
        if leftover > 0:
            tp_qtys[-1] = round_to_step(tp_qtys[-1] + leftover, filters["step_size"])

        sl_order = place_stop_loss(symbol, side, sl)
        tp_orders = place_native_tp_limits(symbol, side, tps, tp_qtys)

        if not tp_orders:
            return jsonify({"error": "no TP orders placed"}), 500

        save_trade(
            symbol=symbol,
            side=side,
            entry=entry,
            initial_sl=sl,
            tp1_price=tps[0],
            tp1_order_id=tp_orders[0]["orderId"],
            sl_order_id=sl_order["orderId"],
        )

        return jsonify({
            "status": "success",
            "opened_order": opened_order,
            "sl_order": sl_order,
            "tp_orders": tp_orders,
            "plan": {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "sl": sl,
                "tps": tps,
                "tp_qtys": tp_qtys,
                "breakeven_after": "TP1"
            }
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "received": data
        }), 500


@app.route("/status", methods=["GET"])
def status():
    rows = db_fetchall("""
        SELECT symbol, side, entry, initial_sl, current_sl,
               tp1_price, tp1_order_id, sl_order_id,
               breakeven_armed, is_active, created_at, updated_at
        FROM trades
        ORDER BY updated_at DESC
    """)
    return jsonify([dict(r) for r in rows])


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


# =========================
# STARTUP
# =========================
init_db()
start_breakeven_worker()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)