import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import pytz
from flask import Flask, flash, redirect, render_template, request, url_for

DB_FILE = os.getenv("SIM_BOT_DB", "sim_bot.db")
DATE_FORMAT = "%d.%m.%Y %H:%M"
DUBAI_TZ = pytz.timezone("Asia/Dubai")
MIN_ALLOWED_AMOUNT = Decimal("0.01")
MAX_ALLOWED_AMOUNT = Decimal("100000")

app = Flask(__name__)
app.secret_key = os.getenv("WEB_APP_SECRET", "dev-secret-change-me")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def cents_to_decimal(cents: int) -> Decimal:
    return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))


def decimal_to_cents(value: Decimal) -> int:
    return int((value * 100).quantize(Decimal("1")))


def format_amount(cents: int) -> str:
    return str(cents_to_decimal(cents))


def get_wallet_balance_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(SUM(amount_cents),0) AS s FROM wallet_ledger").fetchone()
    return int(row["s"])


def get_tariffs(conn: sqlite3.Connection):
    return conn.execute("SELECT * FROM tariffs ORDER BY name").fetchall()


def get_users(conn: sqlite3.Connection):
    rows = conn.execute("SELECT * FROM users ORDER BY expiry_datetime").fetchall()
    now_dubai = datetime.now(DUBAI_TZ)

    users = []
    for row in rows:
        expiry_dt = DUBAI_TZ.localize(datetime.strptime(row["expiry_datetime"], DATE_FORMAT))
        time_left = expiry_dt - now_dubai
        status = "ok"
        if time_left.total_seconds() <= 0:
            status = "expired"
        elif time_left <= timedelta(days=3):
            status = "warning"

        users.append(
            {
                "name": row["name"],
                "phone": row["phone"],
                "tariff_name": row["tariff_name"] or "Не указан",
                "tariff_duration_days": row["tariff_duration_days"],
                "connection_datetime": row["connection_datetime"],
                "expiry_datetime": row["expiry_datetime"],
                "status": status,
            }
        )
    return users


def parse_amount_to_cents(value: str) -> int:
    normalized = value.strip().replace(",", ".")
    amount = Decimal(normalized)
    if amount < MIN_ALLOWED_AMOUNT or amount > MAX_ALLOWED_AMOUNT:
        raise ValueError("Сумма вне диапазона")
    return decimal_to_cents(amount)


@app.get("/")
def index():
    with closing(get_conn()) as conn:
        users = get_users(conn)
        tariffs = get_tariffs(conn)
        wallet_cents = get_wallet_balance_cents(conn)
        recent_wallet_ops = conn.execute(
            """
            SELECT amount_cents, type, description, created_at
            FROM wallet_ledger
            ORDER BY id DESC
            LIMIT 10
            """
        ).fetchall()

    return render_template(
        "index.html",
        users=users,
        tariffs=tariffs,
        wallet_cents=wallet_cents,
        format_amount=format_amount,
        recent_wallet_ops=recent_wallet_ops,
    )


@app.post("/wallet/topup")
def wallet_topup():
    amount_raw = request.form.get("amount", "")
    try:
        cents = parse_amount_to_cents(amount_raw)
    except (InvalidOperation, ValueError):
        flash("Некорректная сумма пополнения.", "error")
        return redirect(url_for("index"))

    with closing(get_conn()) as conn:
        now = datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)
        conn.execute(
            "INSERT INTO wallet_ledger(amount_cents, type, description, created_at) VALUES (?, ?, ?, ?)",
            (cents, "topup", "web topup", now),
        )
        conn.commit()

    flash(f"Баланс пополнен на {format_amount(cents)} AED", "success")
    return redirect(url_for("index"))


@app.post("/users/add")
def add_user():
    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    tariff_name = request.form.get("tariff_name", "").strip()
    connection_dt_raw = request.form.get("connection_datetime", "").strip()

    if not name or not phone:
        flash("Имя и телефон обязательны.", "error")
        return redirect(url_for("index"))

    with closing(get_conn()) as conn:
        exists = conn.execute("SELECT 1 FROM users WHERE phone=?", (phone,)).fetchone()
        if exists:
            flash("Пользователь с таким телефоном уже существует.", "error")
            return redirect(url_for("index"))

        tariff = conn.execute("SELECT * FROM tariffs WHERE name=?", (tariff_name,)).fetchone()
        if not tariff:
            flash("Выберите существующий тариф.", "error")
            return redirect(url_for("index"))

        try:
            if connection_dt_raw:
                connection_dt = DUBAI_TZ.localize(datetime.strptime(connection_dt_raw, DATE_FORMAT))
            else:
                connection_dt = datetime.now(DUBAI_TZ).replace(second=0, microsecond=0)
        except ValueError:
            flash("Неверный формат даты. Используйте ДД.ММ.ГГГГ ЧЧ:ММ", "error")
            return redirect(url_for("index"))

        now = datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)
        cost_cents = int(tariff["cost_cents"])
        duration_days = int(tariff["duration_days"])
        connection_dt_str = connection_dt.strftime(DATE_FORMAT)
        expiry_dt_str = (connection_dt + timedelta(days=duration_days)).strftime(DATE_FORMAT)

        conn.execute(
            "INSERT INTO wallet_ledger(amount_cents, type, description, created_at) VALUES (?, ?, ?, ?)",
            (-cost_cents, "charge", f"tariff charge {phone}", now),
        )
        conn.execute(
            """
            INSERT INTO users(
                phone, name, connection_datetime, expiry_datetime,
                tariff_name, tariff_cost_cents, tariff_duration_days,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                phone,
                name,
                connection_dt_str,
                expiry_dt_str,
                tariff_name,
                cost_cents,
                duration_days,
                now,
                now,
            ),
        )
        conn.commit()

    flash("Пользователь добавлен и тариф списан из кошелька.", "success")
    return redirect(url_for("index"))


@app.post("/users/delete/<phone>")
def delete_user(phone: str):
    with closing(get_conn()) as conn:
        user = conn.execute("SELECT name FROM users WHERE phone=?", (phone,)).fetchone()
        if not user:
            flash("Пользователь не найден.", "error")
            return redirect(url_for("index"))
        conn.execute("DELETE FROM users WHERE phone=?", (phone,))
        conn.commit()
    flash(f"Пользователь {user['name']} удален.", "success")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=True)
