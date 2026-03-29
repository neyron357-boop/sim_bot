import json
import logging
import os
import re
import sqlite3
from io import BytesIO
from contextlib import closing
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional

import pytz
from openpyxl import Workbook
from telegram import Document, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.error import NetworkError, TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    PicklePersistence,
    filters,
)

# --- НАСТРОЙКИ ---
BOT_TOKEN = "8551565500:AAELp8GW-huaInfuiib563IDKNl5ObEQdRg"
DB_FILE = "sim_bot.db"
LEGACY_JSON_FILE = "sim_users.json"

DUBAI_TZ = pytz.timezone("Asia/Dubai")
NOTIFICATION_HOUR_DUBAI = 9
NOTIFICATION_MINUTE_DUBAI = 0
DATE_FORMAT = "%d.%m.%Y %H:%M"

MIN_ALLOWED_AMOUNT = Decimal("0.01")
MAX_ALLOWED_AMOUNT = Decimal("100000")

ENV_ADMIN_IDS = {
    int(raw.strip())
    for raw in os.getenv("ADMIN_CHAT_IDS", "").split(",")
    if raw.strip().isdigit()
}

# --- ЛОГИ ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- СОСТОЯНИЯ ---
(
    ADD_NAME,
    ADD_PHONE,
    ADD_TARIFF_SELECT,
    ADD_TARIFF_NEW_NAME,
    ADD_TARIFF_NEW_COST,
    ADD_TARIFF_NEW_DURATION,
    ADD_CONNECTION_DATETIME,
) = range(7)
(DELETE_PHONE,) = range(7, 8)
(EDIT_SELECT_USER, EDIT_CONNECTION_DATETIME) = range(8, 10)
(WALLET_MENU, WALLET_ADD_FUNDS, WALLET_EXPENSE) = range(10, 13)
(IMPORT_WAIT_FILE,) = range(13, 14)


# --- БАЗА ДАННЫХ ---
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_conn()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tariffs (
                name TEXT PRIMARY KEY,
                cost_cents INTEGER NOT NULL,
                duration_days INTEGER NOT NULL DEFAULT 30,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                phone TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                connection_datetime TEXT NOT NULL,
                expiry_datetime TEXT NOT NULL,
                tariff_name TEXT,
                tariff_cost_cents INTEGER NOT NULL DEFAULT 0,
                tariff_duration_days INTEGER NOT NULL DEFAULT 30,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS wallet_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount_cents INTEGER NOT NULL,
                type TEXT NOT NULL,
                description TEXT,
                actor_chat_id INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                phone TEXT,
                details TEXT,
                actor_chat_id INTEGER,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.commit()


def migrate_legacy_json() -> None:
    if not os.path.exists(LEGACY_JSON_FILE):
        return

    with closing(get_conn()) as conn:
        existing = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        if existing > 0:
            return

    try:
        with open(LEGACY_JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        logger.warning("Не удалось прочитать legacy JSON: %s", exc)
        return

    users = data.get("users", data if isinstance(data, dict) else {})
    wallet = data.get("wallet", 0.0) if isinstance(data, dict) else 0.0
    tariffs = data.get("tariffs", {}) if isinstance(data, dict) else {}

    now = datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)

    with closing(get_conn()) as conn:
        if Decimal(str(wallet)) > 0:
            conn.execute(
                "INSERT INTO wallet_ledger(amount_cents, type, description, created_at) VALUES (?, ?, ?, ?)",
                (decimal_to_cents(Decimal(str(wallet))), "migration_topup", "legacy wallet", now),
            )

        for name, raw_cost in tariffs.items():
            conn.execute(
                "INSERT OR REPLACE INTO tariffs(name, cost_cents, duration_days, created_at) VALUES (?, ?, ?, ?)",
                (name, decimal_to_cents(Decimal(str(raw_cost))), 30, now),
            )

        for phone, details in users.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO users(
                    phone, name, connection_datetime, expiry_datetime,
                    tariff_name, tariff_cost_cents, tariff_duration_days,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    phone,
                    details.get("name", "Unknown"),
                    details.get("connection_datetime", now),
                    details.get("expiry_datetime", now),
                    details.get("tariff_name"),
                    decimal_to_cents(Decimal(str(details.get("tariff_cost", 0)))),
                    30,
                    now,
                    now,
                ),
            )
        conn.commit()

    logger.info("Legacy JSON данные мигрированы в SQLite.")


# --- УТИЛИТЫ ---
def cents_to_decimal(cents: int) -> Decimal:
    return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))


def decimal_to_cents(value: Decimal) -> int:
    return int((value * 100).quantize(Decimal("1")))


def get_wallet_balance_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(SUM(amount_cents),0) AS s FROM wallet_ledger").fetchone()
    return int(row["s"])


def upsert_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, value))


def get_setting(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def log_audit(
    conn: sqlite3.Connection,
    action: str,
    phone: Optional[str],
    details: str,
    actor_chat_id: Optional[int],
) -> None:
    conn.execute(
        "INSERT INTO audit_log(action, phone, details, actor_chat_id, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, phone, details, actor_chat_id, datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)),
    )


def get_main_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["➕ Добавить", "📋 Список"],
            ["💰 Кошелек", "📥 Отчет"],
            ["📤 Экспорт", "📥 Импорт"],
            ["✏️ Редактировать", "🗑️ Удалить"],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def get_today_date_keyboard():
    return ReplyKeyboardMarkup([["Сегодня"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True)


def normalize_phone(value: str) -> str:
    return value.strip()


def extract_phone_from_text(text_input: str) -> str:
    if "(" in text_input and ")" in text_input:
        match = re.search(r"\((.*?)\)", text_input)
        if match:
            return normalize_phone(match.group(1))
    return normalize_phone(text_input)


def is_valid_phone(phone: str) -> bool:
    return bool(phone.strip())


def parse_amount_to_cents(value: str) -> int:
    normalized = value.strip().replace(",", ".")
    amount = Decimal(normalized)
    if amount < MIN_ALLOWED_AMOUNT or amount > MAX_ALLOWED_AMOUNT:
        raise ValueError("Сумма вне диапазона")
    return decimal_to_cents(amount)


def format_amount(cents: int) -> str:
    return str(cents_to_decimal(cents))


def format_timedelta(td: timedelta) -> str:
    if td.total_seconds() <= 0:
        return ""
    seconds = int(td.total_seconds())
    if seconds < 60:
        return "< 1 мин."
    days, seconds = divmod(seconds, 24 * 3600)
    hours, seconds = divmod(seconds, 3600)
    minutes, _ = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days} д.")
    if hours:
        parts.append(f"{hours} ч.")
    if minutes:
        parts.append(f"{minutes} мин.")
    return " ".join(parts)


def fetch_recent_wallet_ops(conn: sqlite3.Connection, limit: int = 3) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, amount_cents, type, description, actor_chat_id, created_at
        FROM wallet_ledger
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def wallet_operation_title(op_type: str, amount_cents: int) -> str:
    mapping = {
        "topup": "Пополнение",
        "migration_topup": "Миграция",
        "charge": "Списание тарифа",
        "expense": "Бытовой расход",
    }
    sign = "+" if amount_cents >= 0 else "-"
    return f"{mapping.get(op_type, op_type)} {sign}{format_amount(abs(amount_cents))} AED"


def format_recent_wallet_ops(ops: list[sqlite3.Row]) -> str:
    if not ops:
        return "Операций пока нет."

    lines = []
    for idx, row in enumerate(ops, start=1):
        lines.append(
            f"{idx}. {wallet_operation_title(row['type'], row['amount_cents'])}\n"
            f"   ├─ Дата: {row['created_at']}\n"
            f"   └─ Описание: {row['description'] or '—'}"
        )
    return "\n".join(lines)


async def send_wallet_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id, amount_cents, type, description, actor_chat_id, created_at
            FROM wallet_ledger
            ORDER BY id ASC
            """
        ).fetchall()
        final_balance = get_wallet_balance_cents(conn)

    if not rows:
        await update.message.reply_text("Операций для отчета пока нет.", reply_markup=get_main_keyboard())
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "Wallet report"
    ws.append(["№", "Дата", "Тип", "Описание", "Сумма (AED)", "Баланс после операции (AED)"])

    running_balance = 0
    for index, row in enumerate(rows, start=1):
        running_balance += int(row["amount_cents"])
        ws.append(
            [
                index,
                row["created_at"],
                row["type"],
                row["description"] or "",
                float(cents_to_decimal(row["amount_cents"])),
                float(cents_to_decimal(running_balance)),
            ]
        )

    report = BytesIO()
    wb.save(report)
    report_name = f"wallet_report_{datetime.now(DUBAI_TZ).strftime('%Y%m%d_%H%M')}.xlsx"
    report.name = report_name
    report.seek(0)

    await update.message.reply_document(
        document=report,
        filename=report_name,
        caption=(
            "📥 Отчет по операциям сформирован.\n"
            f"Всего операций: {len(rows)}\n"
            f"Итоговый баланс: {format_amount(final_balance)} AED"
        ),
        reply_markup=get_main_keyboard(),
    )


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat_id = update.effective_chat.id

    if ENV_ADMIN_IDS:
        if chat_id in ENV_ADMIN_IDS:
            return True
        await update.message.reply_text("У вас нет прав для выполнения этой команды. 🔒", reply_markup=get_main_keyboard())
        return False

    with closing(get_conn()) as conn:
        stored = get_setting(conn, "admin_chat_id")
        if stored is None:
            upsert_setting(conn, "admin_chat_id", str(chat_id))
            conn.commit()
            await update.message.reply_text(
                "Вы назначены администратором этого бота. ✅\nУведомления о тарифах будут приходить вам."
            )
            return True

        if chat_id != int(stored):
            await update.message.reply_text("У вас нет прав для выполнения этой команды. 🔒", reply_markup=get_main_keyboard())
            return False

    return True


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Произошла ошибка в обработчике:", exc_info=context.error)
    if update and isinstance(update, Update) and update.effective_chat:
        message = (
            "⚠️ *Временная проблема с сетью Telegram.* Попробуйте повторить команду через минуту."
            if isinstance(context.error, (NetworkError, TelegramError))
            else "❌ *Произошла внутренняя ошибка.* Проверьте консоль."
        )
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message,
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(),
            )
        except Exception:
            pass


# --- КОМАНДЫ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот для отслеживания тарифов SIM-карт. 📱\n"
        "Я присылаю уведомления в 9:00 по Дубаю и веду аудит операций.\n"
        "Тарифы могут иметь разную длительность (в днях).",
        reply_markup=get_main_keyboard(),
    )
    await is_admin(update, context)


async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    with closing(get_conn()) as conn:
        users = conn.execute("SELECT * FROM users ORDER BY expiry_datetime").fetchall()
        wallet_cents = get_wallet_balance_cents(conn)
        recent_ops = fetch_recent_wallet_ops(conn, limit=3)

    if not users:
        await update.message.reply_text(
            f"💰 Баланс: *{format_amount(wallet_cents)} AED*\n\nСписок сотрудников пуст. 🤷‍♂️",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    now_dubai = datetime.now(DUBAI_TZ)
    expiring_soon = []
    regular = []

    for row in users:
        expiry_dt_obj = DUBAI_TZ.localize(datetime.strptime(row["expiry_datetime"], DATE_FORMAT))
        time_left = expiry_dt_obj - now_dubai
        if time_left <= timedelta(days=3):
            expiring_soon.append((row, expiry_dt_obj, time_left))
        else:
            regular.append((row, expiry_dt_obj, time_left))

    def render_user_block(row: sqlite3.Row, expiry_dt_obj: datetime, time_left: timedelta) -> str:
        if time_left.total_seconds() <= 0:
            status_icon = f"❗️ (Просрочено: {format_timedelta(now_dubai - expiry_dt_obj)})"
            remaining = ""
        elif time_left <= timedelta(days=1):
            status_icon = "⚠️ (Меньше 1 дн.)"
            remaining = format_timedelta(time_left)
        elif time_left <= timedelta(days=3):
            status_icon = "⚠️ (Меньше 3 дн.)"
            remaining = format_timedelta(time_left)
        else:
            status_icon = "✅"
            remaining = format_timedelta(time_left)

        block = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 *{row['name']}*\n"
            f"📞 {row['phone']}\n"
            f"🏷 Тариф: {row['tariff_name'] or 'Не указан'} ({row['tariff_duration_days']} дн.)\n"
            f"🗓 Подключен: {row['connection_datetime']}\n"
            f"⏳ До: *{row['expiry_datetime']}* {status_icon}\n"
        )
        if remaining:
            block += f"⌛ Осталось: {remaining}\n"
        return block

    message = (
        f"💰 Баланс: *{format_amount(wallet_cents)} AED*\n"
        "🧾 *Последние 3 операции:*\n"
        f"{format_recent_wallet_ops(recent_ops)}\n\n"
        "📄 *Список сотрудников:*\n\n"
    )

    message += f"🔴 *До окончания меньше 3 дней*: {len(expiring_soon)}\n\n"
    if expiring_soon:
        for row, expiry_dt_obj, time_left in expiring_soon:
            message += render_user_block(row, expiry_dt_obj, time_left)
    else:
        message += "Нет сотрудников в этой категории.\n"

    message += f"\n🟢 *Остальные*: {len(regular)}\n\n"
    if regular:
        for row, expiry_dt_obj, time_left in regular:
            message += render_user_block(row, expiry_dt_obj, time_left)
    else:
        message += "Нет сотрудников в этой категории.\n"

    await update.message.reply_text(message, parse_mode="Markdown", reply_markup=get_main_keyboard())


def dump_database_payload(conn: sqlite3.Connection) -> dict:
    def fetch_table(name: str) -> list[dict]:
        rows = conn.execute(f"SELECT * FROM {name}").fetchall()
        return [dict(row) for row in rows]

    return {
        "exported_at": datetime.now(DUBAI_TZ).strftime(DATE_FORMAT),
        "version": 1,
        "settings": fetch_table("settings"),
        "tariffs": fetch_table("tariffs"),
        "users": fetch_table("users"),
        "wallet_ledger": fetch_table("wallet_ledger"),
        "audit_log": fetch_table("audit_log"),
    }


def restore_database_payload(conn: sqlite3.Connection, payload: dict) -> None:
    required = {"settings", "tariffs", "users", "wallet_ledger", "audit_log"}
    missing = required.difference(payload.keys())
    if missing:
        raise ValueError(f"В файле не хватает разделов: {', '.join(sorted(missing))}")

    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM settings")
        conn.execute("DELETE FROM tariffs")
        conn.execute("DELETE FROM users")
        conn.execute("DELETE FROM wallet_ledger")
        conn.execute("DELETE FROM audit_log")

        for row in payload["settings"]:
            conn.execute("INSERT INTO settings(key, value) VALUES (?, ?)", (row["key"], row["value"]))

        for row in payload["tariffs"]:
            conn.execute(
                "INSERT INTO tariffs(name, cost_cents, duration_days, created_at) VALUES (?, ?, ?, ?)",
                (row["name"], row["cost_cents"], row["duration_days"], row["created_at"]),
            )

        for row in payload["users"]:
            conn.execute(
                """
                INSERT INTO users(
                    phone, name, connection_datetime, expiry_datetime,
                    tariff_name, tariff_cost_cents, tariff_duration_days,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["phone"],
                    row["name"],
                    row["connection_datetime"],
                    row["expiry_datetime"],
                    row.get("tariff_name"),
                    row["tariff_cost_cents"],
                    row["tariff_duration_days"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )

        for row in payload["wallet_ledger"]:
            conn.execute(
                """
                INSERT INTO wallet_ledger(
                    id, amount_cents, type, description, actor_chat_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["amount_cents"],
                    row["type"],
                    row.get("description"),
                    row.get("actor_chat_id"),
                    row["created_at"],
                ),
            )

        for row in payload["audit_log"]:
            conn.execute(
                """
                INSERT INTO audit_log(
                    id, action, phone, details, actor_chat_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["action"],
                    row.get("phone"),
                    row.get("details"),
                    row.get("actor_chat_id"),
                    row["created_at"],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return

    with closing(get_conn()) as conn:
        payload = dump_database_payload(conn)

    report = BytesIO(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
    report_name = f"sim_bot_backup_{datetime.now(DUBAI_TZ).strftime('%Y%m%d_%H%M')}.json"
    report.name = report_name
    report.seek(0)

    await update.message.reply_document(
        document=report,
        filename=report_name,
        caption="📤 Резервная копия базы данных сформирована.",
        reply_markup=get_main_keyboard(),
    )


async def import_data_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return ConversationHandler.END

    await update.message.reply_text(
        "Отправьте JSON-файл, который был создан через *Экспорт*. Текущие данные будут полностью заменены.",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return IMPORT_WAIT_FILE


async def import_data_apply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc: Optional[Document] = update.message.document
    if not doc or not doc.file_name.lower().endswith(".json"):
        await update.message.reply_text("⛔️ Нужен JSON-файл с резервной копией.")
        return IMPORT_WAIT_FILE

    tg_file = await doc.get_file()
    raw = await tg_file.download_as_bytearray()

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        await update.message.reply_text("⛔️ Не удалось прочитать JSON. Проверьте файл.")
        return IMPORT_WAIT_FILE

    try:
        with closing(get_conn()) as conn:
            restore_database_payload(conn, payload)
            log_audit(conn, "data_import", None, f"import file: {doc.file_name}", update.effective_chat.id)
            conn.commit()
    except Exception as exc:
        await update.message.reply_text(f"⛔️ Импорт не выполнен: {exc}", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    await update.message.reply_text(
        "✅ Импорт завершен. Данные восстановлены из резервной копии.",
        reply_markup=get_main_keyboard(),
    )
    return ConversationHandler.END


# --- КОШЕЛЕК ---
async def wallet_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return ConversationHandler.END

    with closing(get_conn()) as conn:
        wallet_cents = get_wallet_balance_cents(conn)
        recent_ops = fetch_recent_wallet_ops(conn, limit=3)

    markup = ReplyKeyboardMarkup([["➕ Пополнить", "➖ Расход"], ["📥 Скачать отчет"], ["🔙 Назад"]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text(
        "💰 *Кошелек*\n"
        f"Текущий баланс: *{format_amount(wallet_cents)} AED*\n\n"
        "🧾 *Последние 3 операции:*\n"
        f"{format_recent_wallet_ops(recent_ops)}\n\n"
        "Выберите действие:",
        parse_mode="Markdown",
        reply_markup=markup,
    )
    return WALLET_MENU


async def wallet_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "🔙 Назад":
        await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard())
        return ConversationHandler.END
    if update.message.text == "📥 Скачать отчет":
        await send_wallet_report(update, context)
        return ConversationHandler.END
    if update.message.text == "➕ Пополнить":
        await update.message.reply_text("Введите сумму пополнения (в AED):", reply_markup=ReplyKeyboardRemove())
        return WALLET_ADD_FUNDS
    if update.message.text == "➖ Расход":
        await update.message.reply_text(
            "Введите бытовой расход в формате:\n`сумма; описание`\nНапример: `25.5; Такси`",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove(),
        )
        return WALLET_EXPENSE
    await update.message.reply_text("Неизвестная команда.", reply_markup=get_main_keyboard())
    return ConversationHandler.END


async def wallet_add_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cents = parse_amount_to_cents(update.message.text)
        with closing(get_conn()) as conn:
            balance_before = get_wallet_balance_cents(conn)
            conn.execute(
                "INSERT INTO wallet_ledger(amount_cents, type, description, actor_chat_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (cents, "topup", "manual topup", update.effective_chat.id, datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)),
            )
            balance = get_wallet_balance_cents(conn)
            log_audit(conn, "wallet_topup", None, f"+{format_amount(cents)} AED", update.effective_chat.id)
            conn.commit()

        await update.message.reply_text(
            f"✅ Баланс пополнен.\n"
            f"💼 Баланс до: *{format_amount(balance_before)} AED*\n"
            f"➕ Операция: *+{format_amount(cents)} AED*\n"
            f"💰 Баланс после: *{format_amount(balance)} AED*",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
    except (InvalidOperation, ValueError):
        await update.message.reply_text(
            "⛔️ Ошибка! Введите сумму в диапазоне от 0.01 до 100000 AED.",
            reply_markup=get_main_keyboard(),
        )
    return ConversationHandler.END


async def wallet_add_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        raw = update.message.text.strip()
        parts = [p.strip() for p in raw.split(";", 1)]
        if len(parts) != 2 or not parts[1]:
            raise ValueError("Некорректный формат")

        expense_cents = parse_amount_to_cents(parts[0])
        description = parts[1]

        with closing(get_conn()) as conn:
            balance_before = get_wallet_balance_cents(conn)

            conn.execute(
                "INSERT INTO wallet_ledger(amount_cents, type, description, actor_chat_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (-expense_cents, "expense", description, update.effective_chat.id, datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)),
            )
            balance_after = get_wallet_balance_cents(conn)
            log_audit(
                conn,
                "wallet_expense",
                None,
                f"-{format_amount(expense_cents)} AED; {description}",
                update.effective_chat.id,
            )
            conn.commit()

        await update.message.reply_text(
            f"✅ Бытовой расход учтен{' (долг)' if balance_after < 0 else ''}.\n"
            f"💼 Баланс до: *{format_amount(balance_before)} AED*\n"
            f"➖ Операция: *-{format_amount(expense_cents)} AED* ({description})\n"
            f"💰 Баланс после: *{format_amount(balance_after)} AED*",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
    except (InvalidOperation, ValueError):
        await update.message.reply_text(
            "⛔️ Неверный формат. Используйте: `сумма; описание`.\nНапример: `25.5; Такси`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )

    return ConversationHandler.END


# --- ДОБАВЛЕНИЕ ---
async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return ConversationHandler.END
    await update.message.reply_text("Введите *имя* сотрудника:", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
    context.user_data.clear()
    context.user_data["mode"] = "add"
    return ADD_NAME


async def add_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["name"] = update.message.text.strip()
    await update.message.reply_text("Введите *номер телефона* (в любом формате):", parse_mode="Markdown")
    return ADD_PHONE


async def add_get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone(update.message.text)
    if not is_valid_phone(phone):
        await update.message.reply_text("⛔️ Номер телефона не должен быть пустым.", parse_mode="Markdown")
        return ADD_PHONE

    with closing(get_conn()) as conn:
        exists = conn.execute("SELECT 1 FROM users WHERE phone=?", (phone,)).fetchone()
        tariffs = conn.execute("SELECT * FROM tariffs ORDER BY name").fetchall()

    if exists:
        await update.message.reply_text(f"❗️ Номер {phone} уже есть в базе.", reply_markup=get_main_keyboard())
        context.user_data.clear()
        return ConversationHandler.END

    context.user_data["phone"] = phone

    keyboard = [[f"{t['name']} ({format_amount(t['cost_cents'])} AED / {t['duration_days']} дн.)"] for t in tariffs]
    keyboard.append(["➕ Новый тариф"])
    keyboard.append(["/cancel"])

    await update.message.reply_text(
        "Выберите *тариф* для подключения:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True),
    )
    return ADD_TARIFF_SELECT


async def add_tariff_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "➕ Новый тариф":
        await update.message.reply_text("Введите *название* нового тарифа:", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
        return ADD_TARIFF_NEW_NAME

    match = re.search(r"^(.*?) \((\d+(?:\.\d+)?) AED / (\d+) дн\.\)$", text)
    if not match:
        await update.message.reply_text("Пожалуйста, выберите тариф из списка.", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    context.user_data["tariff_name"] = match.group(1)
    context.user_data["tariff_cost_cents"] = parse_amount_to_cents(match.group(2))
    context.user_data["tariff_duration_days"] = int(match.group(3))

    await request_connection_date(update)
    return ADD_CONNECTION_DATETIME


async def add_tariff_new_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_tariff_name"] = update.message.text.strip()
    await update.message.reply_text("Введите *стоимость* тарифа в AED:", parse_mode="Markdown")
    return ADD_TARIFF_NEW_COST


async def add_tariff_new_cost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["new_tariff_cost_cents"] = parse_amount_to_cents(update.message.text)
        await update.message.reply_text("Введите *длительность тарифа* в днях (например, 30):", parse_mode="Markdown")
        return ADD_TARIFF_NEW_DURATION
    except (InvalidOperation, ValueError):
        await update.message.reply_text("⛔️ Введите сумму тарифа от 0.01 до 100000 AED.", parse_mode="Markdown")
        return ADD_TARIFF_NEW_COST


async def add_tariff_new_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    if not raw.isdigit() or int(raw) <= 0 or int(raw) > 3650:
        await update.message.reply_text("⛔️ Длительность должна быть целым числом от 1 до 3650.")
        return ADD_TARIFF_NEW_DURATION

    name = context.user_data["new_tariff_name"]
    cost_cents = context.user_data["new_tariff_cost_cents"]
    duration_days = int(raw)

    with closing(get_conn()) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tariffs(name, cost_cents, duration_days, created_at) VALUES (?, ?, ?, ?)",
            (name, cost_cents, duration_days, datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)),
        )
        log_audit(
            conn,
            "tariff_upsert",
            None,
            f"{name}: {format_amount(cost_cents)} AED / {duration_days} дн.",
            update.effective_chat.id,
        )
        conn.commit()

    context.user_data["tariff_name"] = name
    context.user_data["tariff_cost_cents"] = cost_cents
    context.user_data["tariff_duration_days"] = duration_days

    await update.message.reply_text(
        f"✅ Тариф *{name}* сохранен: *{format_amount(cost_cents)} AED / {duration_days} дн.*",
        parse_mode="Markdown",
    )
    await request_connection_date(update)
    return ADD_CONNECTION_DATETIME


async def request_connection_date(update: Update):
    await update.message.reply_text(
        "Введите *дату и время ПОДКЛЮЧЕНИЯ* в формате *ДД.ММ.ГГГГ ЧЧ:ММ* или нажмите 'Сегодня'.",
        parse_mode="Markdown",
        reply_markup=get_today_date_keyboard(),
    )


async def save_connection_datetime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dt_input = update.message.text.strip()
    mode = context.user_data.get("mode", "add")

    try:
        if dt_input.lower() == "сегодня":
            connection_dt_dubai = datetime.now(DUBAI_TZ).replace(second=0, microsecond=0)
        else:
            connection_dt_dubai = DUBAI_TZ.localize(datetime.strptime(dt_input, DATE_FORMAT))
    except ValueError:
        await update.message.reply_text(
            "⛔️ Неверный формат. Используйте *ДД.ММ.ГГГГ ЧЧ:ММ* или кнопку 'Сегодня'.",
            parse_mode="Markdown",
            reply_markup=get_today_date_keyboard(),
        )
        return EDIT_CONNECTION_DATETIME if mode == "edit" else ADD_CONNECTION_DATETIME

    connection_dt_str = connection_dt_dubai.strftime(DATE_FORMAT)
    duration_days = int(context.user_data.get("tariff_duration_days", 30))
    expiry_dt_str = (connection_dt_dubai + timedelta(days=duration_days)).strftime(DATE_FORMAT)

    name = context.user_data["name"]
    phone = context.user_data["phone"]

    with closing(get_conn()) as conn:
        if mode == "add":
            tariff_name = context.user_data.get("tariff_name")
            cost_cents = int(context.user_data.get("tariff_cost_cents", 0))
            wallet_cents = get_wallet_balance_cents(conn)

            conn.execute(
                "INSERT INTO wallet_ledger(amount_cents, type, description, actor_chat_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (-cost_cents, "charge", f"tariff charge {phone}", update.effective_chat.id, datetime.now(DUBAI_TZ).strftime(DATE_FORMAT)),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO users(
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
                    datetime.now(DUBAI_TZ).strftime(DATE_FORMAT),
                    datetime.now(DUBAI_TZ).strftime(DATE_FORMAT),
                ),
            )
            new_balance = get_wallet_balance_cents(conn)
            log_audit(
                conn,
                "user_add",
                phone,
                f"{name}; {tariff_name}; {format_amount(cost_cents)} AED; {duration_days} дн.",
                update.effective_chat.id,
            )
            conn.commit()

            await update.message.reply_text(
                f"✅ *Тариф подключен{' (долг)' if new_balance < 0 else ''}:*\nИмя: {name}\nТариф: {tariff_name}\nПодключен: {connection_dt_str}\nИстекает: *{expiry_dt_str}*\n"
                f"💼 Баланс до: *{format_amount(wallet_cents)} AED*\n"
                f"💸 Списание: *-{format_amount(cost_cents)} AED*\n"
                f"💰 Баланс после: *{format_amount(new_balance)} AED*",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(),
            )
        else:
            conn.execute(
                "UPDATE users SET connection_datetime=?, expiry_datetime=?, updated_at=? WHERE phone=?",
                (connection_dt_str, expiry_dt_str, datetime.now(DUBAI_TZ).strftime(DATE_FORMAT), phone),
            )
            log_audit(conn, "user_edit_date", phone, f"new connection={connection_dt_str}", update.effective_chat.id)
            conn.commit()
            await update.message.reply_text(
                f"✅ Дата подключения обновлена.\nПодключен: {connection_dt_str}\nИстекает: *{expiry_dt_str}*",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(),
            )

    context.user_data.clear()
    return ConversationHandler.END


# --- УДАЛЕНИЕ / РЕДАКТИРОВАНИЕ ---
async def delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return ConversationHandler.END

    with closing(get_conn()) as conn:
        users = conn.execute("SELECT phone, name FROM users ORDER BY name").fetchall()

    if not users:
        await update.message.reply_text("Список пуст. 🤷‍♂️", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    keyboard = [[f"{u['name']} ({u['phone']})"] for u in users] + [["/cancel"]]
    await update.message.reply_text(
        "Выберите сотрудника для удаления:",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
    )
    return DELETE_PHONE


async def delete_get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = extract_phone_from_text(update.message.text)
    with closing(get_conn()) as conn:
        user = conn.execute("SELECT name FROM users WHERE phone=?", (phone,)).fetchone()
        if not user:
            await update.message.reply_text(f"❗️ Номер '{phone}' не найден.", reply_markup=get_main_keyboard())
            return ConversationHandler.END

        conn.execute("DELETE FROM users WHERE phone=?", (phone,))
        log_audit(conn, "user_delete", phone, user["name"], update.effective_chat.id)
        conn.commit()

    await update.message.reply_text(
        f"🗑 *Сотрудник удален:*\n{user['name']} ({phone})",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(),
    )
    return ConversationHandler.END


async def edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update, context):
        return ConversationHandler.END

    with closing(get_conn()) as conn:
        users = conn.execute("SELECT phone, name, expiry_datetime, tariff_duration_days FROM users ORDER BY name").fetchall()

    if not users:
        await update.message.reply_text("Список пуст. 🤷‍♂️", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    keyboard = [[f"{u['name']} ({u['phone']}) - до {u['expiry_datetime']}"] for u in users] + [["/cancel"]]
    await update.message.reply_text(
        "✏️ Выберите сотрудника, чтобы изменить дату подключения:",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
    )
    return EDIT_SELECT_USER


async def edit_select_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = extract_phone_from_text(update.message.text)
    with closing(get_conn()) as conn:
        user = conn.execute("SELECT * FROM users WHERE phone=?", (phone,)).fetchone()

    if not user:
        await update.message.reply_text(f"❗️ Номер '{phone}' не найден.", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    context.user_data["phone"] = phone
    context.user_data["name"] = user["name"]
    context.user_data["mode"] = "edit"
    context.user_data["tariff_duration_days"] = user["tariff_duration_days"]

    await update.message.reply_text(
        f"Выбран: *{user['name']}* ({phone}).\nТекущая дата подключения: {user['connection_datetime']}\n\n"
        "Введите *НОВУЮ дату и время ПОДКЛЮЧЕНИЯ*:",
        parse_mode="Markdown",
        reply_markup=get_today_date_keyboard(),
    )
    return EDIT_CONNECTION_DATETIME


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Действие отменено.", reply_markup=get_main_keyboard())
    return ConversationHandler.END


# --- УВЕДОМЛЕНИЯ ---
async def check_expirations(context: ContextTypes.DEFAULT_TYPE):
    now_dubai = datetime.now(DUBAI_TZ).replace(second=0, microsecond=0)

    with closing(get_conn()) as conn:
        if ENV_ADMIN_IDS:
            admin_chat_ids = list(ENV_ADMIN_IDS)
        else:
            stored = get_setting(conn, "admin_chat_id")
            admin_chat_ids = [int(stored)] if stored else []

        if not admin_chat_ids:
            logger.warning("Проверка пропущена: не найден администратор.")
            return

        users = conn.execute("SELECT * FROM users").fetchall()

    if not users:
        logger.info("Проверка завершена: база пользователей пуста.")
        return

    buckets = {7: [], 3: [], 1: [], 0: [], -1: []}
    for user in users:
        try:
            expiry = DUBAI_TZ.localize(datetime.strptime(user["expiry_datetime"], DATE_FORMAT))
            delta = expiry - now_dubai
            if timedelta(days=6) < delta <= timedelta(days=7):
                buckets[7].append((user, delta))
            elif timedelta(days=2) < delta <= timedelta(days=3):
                buckets[3].append((user, delta))
            elif timedelta(hours=0) < delta <= timedelta(days=1):
                buckets[1].append((user, delta))
            elif timedelta(hours=-24) < delta <= timedelta(seconds=0):
                buckets[0].append((user, delta))
            elif delta <= timedelta(days=-1):
                buckets[-1].append((user, delta))
        except ValueError:
            logger.error("Ошибка даты: %s %s", user["phone"], user["expiry_datetime"])

    labels = {
        7: "через 7 дней",
        3: "через 3 дня",
        1: "завтра",
        0: "сегодня",
        -1: "просроченные",
    }

    for chat_id in admin_chat_ids:
        for key in [7, 3, 1, 0, -1]:
            if not buckets[key]:
                continue
            text = f"🔔 *Контракты, которые истекают {labels[key]}:*\n\n"
            for user, delta in buckets[key]:
                left = format_timedelta(abs(delta)) if key == -1 else format_timedelta(delta)
                suffix = f"Просрочено: {left}" if key == -1 else f"Осталось: {left}"
                text += f"👤 *{user['name']}* ({user['phone']})\n└─ До: {user['expiry_datetime']} ({suffix})\n"
            try:
                await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            except Exception as exc:
                logger.error("Не удалось отправить уведомление %s: %s", chat_id, exc)


# --- MAIN ---
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Укажите BOT_TOKEN в переменной окружения.")

    init_db()
    migrate_legacy_json()

    persistence = PicklePersistence(filepath="bot_persistence")
    app = Application.builder().token(BOT_TOKEN).persistence(persistence).build()

    add_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("add", add_start),
            MessageHandler(filters.Regex(r"^➕ Добавить$") & ~filters.COMMAND, add_start),
        ],
        states={
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_get_name)],
            ADD_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_get_phone)],
            ADD_TARIFF_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tariff_select)],
            ADD_TARIFF_NEW_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tariff_new_name)],
            ADD_TARIFF_NEW_COST: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tariff_new_cost)],
            ADD_TARIFF_NEW_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tariff_new_duration)],
            ADD_CONNECTION_DATETIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_connection_datetime)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="add_conversation",
    )

    delete_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("delete", delete_start),
            MessageHandler(filters.Regex(r"^🗑️ Удалить$") & ~filters.COMMAND, delete_start),
        ],
        states={DELETE_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_get_phone)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="delete_conversation",
    )

    edit_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("edit", edit_start),
            MessageHandler(filters.Regex(r"^✏️ Редактировать$") & ~filters.COMMAND, edit_start),
        ],
        states={
            EDIT_SELECT_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_select_user)],
            EDIT_CONNECTION_DATETIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_connection_datetime)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="edit_conversation",
    )

    wallet_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("wallet", wallet_start),
            MessageHandler(filters.Regex(r"^💰 Кошелек$") & ~filters.COMMAND, wallet_start),
        ],
        states={
            WALLET_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_menu_handler)],
            WALLET_ADD_FUNDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_add_funds)],
            WALLET_EXPENSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_add_expense)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="wallet_conversation",
    )

    import_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("import_data", import_data_start),
            MessageHandler(filters.Regex(r"^📥 Импорт$") & ~filters.COMMAND, import_data_start),
        ],
        states={
            IMPORT_WAIT_FILE: [MessageHandler(filters.Document.ALL & ~filters.COMMAND, import_data_apply)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="import_conversation",
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("list", list_users))
    app.add_handler(CommandHandler("report", send_wallet_report))
    app.add_handler(CommandHandler("export_data", export_data))
    app.add_handler(MessageHandler(filters.Regex(r"^📋 Список$") & ~filters.COMMAND, list_users))
    app.add_handler(MessageHandler(filters.Regex(r"^📥 Отчет$") & ~filters.COMMAND, send_wallet_report))
    app.add_handler(MessageHandler(filters.Regex(r"^📤 Экспорт$") & ~filters.COMMAND, export_data))

    app.add_handler(add_conv_handler)
    app.add_handler(delete_conv_handler)
    app.add_handler(edit_conv_handler)
    app.add_handler(wallet_conv_handler)
    app.add_handler(import_conv_handler)

    app.add_error_handler(error_handler)

    app.job_queue.run_daily(
        check_expirations,
        time=time(hour=NOTIFICATION_HOUR_DUBAI, minute=NOTIFICATION_MINUTE_DUBAI, tzinfo=DUBAI_TZ),
        job_kwargs={"misfire_grace_time": 15 * 60},
    )

    print(f"Бот запущен... Ежедневная проверка в {NOTIFICATION_HOUR_DUBAI}:{NOTIFICATION_MINUTE_DUBAI:02d} по Дубаю.")
    app.run_polling()


if __name__ == "__main__":
    main()
