import logging
import json
from datetime import datetime, time, timedelta
import os
import pytz
import re

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
    PicklePersistence,
)
from telegram.error import NetworkError, TelegramError

# --- НАСТРОЙКИ ---
BOT_TOKEN = "8285737349:AAFj5pKBjZwHyBX_Ma4viTL7f--OyQsG7KY"
JSON_FILE = "sim_users.json"

# Настройки времени (Дубай)
DUBAI_TZ = pytz.timezone('Asia/Dubai')
# ФИНАЛЬНОЕ ВРЕМЯ для ежедневной проверки
NOTIFICATION_HOUR_DUBAI = 9
NOTIFICATION_MINUTE_DUBAI = 0
# Формат даты для ввода и вывода
DATE_FORMAT = "%d.%m.%Y %H:%M"
# -----------------

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Определяем состояния для диалогов
(ADD_NAME, ADD_PHONE, ADD_TARIFF_SELECT, ADD_TARIFF_NEW_NAME, ADD_TARIFF_NEW_COST, ADD_CONNECTION_DATETIME) = range(6)
(DELETE_PHONE) = range(6, 7)
(EDIT_SELECT_USER, EDIT_CONNECTION_DATETIME) = range(7, 9)
(WALLET_MENU, WALLET_ADD_FUNDS) = range(9, 11)

# --- УТИЛИТЫ UI ---

def get_main_keyboard():
    """Возвращает разметку главного меню."""
    keyboard = [
        ["➕ Добавить", "📋 Список"],
        ["💰 Кошелек", "✏️ Редактировать"],
        ["🗑️ Удалить"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)


def get_today_date_keyboard():
    """Возвращает разметку для ввода даты подключения."""
    keyboard = [
        ["Сегодня"],
        ["/cancel"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)


def format_timedelta(td: timedelta) -> str:
    """Преобразует timedelta в удобочитаемый формат Дни/Часы/Минуты."""
    if td.total_seconds() <= 0:
        return ""

    seconds = int(td.total_seconds())
    if seconds < 60:
        return "< 1 мин."

    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60

    parts = []
    if days > 0:
        parts.append(f"{days} д.")
    if hours > 0:
        parts.append(f"{hours} ч.")
    if minutes > 0:
        parts.append(f"{minutes} мин.")

    return " ".join(parts)


# --- Функции для работы с JSON (наша "база данных") ---

def load_data():
    """Загружает данные из JSON-файла. Автоматически мигрирует старый формат."""
    if not os.path.exists(JSON_FILE):
        return {"users": {}, "wallet": 0.0, "tariffs": {}}

    try:
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Миграция: если данные выглядят как старый словарь пользователей (ключи - номера телефонов)
        # Проверяем наличие ключа "users", если нет - значит это старый формат или пустой файл
        if "users" not in data and data:
            # Эвристика: если ключи похожи на телефоны или файл не пустой
            logger.info("Обнаружен старый формат данных. Выполняю миграцию...")
            new_data = {
                "users": data,
                "wallet": 0.0,
                "tariffs": {}
            }
            save_data(new_data)  # Сразу сохраняем новую структуру
            return new_data
        if not data:
            return {"users": {}, "wallet": 0.0, "tariffs": {}}

        return data

    except json.JSONDecodeError:
        logger.warning(f"Файл {JSON_FILE} поврежден или пуст. Создаю новый.")
        return {"users": {}, "wallet": 0.0, "tariffs": {}}
    except Exception as e:
        logger.error(f"Ошибка при чтении {JSON_FILE}: {e}")
        return {"users": {}, "wallet": 0.0, "tariffs": {}}


def save_data(data):
    """Сохраняет данные в JSON-файл."""
    try:
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Ошибка при сохранении {JSON_FILE}: {e}")


# --- Вспомогательная функция проверки администратора и парсинга телефона ---

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет, является ли пользователь администратором."""
    admin_chat_id = context.bot_data.get('admin_chat_id')

    if not admin_chat_id:
        context.bot_data['admin_chat_id'] = update.effective_chat.id
        await update.message.reply_text(
            "Вы назначены администратором этого бота. ✅\n"
            "Уведомления о тарифах будут приходить вам."
        )
        return True

    if update.effective_chat.id != admin_chat_id:
        await update.message.reply_text("У вас нет прав для выполнения этой команды. 🔒", reply_markup=get_main_keyboard())
        return False

    return True


def extract_phone_from_text(text_input: str) -> str:
    """Извлекает только номер телефона из строки 'Имя (номер)' или возвращает очищенный номер."""
    # Если формат 'Имя (номер)'
    if "(" in text_input and ")" in text_input:
        match = re.search(r'\((.*?)\)', text_input)
        if match:
            return match.group(1).strip().replace(" ", "")

    # Если это просто номер или текст, возвращаем очищенный текст
    return text_input.strip().replace(" ", "")


# --- Обработчик ошибок ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логирует ошибки, вызванные обработчиками."""
    logger.error("Произошла ошибка в обработчике:", exc_info=context.error)

    if update and isinstance(update, Update) and update.effective_chat:
        if isinstance(context.error, (NetworkError, TelegramError)):
            message = "⚠️ *Временная проблема с сетью Telegram.* Попробуйте повторить команду через минуту."
        else:
            message = "❌ *Произошла внутренняя ошибка.* Проверьте консоль."

        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message,
                parse_mode="Markdown",
                reply_markup=get_main_keyboard()
            )
        except Exception:
            pass


# --- Основные команды ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет приветственное сообщение и регистрирует админа."""
    await update.message.reply_text(
        "Привет! Я бот для отслеживания тарифов SIM-карт. 📱\n"
        "Я буду присылать уведомления в 9:00 по Дубаю (UTC+4).\n"
        "Тариф действует ровно 30 дней с момента подключения.",
        reply_markup=get_main_keyboard()
    )

    await is_admin(update, context)


async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список всех сотрудников с точным оставшимся временем."""
    if not await is_admin(update, context):
        return

    data = load_data()
    users = data.get("users", {})
    wallet = data.get("wallet", 0.0)

    if not users:
        await update.message.reply_text(
            f"💰 Баланс: *{wallet} AED*\n\nСписок сотрудников пуст. 🤷‍♂️",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
        return

    message = f"💰 Баланс: *{wallet} AED*\n"
    message += "📄 *Список сотрудников и дат:*\n\n"
    sorted_users = sorted(users.items(), key=lambda item: item[1].get('expiry_datetime', ''))

    now_dubai = datetime.now(DUBAI_TZ)

    for phone, details in sorted_users:
        name = details['name']
        tariff = details.get('tariff_name', 'Не указан')
        conn_dt_str = details.get('connection_datetime', 'Н/Д')
        expiry_dt_str = details.get('expiry_datetime', 'Н/Д')

        status_icon = ""
        remaining_time = ""

        try:
            # Изменили формат на DATE_FORMAT
            expiry_dt_obj = DUBAI_TZ.localize(datetime.strptime(expiry_dt_str, DATE_FORMAT))

            time_left: timedelta = expiry_dt_obj - now_dubai
            remaining_time = format_timedelta(time_left)

            if time_left.total_seconds() <= 0:
                time_overdue = format_timedelta(now_dubai - expiry_dt_obj)
                status_icon = f"❗️ (Просрочено: {time_overdue})"
                remaining_time = ""
            elif time_left.total_seconds() < timedelta(days=1).total_seconds():
                status_icon = "⚠️ (Меньше 1 дн.)"
            elif time_left.total_seconds() < timedelta(days=3).total_seconds():
                status_icon = "⚠️ (Меньше 3 дн.)"
            else:
                status_icon = "✅"

            message += f"👤 *{name}* ({phone})\n"
            message += f"   ├─ Тариф: {tariff}\n"
            message += f"   ├─ Подключен: {conn_dt_str}\n"
            message += f"   └─ До: *{expiry_dt_str}* {status_icon}\n"

            if remaining_time:
                message += f"   └─ Осталось: {remaining_time}\n"

        except ValueError:
            message += f"👤 *{name}* ({phone})\n"
            message += f"   └─ ОШИБКА ДАТЫ/ВРЕМЕНИ: {expiry_dt_str}\n"

    await update.message.reply_text(message, parse_mode="Markdown", reply_markup=get_main_keyboard())


# --- Диалог: Кошелек ---

async def wallet_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню кошелька."""
    if not await is_admin(update, context):
        return ConversationHandler.END

    data = load_data()
    wallet = data.get("wallet", 0.0)

    keyboard = [
        ["➕ Пополнить"],
        ["🔙 Назад"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    await update.message.reply_text(
        f"💰 *Кошелек*\n"
        f"Текущий баланс: *{wallet} AED*\n\n"
        f"Выберите действие:",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )
    return WALLET_MENU


async def wallet_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор в меню кошелька."""
    text = update.message.text

    if text == "🔙 Назад":
        await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    if text == "➕ Пополнить":
        await update.message.reply_text("Введите сумму пополнения (в AED):", reply_markup=ReplyKeyboardRemove())
        return WALLET_ADD_FUNDS

    await update.message.reply_text("Неизвестная команда.", reply_markup=get_main_keyboard())
    return ConversationHandler.END


async def wallet_add_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод суммы."""
    text = update.message.text.strip()

    try:
        amount = float(text)
        data = load_data()
        data["wallet"] = data.get("wallet", 0.0) + amount
        save_data(data)

        await update.message.reply_text(
            f"✅ Баланс пополнен на *{amount} AED*.\n"
            f"Текущий баланс: *{data['wallet']} AED*",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard()
        )
    except ValueError:
        await update.message.reply_text(
            "⛔️ Ошибка! Введите корректное число.",
            reply_markup=get_main_keyboard()
        )

    return ConversationHandler.END


# --- Диалог: Добавление сотрудника (ConversationHandler) ---

async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает диалог добавления сотрудника."""
    if not await is_admin(update, context):
        return ConversationHandler.END

    await update.message.reply_text("Введите *имя* сотрудника:", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
    return ADD_NAME


async def add_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает имя и запрашивает номер телефона."""
    context.user_data['name'] = update.message.text.strip()
    await update.message.reply_text("Введите *номер телефона* (он будет ключом):", parse_mode="Markdown")
    return ADD_PHONE


async def add_get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает номер и запрашивает тариф."""
    phone = update.message.text.strip().replace(" ", "")

    data = load_data()
    users = data.get("users", {})
    if phone in users:
        await update.message.reply_text(
            f"❗️ Этот номер ({phone}) уже есть в базе. Сначала удалите старую запись (/delete).\n\n"
            "Добавление отменено.", reply_markup=get_main_keyboard()
        )
        context.user_data.clear()
        return ConversationHandler.END

    context.user_data['phone'] = phone

    # Показываем список тарифов
    tariffs = data.get("tariffs", {})
    keyboard = []
    for t_name, t_cost in tariffs.items():
        keyboard.append([f"{t_name} ({t_cost} AED)"])
    keyboard.append(["➕ Новый тариф"])
    keyboard.append(["/cancel"])

    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    await update.message.reply_text(
        "Выберите *тариф* для подключения (сумма спишется с кошелька):",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )
    return ADD_TARIFF_SELECT


async def add_tariff_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор тарифа."""
    text = update.message.text.strip()

    if text == "➕ Новый тариф":
        await update.message.reply_text("Введите *название* нового тарифа:", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
        return ADD_TARIFF_NEW_NAME

    # Пытаемся распарсить "Name (Cost AED)"
    match = re.search(r'^(.*?) \((\d+(\.\d+)?) AED\)$', text)
    if match:
        tariff_name = match.group(1)
        tariff_cost = float(match.group(2))
        context.user_data['tariff_name'] = tariff_name
        context.user_data['tariff_cost'] = tariff_cost

        await request_connection_date(update)
        return ADD_CONNECTION_DATETIME

    await update.message.reply_text("Пожалуйста, выберите тариф из списка или создайте новый.", reply_markup=get_main_keyboard())
    return ConversationHandler.END


async def add_tariff_new_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['new_tariff_name'] = update.message.text.strip()
    await update.message.reply_text("Введите *стоимость* тарифа в дирхамах (только число):", parse_mode="Markdown")
    return ADD_TARIFF_NEW_COST


async def add_tariff_new_cost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cost = float(update.message.text.strip())
        name = context.user_data['new_tariff_name']

        # Сохраняем тариф в базу сразу
        data = load_data()
        data["tariffs"][name] = cost
        save_data(data)

        context.user_data['tariff_name'] = name
        context.user_data['tariff_cost'] = cost

        await update.message.reply_text(f"✅ Тариф *{name}* ({cost} AED) сохранен.", parse_mode="Markdown")
        await request_connection_date(update)
        return ADD_CONNECTION_DATETIME

    except ValueError:
        await update.message.reply_text("⛔️ Введите корректное число.", parse_mode="Markdown")
        return ADD_TARIFF_NEW_COST


async def request_connection_date(update: Update):
    """Вспомогательная функция запроса даты."""
    await update.message.reply_text(
        "Введите *дату и время ПОДКЛЮЧЕНИЯ* тарифа в формате *ДД.ММ.ГГГГ ЧЧ:ММ* (например, 20.11.2025 15:30).\n"
        "Тариф действует ровно 30 дней.",
        parse_mode="Markdown",
        reply_markup=get_today_date_keyboard()
    )


# Эта функция используется и для добавления, и для редактирования, поэтому логика вынесена
async def save_connection_datetime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает дату/время подключения, вычисляет дату/время окончания и сохраняет."""
    dt_input = update.message.text.strip()

    try:
        if dt_input.lower() == "сегодня":
            connection_dt_dubai = datetime.now(DUBAI_TZ).replace(second=0, microsecond=0)
            connection_dt_str = connection_dt_dubai.strftime(DATE_FORMAT)
            await update.message.reply_text(f"✅ Использую текущее время подключения (Дубай): *{connection_dt_str}*", parse_mode="Markdown")
        else:
            # Проверка формата DATE_FORMAT (%d.%m.%Y %H:%M)
            connection_dt = datetime.strptime(dt_input, DATE_FORMAT)
            connection_dt_dubai = DUBAI_TZ.localize(connection_dt)
            connection_dt_str = dt_input

    except ValueError:
        mode = context.user_data.get('mode')  # 'add' или 'edit'
        text = "⛔️ *Неверный формат!* \nПожалуйста, введите в формате *ДД.ММ.ГГГГ ЧЧ:ММ* или нажмите 'Сегодня'."

        # Если редактируем, возвращаемся в состояние редактирования, иначе - добавления
        return_state = EDIT_CONNECTION_DATETIME if mode == 'edit' else ADD_CONNECTION_DATETIME

        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_today_date_keyboard())
        return return_state

    # --- Точный расчет: +30 дней ---
    expiry_dt_dubai = connection_dt_dubai + timedelta(days=30)
    expiry_dt_str = expiry_dt_dubai.strftime(DATE_FORMAT)

    name = context.user_data['name']
    phone = context.user_data['phone']
    mode = context.user_data.get('mode', 'add')

    tariff_name = context.user_data.get('tariff_name')
    tariff_cost = context.user_data.get('tariff_cost', 0.0)

    data = load_data()
    users = data.get("users", {})
    wallet = data.get("wallet", 0.0)

    # Логика списания только при добавлении
    wallet_msg = ""
    if mode == 'add':
        wallet -= tariff_cost
        data["wallet"] = wallet
        wallet_msg = f"\n💸 Списано: *{tariff_cost} AED*\n💰 Остаток: *{wallet} AED*"

        # Создаем запись
        users[phone] = {
            "name": name,
            "connection_datetime": connection_dt_str,
            "expiry_datetime": expiry_dt_str,
            "tariff_name": tariff_name,
            "tariff_cost": tariff_cost
        }
    else:
        # Редактирование (только даты пока)
        if phone in users:
            users[phone].update({
                "connection_datetime": connection_dt_str,
                "expiry_datetime": expiry_dt_str
            })

    data["users"] = users
    save_data(data)

    action_text = "Подключен" if mode == 'add' else "обновлен"

    await update.message.reply_text(
        f"✅ *Тариф {action_text}:*\n"
        f"Имя: {name}\n"
        f"Тариф: {tariff_name or 'Old'}\n"
        f"Подключен: {connection_dt_str}\n"
        f"Истекает: *{expiry_dt_str}*"
        f"{wallet_msg}",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )

    context.user_data.clear()
    return ConversationHandler.END


# --- Диалог: Удаление сотрудника ---

async def delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает диалог удаления. Показывает список для выбора."""
    if not await is_admin(update, context):
        return ConversationHandler.END

    data = load_data()
    users = data.get("users", {})
    if not users:
        await update.message.reply_text("Список пуст. 🤷‍♂️", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    keyboard = []
    for phone, details in users.items():
        keyboard.append([f"{details['name']} ({phone})"])

    keyboard.append(["/cancel"])
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        "Выберите сотрудника для удаления:",
        reply_markup=reply_markup
    )
    return DELETE_PHONE


async def delete_get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает выбор (номер) и удаляет."""
    text_input = update.message.text
    phone = extract_phone_from_text(text_input)

    data = load_data()
    users = data.get("users", {})

    if phone not in users:
        await update.message.reply_text(
            f"❗️ Номер '{phone}' не найден в базе.",
            reply_markup=get_main_keyboard()
        )
        return ConversationHandler.END

    removed_details = users.pop(phone)
    data["users"] = users
    save_data(data)

    await update.message.reply_text(
        f"🗑 *Сотрудник удален:*\n"
        f"{removed_details['name']} ({phone})",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )
    return ConversationHandler.END


# --- Диалог: Редактирование сотрудника ---

async def edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает диалог редактирования. Показывает список для выбора."""
    if not await is_admin(update, context):
        return ConversationHandler.END

    data = load_data()
    users = data.get("users", {})
    if not users:
        await update.message.reply_text("Список пуст. 🤷‍♂️", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    keyboard = []
    for phone, details in users.items():
        keyboard.append([f"{details['name']} ({phone}) - до {details.get('expiry_datetime', 'Н/Д')}"])

    keyboard.append(["/cancel"])
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        "✏️ Выберите сотрудника, чтобы изменить дату подключения:",
        reply_markup=reply_markup
    )
    return EDIT_SELECT_USER


async def edit_select_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получает выбор сотрудника и запрашивает новую дату/время."""
    text_input = update.message.text
    phone = extract_phone_from_text(text_input)

    data = load_data()
    users = data.get("users", {})

    if phone not in users:
        await update.message.reply_text(
            f"❗️ Номер '{phone}' не найден в базе. Попробуйте выбрать из списка.",
            reply_markup=get_main_keyboard()
        )
        return ConversationHandler.END

    # Сохраняем данные для последующего сохранения
    context.user_data['phone'] = phone
    context.user_data['name'] = users[phone]['name']
    context.user_data['mode'] = 'edit'  # Флаг для функции сохранения

    await update.message.reply_text(
        f"Выбран сотрудник: *{users[phone]['name']}* ({phone}).\n"
        f"Текущая дата подключения: {users[phone].get('connection_datetime', 'Н/Д')}\n\n"
        "Введите *НОВУЮ дату и время ПОДКЛЮЧЕНИЯ* в формате *ДД.ММ.ГГГГ ЧЧ:ММ*:",
        parse_mode="Markdown",
        reply_markup=get_today_date_keyboard()
    )
    return EDIT_CONNECTION_DATETIME


# --- Общие команды ---

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет текущий диалог."""
    context.user_data.clear()
    await update.message.reply_text(
        "Действие отменено.",
        reply_markup=get_main_keyboard()
    )
    return ConversationHandler.END


# --- Ежедневная проверка (JobQueue) ---

async def check_expirations(context: ContextTypes.DEFAULT_TYPE):
    """Проверяет сроки и отправляет уведомление админу за 3 дня и за 1 день."""

    now_dubai = datetime.now(DUBAI_TZ).replace(second=0, microsecond=0)
    logger.info(f"Запускаю точную проверку тарифов. Текущее время Дубая: {now_dubai.strftime(DATE_FORMAT)}")

    admin_chat_id = context.bot_data.get('admin_chat_id')
    if not admin_chat_id:
        logger.warning("Проверка пропущена: admin_chat_id не установлен.")
        return

    data = load_data()
    users = data.get("users", {})

    if not users:
        logger.info("Проверка завершена: база данных пуста.")
        return

    notifications = {
        3: [],  # 3-дневное предупреждение
        1: [],  # 1-дневное предупреждение
    }

    for phone, details in users.items():
        expiry_dt_str = details.get('expiry_datetime')
        if not expiry_dt_str:
            continue

        try:
            # Парсим дату в новом формате
            expiry_dt_obj = DUBAI_TZ.localize(datetime.strptime(expiry_dt_str, DATE_FORMAT))
            time_left: timedelta = expiry_dt_obj - now_dubai

            # 1. 1-дневное (Завтра истекает): если осталось <= 1 день (24h) и > 0
            if time_left <= timedelta(days=1) and time_left > timedelta(seconds=0):
                notifications[1].append((phone, details, time_left))

            # 2. 3-дневное: если осталось <= 3 дня (72h) и > 1 день (24h)
            elif time_left <= timedelta(days=3) and time_left > timedelta(days=1):
                notifications[3].append((phone, details, time_left))

        except ValueError:
            logger.error(f"Ошибка парсинга даты для {phone}: {expiry_dt_str}")

    users_notified_count = 0

    # Отправляем 1-дневное уведомление первым
    for days in [1, 3]:
        users_list = notifications[days]
        if not users_list:
            continue

        warning_time_str = "через 3 дня" if days == 3 else "завтра"

        message = (
            f"🔔 *ВНИМАНИЕ! Тариф истекает {warning_time_str}:*\n\n"
        )

        for phone, details, time_left in users_list:
            remaining_str = format_timedelta(time_left)
            message += f"👤 *{details['name']}*\n"
            message += f"   └─ Истекает в: {details['expiry_datetime']} (Осталось: {remaining_str})\n"

        try:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=message,
                parse_mode="Markdown"
            )
            users_notified_count += len(users_list)
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление админу: {e}")

    if users_notified_count > 0:
        logger.info(f"Отправлено {users_notified_count} уведомлений в двух окнах.")
    else:
        logger.info("Проверка завершена: нет тарифов для уведомления.")


# --- Функция Main ---

def main():
    """Запускает бота."""

    persistence = PicklePersistence(filepath="bot_persistence")

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .persistence(persistence)
        .build()
    )

    # --- Регистрируем обработчики ---

    # Хендлер для добавления
    add_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("add", add_start),
            MessageHandler(filters.Regex(r'^➕ Добавить$') & ~filters.COMMAND, add_start)
        ],
        states={
            ADD_NAME: [MessageHandler(filters.ALL & ~filters.COMMAND, add_get_name)],
            ADD_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_get_phone)],
            ADD_TARIFF_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tariff_select)],
            ADD_TARIFF_NEW_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tariff_new_name)],
            ADD_TARIFF_NEW_COST: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tariff_new_cost)],
            ADD_CONNECTION_DATETIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_connection_datetime)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="add_conversation"
    )

    # Хендлер для удаления
    delete_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("delete", delete_start),
            MessageHandler(filters.Regex(r'^🗑️ Удалить$') & ~filters.COMMAND, delete_start)
        ],
        states={
            DELETE_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_get_phone)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="delete_conversation"
    )

    # Хендлер для редактирования
    edit_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("edit", edit_start),
            MessageHandler(filters.Regex(r'^✏️ Редактировать$') & ~filters.COMMAND, edit_start)
        ],
        states={
            EDIT_SELECT_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_select_user)],
            EDIT_CONNECTION_DATETIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_connection_datetime)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="edit_conversation"
    )

    # Хендлер для кошелька
    wallet_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("wallet", wallet_start),
            MessageHandler(filters.Regex(r'^💰 Кошелек$') & ~filters.COMMAND, wallet_start)
        ],
        states={
            WALLET_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_menu_handler)],
            WALLET_ADD_FUNDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_add_funds)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=True,
        name="wallet_conversation"
    )

    # Общие обработчики (команды и кнопки главного меню, не начинающие диалог)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("list", list_users))
    application.add_handler(MessageHandler(filters.Regex(r'^📋 Список$') & ~filters.COMMAND, list_users))

    # Добавляем диалоги
    application.add_handler(add_conv_handler)
    application.add_handler(delete_conv_handler)
    application.add_handler(edit_conv_handler)
    application.add_handler(wallet_conv_handler)

    application.add_error_handler(error_handler)

    # --- Настройка ежедневной задачи (JobQueue) ---
    job_queue = application.job_queue

    job_queue.run_daily(
        check_expirations,
        time=time(hour=NOTIFICATION_HOUR_DUBAI, minute=NOTIFICATION_MINUTE_DUBAI, tzinfo=DUBAI_TZ),
        job_kwargs={"misfire_grace_time": 15 * 60}
    )

    print(f"Бот запущен... Ежедневная проверка в {NOTIFICATION_HOUR_DUBAI}:{NOTIFICATION_MINUTE_DUBAI:02d} по Дубаю.")
    application.run_polling()


if __name__ == "__main__":
    main()
