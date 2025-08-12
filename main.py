# main.py — aiogram 3.7+
import asyncio
import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice,
    BotCommand, ReplyKeyboardMarkup, KeyboardButton, PreCheckoutQuery
)

import config

router = Router()
BOT_UN = ""  # username бота (без @), подхватим при старте

# ======================== БАЗА ДАННЫХ =========================

def _db_connect():
    os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

DB = _db_connect()

def _db_init():
    DB.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            is_admin    INTEGER DEFAULT 0,
            created_at  INTEGER NOT NULL
        );
    """)
    DB.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            plan        TEXT NOT NULL,
            created_at  INTEGER NOT NULL,
            expires_at  INTEGER,           -- NULL = навсегда
            gifted_by   INTEGER,           -- кто подарил (если подарок)
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
    """)
    # старая схема channels могла быть без owner_id. создадим если нет
    DB.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            chat_id     INTEGER PRIMARY KEY,
            title       TEXT,
            added_at    INTEGER NOT NULL,
            owner_id    INTEGER,           -- добавлено
            username    TEXT               -- добавлено
        );
    """)
    # миграции столбцов, если вдруг отсутствуют
    cols = {r[1] for r in DB.execute("PRAGMA table_info(channels)")}
    if "owner_id" not in cols:
        DB.execute("ALTER TABLE channels ADD COLUMN owner_id INTEGER;")
    if "username" not in cols:
        DB.execute("ALTER TABLE channels ADD COLUMN username TEXT;")
    DB.commit()

_db_init()

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def ensure_user(user_id: int, username: str | None):
    DB.execute(
        "INSERT OR IGNORE INTO users(user_id, username, is_admin, created_at) VALUES(?,?,?,?)",
        (user_id, (username or ""), 1 if user_id == getattr(config, "ADMIN_ID", 0) else 0, now_ts())
    )
    if username is not None:
        DB.execute("UPDATE users SET username=? WHERE user_id=?", (username, user_id))
    DB.commit()

def has_active_subscription(user_id: int) -> bool:
    ts = now_ts()
    cur = DB.execute("""
        SELECT 1 FROM subscriptions
        WHERE user_id = ?
          AND (expires_at IS NULL OR expires_at > ?)
        ORDER BY COALESCE(expires_at, 1<<62) DESC
        LIMIT 1
    """, (user_id, ts))
    return cur.fetchone() is not None

def grant_subscription(user_id: int, plan: str, gifted_by: int | None = None):
    created = now_ts()
    exp = None
    if plan == "week":
        exp = created + 7 * 24 * 3600
    elif plan == "month":
        exp = created + 30 * 24 * 3600
    elif plan == "year":
        exp = created + 365 * 24 * 3600
    elif plan == "forever":
        exp = None
    else:
        raise ValueError("Unknown plan")

    DB.execute(
        "INSERT INTO subscriptions(user_id, plan, created_at, expires_at, gifted_by) VALUES(?,?,?,?,?)",
        (user_id, plan, created, exp, gifted_by)
    )
    DB.commit()

# --- channels helpers (НОВОЕ) ---
def channels_all_admin():
    cur = DB.execute("""
        SELECT c.chat_id, c.title, c.username, c.owner_id,
               COALESCE(u.username,'') AS owner_username
        FROM channels c
        LEFT JOIN users u ON u.user_id = c.owner_id
        ORDER BY c.added_at DESC
    """)
    return cur.fetchall()

def channels_by_owner(owner_id: int):
    cur = DB.execute("""
        SELECT chat_id, COALESCE(title,''), COALESCE(username,'')
        FROM channels
        WHERE owner_id=?
        ORDER BY added_at DESC
    """, (owner_id,))
    return cur.fetchall()

def channel_add_owned(owner_id: int, chat_id: int, title: str | None, username: str | None):
    DB.execute("""
        INSERT OR REPLACE INTO channels(chat_id, title, added_at, owner_id, username)
        VALUES(?,?,?,?,?)
    """, (chat_id, title or "", now_ts(), owner_id, (username or "")))
    DB.commit()

def channel_remove(chat_id: int):
    DB.execute("DELETE FROM channels WHERE chat_id=?", (chat_id,))
    DB.commit()

def admin_username_norm() -> str:
    u = getattr(config, "ADMIN_USERNAME", "") or ""
    return u.lstrip("@").lower()

def is_admin(user_id: int, username: str | None) -> bool:
    if user_id == getattr(config, "ADMIN_ID", 0):
        return True
    if username:
        return username.lower() == admin_username_norm()
    return False

def is_channel_allowed(chat_id: int) -> bool:
    # теперь работаем ТОЛЬКО в привязанных каналах
    cur = DB.execute("SELECT 1 FROM channels WHERE chat_id=? LIMIT 1", (chat_id,))
    return cur.fetchone() is not None

# ======================== ТЕКСТЫ/КНОПКИ ЛИЧКИ =========================

HOWTO = (
    "<b>Как подключить бота к Telegram Business</b>\n\n"
    "1) Настройки → Бизнес → Чат-боты → Подключить бота.\n"
    "2) Выбери этого бота и дай разрешение управлять сообщениями.\n\n"
    "Формат кнопки: /button Название \"https://example.com\" или \"tg://settings\".\n"
    "Можно несколько кнопок в одном сообщении."
)

def kb_private(user_id: int | None = None, username: str | None = None) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Как подключить")],
        [KeyboardButton(text="Планы и оплата")],
    ]
    # «Создать кнопку» — только подписчики или админ
    if user_id and (has_active_subscription(user_id) or is_admin(user_id, username)):
        rows.insert(1, [KeyboardButton(text="Создать кнопку")])
        rows.append([KeyboardButton(text="Привязать канал")])
        rows.append([KeyboardButton(text="Мои каналы")])
    if user_id and is_admin(user_id, username):
        rows.append([KeyboardButton(text="Админ панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Привязать канал (инструкция ниже)", callback_data="admin:bindinfo")],
        [InlineKeyboardButton(text="📋 Каналы (все)", callback_data="admin:listch")],
        [InlineKeyboardButton(text="🗑 Отвязать канал (по ID)", callback_data="admin:unbindask")],
        [InlineKeyboardButton(text="🎁 Выдать подписку", callback_data="admin:grant")],
        [InlineKeyboardButton(text="📣 Рассылка", callback_data="admin:broadcast")],
        [InlineKeyboardButton(text="🧮 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="🧩 Сделать кнопку (мастер)", callback_data="admin:makebtn")],
    ])

def kb_plans_inline() -> InlineKeyboardMarkup:
    p = config.PRICES_STARS
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Неделя — {p['week']}⭐", callback_data="buy:week"),
         InlineKeyboardButton(text="🎁 Подарить", callback_data="gift:week")],
        [InlineKeyboardButton(text=f"💳 Месяц — {p['month']}⭐", callback_data="buy:month"),
         InlineKeyboardButton(text="🎁 Подарить", callback_data="gift:month")],
        [InlineKeyboardButton(text=f"💳 Год — {p['year']}⭐", callback_data="buy:year"),
         InlineKeyboardButton(text="🎁 Подарить", callback_data="gift:year")],
        [InlineKeyboardButton(text=f"💳 Навсегда — {p['forever']}⭐", callback_data="buy:forever"),
         InlineKeyboardButton(text="🎁 Подарить", callback_data="gift:forever")],
    ])

# ======================== СОСТОЯНИЯ =========================

class CreateBtn(StatesGroup):
    text = State()
    label = State()
    url = State()

class AdminBind(StatesGroup):
    wait = State()      # больше не используется для форварда, но оставим, если надо

class AdminUnbind(StatesGroup):
    wait = State()

class AdminBroadcast(StatesGroup):
    text = State()

class AdminGrant(StatesGroup):
    user = State()
    plan = State()

class GiftBuy(StatesGroup):
    plan = State()
    target = State()

class ChannelLink(StatesGroup):
    wait_forward = State()

# ======================== УТИЛИТЫ КНОПОК/ПАРСИНГ =========================

QUOTE_OPEN = ['"', '«', '“']
ALLOWED_SCHEMES = {"http", "https", "tg"}
MAX_BTNS = 8

def is_allowed_url(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ALLOWED_SCHEMES:
        return False
    if p.scheme in {"http", "https"}:
        return bool(p.netloc)
    if p.scheme == "tg":
        return True
    return False

def _find_next(text_lower: str, start: int, triggers_lower: list[str]):
    nxt, tlen = None, 0
    for t in triggers_lower:
        j = text_lower.find(t, start)
        if j != -1 and (nxt is None or j < nxt):
            nxt, tlen = j, len(t)
    return nxt, tlen

def parse_buttons_and_clean(text: str, triggers: list[str]):
    text_lower = text.lower()
    triggers_lower = [t.lower() for t in triggers]

    i = 0
    buttons, spans = [], []

    while True:
        idx, tlen = _find_next(text_lower, i, triggers_lower)
        if idx is None:
            break

        j = idx + tlen
        while j < len(text) and text[j].isspace():
            j += 1

        quote_pos, quote_char = None, None
        k = j
        while k < len(text):
            ch = text[k]
            if ch in QUOTE_OPEN:
                quote_pos, quote_char = k, ch
                break
            k += 1
        if quote_pos is None:
            i = j
            continue

        label = text[j:quote_pos].strip()
        if not label:
            i = quote_pos + 1
            continue

        close_char = {'«': '»', '“': '”'}.get(quote_char, '"')
        url_start = quote_pos + 1
        url_end = text.find(close_char, url_start)
        if url_end == -1:
            i = url_start
            continue

        url = text[url_start:url_end].strip()
        if not is_allowed_url(url):
            i = url_end + 1
            continue

        buttons.append((label, url))
        spans.append((idx, url_end + 1))
        i = url_end + 1

        if len(buttons) >= MAX_BTNS:
            break

    if not buttons:
        return text, []

    out, last = [], 0
    for s, e in sorted(spans):
        out.append(text[last:s])
        last = e
    out.append(text[last:])
    clean = " ".join("".join(out).split()) or " "
    return clean, buttons

def build_kb_from_pairs(buttons: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=label, url=url)] for label, url in buttons]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ======================== ПЛАТЁЖИ/ПОДПИСКИ (Stars) =========================

PLANS = ("week", "month", "year", "forever")

def normalize_plan(s: str) -> str | None:
    s = (s or "").strip().lower()
    aliases = {
        "w": "week", "неделя": "week",
        "m": "month", "месяц": "month",
        "y": "year", "год": "year",
        "f": "forever", "навсегда": "forever",
    }
    if s in PLANS:
        return s
    return aliases.get(s)

def plan_human(plan: str) -> str:
    return {
        "week": "1 неделя",
        "month": "1 месяц",
        "year": "1 год",
        "forever": "Навсегда",
    }[plan]

def calc_price_stars(plan: str, *, is_gift: bool, buyer_has_sub: bool) -> int:
    base = int(config.PRICES_STARS[plan])
    if is_gift and buyer_has_sub:
        return max(1, round(base * (100 - int(config.GIFT_DISCOUNT_PCT)) / 100))
    return base

def make_invoice_payload(data: dict) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

def parse_invoice_payload(payload: str) -> dict:
    try:
        return json.loads(payload)
    except Exception:
        return {}

async def send_subscription_invoice(m: Message, plan: str, *, gift_to_user_id: int | None = None, gift_to_username: str | None = None):
    buyer_id = m.from_user.id
    buyer_has = has_active_subscription(buyer_id)
    price = calc_price_stars(plan, is_gift=(gift_to_user_id is not None or gift_to_username is not None), buyer_has_sub=buyer_has)

    title = f"Подписка: {plan_human(plan)}"
    if gift_to_user_id or gift_to_username:
        title += " (подарок)"

    desc_lines = [f"Доступ к функциям бота для бизнес-сообщений и каналов.", f"Срок: {plan_human(plan)}."]
    if gift_to_user_id or gift_to_username:
        desc_lines.append("Это подарочная подписка.")
        if buyer_has:
            desc_lines.append(f"Применена скидка -{config.GIFT_DISCOUNT_PCT}%.")
        else:
            desc_lines.append("У дарителя нет активной подписки — скидка не применяется.")
    description = "\n".join(desc_lines)

    payload = make_invoice_payload({
        "kind": "subscription",
        "type": "gift" if (gift_to_user_id or gift_to_username) else "self",
        "plan": plan,
        "gift_to_user_id": gift_to_user_id,
        "gift_to_username": gift_to_username,
    })

    prices = [LabeledPrice(label=f"{plan_human(plan)}", amount=price)]  # XTR

    await m.bot.send_invoice(
        chat_id=m.chat.id,
        title=title,
        description=description,
        payload=payload,
        provider_token="",                 # Stars: пустая строка
        currency="XTR",                    # Stars
        prices=prices,
        need_name=False,
        need_email=False,
        is_flexible=False,
        start_parameter=f"{plan}-stars"
    )

# ======================== ХЕЛПЕР: ОТПРАВКА/РЕДАКТИРОВАНИЕ С УЧЁТОМ МЕДИА =========================

async def edit_or_send_with_media(m: Message, clean_text: str, buttons: list[tuple[str, str]]):
    kb = build_kb_from_pairs(buttons)
    has_photo = bool(m.photo)
    has_video = bool(m.video)

    # бизнес-сообщения
    if m.business_connection_id:
        if has_photo or has_video:
            await m.bot.edit_message_caption(
                business_connection_id=m.business_connection_id,
                chat_id=m.chat.id,
                message_id=m.message_id,
                caption=clean_text,
                reply_markup=kb
            )
        else:
            await m.bot.edit_message_text(
                business_connection_id=m.business_connection_id,
                chat_id=m.chat.id,
                message_id=m.message_id,
                text=clean_text,
                reply_markup=kb
            )
        return

    # каналы/чаты
    try:
        await m.bot.delete_message(m.chat.id, m.message_id)
    except Exception:
        pass

    if has_photo:
        await m.bot.send_photo(
            chat_id=m.chat.id,
            photo=m.photo[-1].file_id,
            caption=clean_text,
            reply_markup=kb
        )
    elif has_video:
        vid = getattr(m.video, "file_id", None) or (m.video[-1].file_id if isinstance(m.video, list) else None)
        await m.bot.send_video(
            chat_id=m.chat.id,
            video=vid,
            caption=clean_text,
            reply_markup=kb
        )
    else:
        await m.bot.send_message(chat_id=m.chat.id, text=clean_text, reply_markup=kb)

# ======================== ЛИЧКА: БАЗОВОЕ =========================

@router.message(CommandStart(), (F.chat.type == ChatType.PRIVATE))
async def start_private(m: Message):
    ensure_user(m.from_user.id, m.from_user.username)
    await m.answer(
        "Привет! Я помогу подключить бота к Telegram Business.\n"
        "Чтобы пользоваться в бизнес-чатах и каналах — нужна подписка.\n"
        "Команды: /plans, /buy, /gift, /status, /howto, /admin\n",
        reply_markup=kb_private(m.from_user.id, m.from_user.username)
    )

@router.message(Command("howto"), (F.chat.type == ChatType.PRIVATE))
@router.message(F.text.lower() == "как подключить", (F.chat.type == ChatType.PRIVATE))
async def howto_private(m: Message):
    await m.answer(HOWTO, reply_markup=kb_private(m.from_user.id, m.from_user.username))

# ======================== ЛИЧКА: ПЛАНЫ/СТАТУС/ПОКУПКА/ПОДАРОК =========================

@router.message(F.text.lower() == "планы и оплата", (F.chat.type == ChatType.PRIVATE))
@router.message(Command("plans"), (F.chat.type == ChatType.PRIVATE))
async def plans_cmd(m: Message):
    prices = config.PRICES_STARS
    lines = [
        "<b>Подписки (Telegram Stars)</b>",
        f"• Неделя — {prices['week']}⭐",
        f"• Месяц — {prices['month']}⭐",
        f"• Год — {prices['year']}⭐",
        f"• Навсегда — {prices['forever']}⭐",
        "",
        "Нажми кнопку ниже, чтобы купить или подарить.",
        "Скидка на подарок −25% работает только если у дарителя уже есть активная подписка.",
    ]
    await m.answer("\n".join(lines), reply_markup=kb_plans_inline())

@router.message(Command("status"), (F.chat.type == ChatType.PRIVATE))
async def status_cmd(m: Message):
    ensure_user(m.from_user.id, m.from_user.username)
    active = has_active_subscription(m.from_user.id)
    if not active:
        await m.answer("У тебя нет активной подписки. /plans — посмотреть тарифы.",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return

    cur = DB.execute("""
        SELECT plan, expires_at, gifted_by, created_at
        FROM subscriptions
        WHERE user_id=?
        ORDER BY COALESCE(expires_at, 1<<62) DESC, id DESC
        LIMIT 1
    """, (m.from_user.id,))
    row = cur.fetchone()
    if not row:
        await m.answer("У тебя нет активной подписки. /plans",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return

    plan, expires_at, gifted_by, created_at = row
    if expires_at is None:
        exp_str = "никогда (навсегда)"
    else:
        dt = datetime.fromtimestamp(expires_at, tz=timezone.utc)
        exp_str = dt.strftime("%Y-%m-%d %H:%M UTC")

    s = [
        f"<b>Активная подписка</b>: {plan_human(plan)}",
        f"Действует до: {exp_str}",
    ]
    if gifted_by:
        s.append(f"Получена в подарок (от ID {gifted_by}).")
    await m.answer("\n".join(s), reply_markup=kb_private(m.from_user.id, m.from_user.username))

@router.message(Command("buy"), (F.chat.type == ChatType.PRIVATE))
async def buy_cmd(m: Message):
    ensure_user(m.from_user.id, m.from_user.username)
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Формат: /buy <week|month|year|forever>",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    plan = normalize_plan(parts[1])
    if plan is None:
        await m.answer("Не понял план. Используй: week, month, year, forever.",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    await send_subscription_invoice(m, plan)

@router.message(Command("gift"), (F.chat.type == ChatType.PRIVATE))
async def gift_cmd(m: Message):
    ensure_user(m.from_user.id, m.from_user.username)
    if not has_active_subscription(m.from_user.id):
        await m.answer("Дарить можно только если у тебя уже есть активная подписка. Сначала оформи /buy.",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    parts = (m.text or "").split()
    if len(parts) < 2:
        await m.answer("Формат: в ответ на сообщение получателя — /gift <plan>\nили /gift <plan> @username",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    plan = normalize_plan(parts[1])
    if plan is None:
        await m.answer("Не понял план. Используй: week, month, year, forever.",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return

    gift_to_user_id = None
    gift_to_username = None

    if m.reply_to_message and m.reply_to_message.from_user:
        gift_to_user_id = m.reply_to_message.from_user.id
        ensure_user(gift_to_user_id, m.reply_to_message.from_user.username)
    else:
        if len(parts) >= 3 and parts[2].startswith("@"):
            gift_to_username = parts[2][1:]
        else:
            await m.answer("Укажи получателя: ответь на его сообщение или добавь @username.",
                           reply_markup=kb_private(m.from_user.id, m.from_user.username))
            return

    await send_subscription_invoice(m, plan, gift_to_user_id=gift_to_user_id, gift_to_username=gift_to_username)

@router.callback_query(F.data.startswith("buy:"))
async def cb_buy(cq: CallbackQuery):
    if cq.message.chat.type != ChatType.PRIVATE:
        await cq.answer("Открой меня в личке, там оформим покупку.", show_alert=True)
        return
    plan = normalize_plan(cq.data.split(":", 1)[1])
    if not plan:
        await cq.answer("Неизвестный тариф", show_alert=True)
        return
    await send_subscription_invoice(cq.message, plan)
    await cq.answer()

@router.callback_query(F.data.startswith("gift:"))
async def cb_gift(cq: CallbackQuery, state: FSMContext):
    if cq.message.chat.type != ChatType.PRIVATE:
        await cq.answer("Открой меня в личке, там оформим подарок.", show_alert=True)
        return
    if not has_active_subscription(cq.from_user.id):
        await cq.answer("Сначала оформи свою подписку — тогда будет скидка −25% на подарок.", show_alert=True)
        return
    plan = normalize_plan(cq.data.split(":", 1)[1])
    if not plan:
        await cq.answer("Неизвестный тариф", show_alert=True)
        return
    await state.set_state(GiftBuy.target)
    await state.update_data(plan=plan)
    await cq.message.answer(
        "Кому подарить? Ответь на сообщение получателя ИЛИ пришли @username.\n"
        "После этого выставлю счёт со скидкой −25%."
    )
    await cq.answer()

@router.message(GiftBuy.target, (F.chat.type == ChatType.PRIVATE))
async def gift_target_step(m: Message, state: FSMContext):
    data = await state.get_data()
    plan = data.get("plan")

    gift_to_user_id = None
    gift_to_username = None

    if m.reply_to_message and m.reply_to_message.from_user:
        gift_to_user_id = m.reply_to_message.from_user.id
        ensure_user(gift_to_user_id, m.reply_to_message.from_user.username)
    else:
        t = (m.text or "").strip()
        if t.startswith("@"):
            gift_to_username = t[1:]
        else:
            await m.answer("Укажи получателя: ответь на его сообщение или пришли @username.")
            return

    await send_subscription_invoice(
        m, plan,
        gift_to_user_id=gift_to_user_id,
        gift_to_username=gift_to_username
    )
    await state.clear()

# ======================== МАСТЕР "СОЗДАТЬ КНОПКУ" (личка) =========================

@router.message((F.chat.type == ChatType.PRIVATE) & (F.text == "Создать кнопку"))
async def create_btn_start(m: Message, state: FSMContext):
    if not (has_active_subscription(m.from_user.id) or is_admin(m.from_user.id, m.from_user.username)):
        await m.answer("Эта функция доступна по подписке. Оформи /plans и возвращайся 🙌",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    await state.set_state(CreateBtn.text)
    await m.answer(
        "Ок! Отправь текст сообщения, который я опубликую с кнопкой.\n\n"
        "Можно использовать HTML (<b>жирный</b>, <i>курсив</i> и т.д.).\n\n"
        "Для отмены — /cancel",
        reply_markup=kb_private(m.from_user.id, m.from_user.username)
    )

@router.message(Command("cancel"), (F.chat.type == ChatType.PRIVATE))
async def create_btn_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Отменил. Что дальше?", reply_markup=kb_private(m.from_user.id, m.from_user.username))

@router.message(CreateBtn.text, (F.chat.type == ChatType.PRIVATE))
async def create_btn_got_text(m: Message, state: FSMContext):
    await state.update_data(text=m.html_text or (m.text or ""))
    await state.set_state(CreateBtn.label)
    await m.answer("Теперь отправь название кнопки (надпись на кнопке).")

@router.message(CreateBtn.label, (F.chat.type == ChatType.PRIVATE))
async def create_btn_got_label(m: Message, state: FSMContext):
    label = (m.text or "").strip()
    if not label:
        await m.answer("Название пустое. Пришли текст для названия кнопки.")
        return
    await state.update_data(label=label)
    await state.set_state(CreateBtn.url)
    await m.answer("И пришли ссылку. Допустимые схемы: http/https/tg (например, https://example.com или tg://settings).")

@router.message(CreateBtn.url, (F.chat.type == ChatType.PRIVATE))
async def create_btn_got_url(m: Message, state: FSMContext):
    url = (m.text or "").strip().strip("“”«»\"'")
    if not is_allowed_url(url):
        await m.answer(
            "Ссылка некорректна. Допустимые схемы: http, https, tg.\n"
            "Пример: https://example.com или tg://settings\n"
            "Пришли ссылку ещё раз или /cancel."
        )
        return
    data = await state.get_data()
    text = (data.get("text") or "").strip() or " "
    label = data.get("label") or "Открыть"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=label, url=url)]])
    await m.answer(text, reply_markup=kb)
    await state.clear()
    await m.answer("Готово! Хочешь ещё одну? Нажми «Создать кнопку».",
                   reply_markup=kb_private(m.from_user.id, m.from_user.username))

# ======================== PAYMENTS CALLBACKS =========================

@router.pre_checkout_query()
async def on_pre_checkout(q: PreCheckoutQuery, bot: Bot):
    await bot.answer_pre_checkout_query(q.id, ok=True)

@router.message(F.successful_payment)
async def on_success_payment(m: Message):
    sp = m.successful_payment
    data = parse_invoice_payload(sp.invoice_payload)
    kind = data.get("kind")
    if kind != "subscription":
        await m.answer("Платёж получен, но тип не распознан. Напиши /support.")
        return

    plan = data.get("plan")
    if plan not in PLANS:
        await m.answer("Платёж получен, но план не распознан. Напиши /support.")
        return

    buyer_id = m.from_user.id
    ensure_user(buyer_id, m.from_user.username)

    if data.get("type") == "gift":
        to_uid = data.get("gift_to_user_id")
        to_un = data.get("gift_to_username")
        if to_uid:
            ensure_user(to_uid, None)
            grant_subscription(to_uid, plan, gifted_by=buyer_id)
            await m.answer(f"Подарочная подписка «{plan_human(plan)}» активирована для ID {to_uid}.")
            try:
                await m.bot.send_message(to_uid, f"Тебе подарили подписку: {plan_human(plan)} 🎁")
            except Exception:
                pass
        else:
            grant_subscription(buyer_id, plan, gifted_by=buyer_id)  # временно у дарителя
            await m.answer(
                "Оплата прошла. Я временно привязал подписку к тебе. "
                "Как только получатель напишет боту, перешлю — пришли команду /activategift @username"
            )
    else:
        grant_subscription(buyer_id, plan)
        await m.answer(f"Подписка активирована: {plan_human(plan)} ✅")

@router.message(Command("activategift"), (F.chat.type == ChatType.PRIVATE))
async def activate_gift(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].startswith("@"):
        await m.answer("Формат: /activategift @username",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    username = parts[1][1:]

    cur = DB.execute("SELECT user_id FROM users WHERE lower(username)=lower(?)", (username.lower(),))
    row = cur.fetchone()
    if not row:
        await m.answer("Этот пользователь ещё не писал боту. Попроси его нажать /start.",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    target_id = int(row[0])

    cur2 = DB.execute("""
        SELECT id, plan, created_at, expires_at FROM subscriptions
        WHERE user_id=? ORDER BY id DESC LIMIT 1
    """, (m.from_user.id,))
    last = cur2.fetchone()
    if not last:
        await m.answer("У тебя нет подписки для переноса.",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    sid, plan, created_at, expires_at = last

    DB.execute("DELETE FROM subscriptions WHERE id=?", (sid,))
    DB.commit()

    DB.execute(
        "INSERT INTO subscriptions(user_id, plan, created_at, expires_at, gifted_by) VALUES(?,?,?,?,?)",
        (target_id, plan, created_at, expires_at, m.from_user.id)
    )
    DB.commit()

    await m.answer(f"Подарок активирован для @{username} ({plan_human(plan)}).")
    try:
        await m.bot.send_message(target_id, f"Тебе активировали подарок: {plan_human(plan)} 🎁")
    except Exception:
        pass

# ======================== УПРАВЛЕНИЕ КАНАЛАМИ (ПОЛЬЗОВАТЕЛЬ) =========================

@router.message((F.chat.type == ChatType.PRIVATE) & (F.text == "Привязать канал"))
async def user_link_channel(m: Message, state: FSMContext):
    if not (has_active_subscription(m.from_user.id) or is_admin(m.from_user.id, m.from_user.username)):
        await m.answer("Привязка канала доступна только по подписке.",
                       reply_markup=kb_private(m.from_user.id, m.from_user.username))
        return
    await m.answer("Перешли сюда любое сообщение из канала, который хочешь привязать.\n"
                   "Ты должен быть владельцем (creator) канала, а бот — админом канала.")
    await state.set_state(ChannelLink.wait_forward)

@router.message(ChannelLink.wait_forward, (F.chat.type == ChatType.PRIVATE))
async def user_link_channel_step(m: Message, state: FSMContext):
    ch = getattr(m, "forward_from_chat", None)
    if not ch or ch.type != ChatType.CHANNEL:
        await m.answer("Это не пересланное сообщение из канала. Попробуй снова.\n"
                       "Перешли ЛЮБОЕ сообщение из нужного канала.")
        return

    chat_id = ch.id
    # проверка прав бота в канале
    try:
        me_member = await m.bot.get_chat_member(chat_id, (await m.bot.get_me()).id)
        if me_member.status not in ("administrator", "creator"):
            await m.answer("Бот должен быть администратором канала. Добавь его админом и повтори.")
            return
    except Exception:
        await m.answer("Не удалось проверить права бота. Убедись, что бот добавлен в канал как админ.")
        return

    # проверка, что пользователь — владелец (creator)
    try:
        admins = await m.bot.get_chat_administrators(chat_id)
        creator = next((a for a in admins if a.status == "creator"), None)
        if not creator or creator.user.id != m.from_user.id:
            await m.answer("Ты не являешься владельцем (creator) этого канала.")
            return
    except Exception:
        await m.answer("Не удалось получить администраторов канала.")
        return

    channel_add_owned(m.from_user.id, chat_id, ch.title, ch.username)
    await state.clear()
    await m.answer(f"Канал <b>{ch.title}</b> привязан ✅",
                   reply_markup=kb_private(m.from_user.id, m.from_user.username))

@router.message((F.chat.type == ChatType.PRIVATE) & (F.text == "Мои каналы"))
async def my_channels_list(m: Message):
    rows = channels_by_owner(m.from_user.id)
    if not rows:
        await m.answer("У тебя нет привязанных каналов.")
        return

    kb = []
    text_lines = ["<b>Твои каналы:</b>"]
    for chat_id, title, uname in rows:
        uname_t = f"@{uname}" if uname else "—"
        text_lines.append(f"• {title} ({uname_t}) — <code>{chat_id}</code>")
        kb.append([InlineKeyboardButton(text=f"Отвязать «{title}»", callback_data=f"unlink:{chat_id}")])
    await m.answer("\n".join(text_lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("unlink:"))
async def unlink_channel_cb(cq: CallbackQuery):
    chat_id = int(cq.data.split(":", 1)[1])

    # если не админ — можно отвязать только свой канал
    if not is_admin(cq.from_user.id, cq.from_user.username):
        cur = DB.execute("SELECT owner_id FROM channels WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        if not row or int(row[0]) != cq.from_user.id:
            await cq.answer("Ты не можешь отвязать этот канал.", show_alert=True)
            return

    # выйти из канала и удалить запись
    try:
        await cq.bot.leave_chat(chat_id)
    except Exception:
        pass
    channel_remove(chat_id)
    await cq.answer("Канал отвязан.", show_alert=True)
    try:
        await cq.message.delete()
    except Exception:
        pass

# ======================== АДМИН-ПАНЕЛЬ =========================

@router.message((F.chat.type == ChatType.PRIVATE) & (F.text.lower() == "админ панель"))
@router.message(Command("admin"), (F.chat.type == ChatType.PRIVATE))
async def admin_panel(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, m.from_user.username):
        return
    await state.clear()
    await m.answer("Админ панель:", reply_markup=kb_private(m.from_user.id, m.from_user.username))
    await m.answer("Выбери действие:", reply_markup=kb_admin())

def _parse_chat_ref(text: str) -> tuple[int | None, str | None]:
    t = (text or "").strip()
    if not t:
        return None, None
    if t.startswith("@"):
        return None, t[1:].lower()
    try:
        cid = int(t)
        return cid, None
    except Exception:
        return None, None

@router.callback_query(F.data.startswith("admin:"))
async def admin_callbacks(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id, cq.from_user.username):
        await cq.answer("Только для админа", show_alert=True)
        return

    action = cq.data.split(":", 1)[1]

    if action == "bindinfo":
        await cq.message.answer(
            "🔗 <b>Как привязать канал (для пользователя):</b>\n"
            "1) Добавь бота админом в своём канале.\n"
            "2) В личке нажми «Привязать канал» и перешли сюда любое сообщение из канала.\n"
            "3) Канал появится в «Мои каналы».",
        )
        await cq.answer()
    elif action == "listch":
        rows = channels_all_admin()
        if not rows:
            await cq.message.answer("Нет привязанных каналов.")
            await cq.answer()
            return
        out = ["<b>Все каналы:</b>"]
        kb = []
        for chat_id, title, uname, owner_id, owner_username in rows:
            owner_tag = f"@{owner_username}" if owner_username else owner_id
            uname_t = f"@{uname}" if uname else "—"
            out.append(f"• {title} ({uname_t}) — владелец {owner_tag} — <code>{chat_id}</code>")
            kb.append([InlineKeyboardButton(text=f"Отвязать «{title}»", callback_data=f"unlink:{chat_id}")])
        await cq.message.answer("\n".join(out), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        await cq.answer()
    elif action == "unbindask":
        await cq.message.answer("Пришли -100id канала или @username для отвязки.")
        await state.set_state(AdminUnbind.wait)
        await cq.answer()
    elif action == "grant":
        await state.set_state(AdminGrant.user)
        await cq.message.answer("Выдача подписки: ответь на сообщение пользователя или пришли @username/ID.")
        await cq.answer()
    elif action == "broadcast":
        await state.set_state(AdminBroadcast.text)
        await cq.message.answer("Текст рассылки? (HTML разрешён). Отправь сообщением.")
        await cq.answer()
    elif action == "stats":
        cur = DB.execute("SELECT COUNT(*) FROM users")
        users = cur.fetchone()[0]
        cur = DB.execute("""
            SELECT COUNT(DISTINCT user_id)
            FROM subscriptions
            WHERE expires_at IS NULL OR expires_at > ?
        """, (now_ts(),))
        active = cur.fetchone()[0]
        await cq.message.answer(f"Пользователей: {users}\nАктивных подписок: {active}")
        await cq.answer()
    elif action == "makebtn":
        await state.set_state(CreateBtn.text)
        await cq.message.answer(
            "Ок! Отправь текст сообщения, который я опубликую с кнопкой.\n"
            "Для отмены — /cancel"
        )
        await cq.answer()

@router.message(AdminUnbind.wait, (F.chat.type == ChatType.PRIVATE))
async def admin_unbind_receive(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, m.from_user.username):
        return
    cid, uname = _parse_chat_ref(m.text or "")
    target_chat_id = None
    if uname:
        try:
            chat = await m.bot.get_chat("@"+uname)
            target_chat_id = chat.id
        except Exception:
            await m.answer("Не нашёл канал по @username.")
            return
    elif cid:
        target_chat_id = cid
    else:
        await m.answer("Пришли @username или -100id.")
        return

    try:
        await m.bot.leave_chat(target_chat_id)
    except Exception:
        pass
    channel_remove(target_chat_id)
    await state.clear()
    await m.answer(f"Канал <code>{target_chat_id}</code> отвязан.")

@router.message(AdminBroadcast.text, (F.chat.type == ChatType.PRIVATE))
async def admin_broadcast_send(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, m.from_user.username):
        return
    text = m.html_text or (m.text or "")
    cur = DB.execute("SELECT user_id FROM users")
    ids = [int(r[0]) for r in cur.fetchall()]
    ok, fail = 0, 0
    for uid in ids:
        try:
            await m.bot.send_message(uid, text)
            ok += 1
        except Exception:
            fail += 1
    await state.clear()
    await m.answer(f"Рассылка завершена. Успехов: {ok}, ошибок: {fail}.")

@router.message(AdminGrant.user, (F.chat.type == ChatType.PRIVATE))
async def admin_grant_user(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, m.from_user.username):
        return

    target_id = None
    if m.reply_to_message and m.reply_to_message.from_user:
        target_id = m.reply_to_message.from_user.id
        ensure_user(target_id, m.reply_to_message.from_user.username)
    else:
        t = (m.text or "").strip()
        if t.startswith("@"):
            uname = t[1:]
            cur = DB.execute("SELECT user_id FROM users WHERE lower(username)=lower(?)", (uname.lower(),))
            row = cur.fetchone()
            if row:
                target_id = int(row[0])
        elif t.isdigit():
            target_id = int(t)

    if not target_id:
        await m.answer("Пришли пользователя: reply, @username (если уже писал боту) или numeric user_id.")
        return

    await state.update_data(target_id=target_id)
    await state.set_state(AdminGrant.plan)
    await m.answer("Какой план выдать? (week|month|year|forever)")

@router.message(AdminGrant.plan, (F.chat.type == ChatType.PRIVATE))
async def admin_grant_plan(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id, m.from_user.username):
        return
    plan = normalize_plan((m.text or "").strip().lower())
    if plan not in PLANS:
        await m.answer("Не понял план. Используй: week, month, year, forever.")
        return
    data = await state.get_data()
    target_id = int(data["target_id"])
    grant_subscription(target_id, plan, gifted_by=m.from_user.id)
    await state.clear()
    await m.answer(f"Выдал подписку {plan_human(plan)} пользователю <code>{target_id}</code> ✅")
    try:
        await m.bot.send_message(target_id, f"Вам выдана подписка: {plan_human(plan)} ✅")
    except Exception:
        pass

# ======================== БИЗНЕС/КАНАЛЫ (с учётом подписки) =========================

@router.business_message(F.text | F.caption)
async def business_handler(m: Message):
    if not (m.from_user and has_active_subscription(m.from_user.id)):
        try:
            await m.bot.send_message(
                m.from_user.id,
                "Чтобы пользоваться функциями в бизнес-сообщениях, нужна активная подписка. /plans"
            )
        except Exception:
            pass
        return

    triggers = ["/button"]
    if BOT_UN:
        triggers.append(f"@{BOT_UN}")
    text = m.text or m.caption or ""
    if not any(t.lower() in text.lower() for t in triggers):
        return
    clean_text, buttons = parse_buttons_and_clean(text, triggers)
    if not buttons:
        return
    await edit_or_send_with_media(m, clean_text, buttons)

@router.channel_post(F.text | F.caption)
async def channel_handler(m: Message):
    if not is_channel_allowed(m.chat.id):
        return

    triggers = ["/button"]
    if BOT_UN:
        triggers.append(f"@{BOT_UN}")
    text = m.text or m.caption or ""
    if not any(t.lower() in text.lower() for t in triggers):
        return
    clean_text, buttons = parse_buttons_and_clean(text, triggers)
    if not buttons:
        return
    await edit_or_send_with_media(m, clean_text, buttons)

# ======================== АВТО-ОБНОВЛЕНИЕ ИЗ GIT =========================

def _git(cmd: list[str]) -> str:
    out = subprocess.check_output(cmd, cwd=os.getcwd())
    return out.decode("utf-8", "ignore").strip()

def _has_git_repo() -> bool:
    return os.path.isdir(os.path.join(os.getcwd(), ".git"))

async def git_autoupdate_loop():
    if not getattr(config, "AUTO_UPDATE_ENABLED", False):
        return
    interval = getattr(config, "AUTO_UPDATE_INTERVAL_MIN", 10)
    remote = getattr(config, "GIT_REMOTE", "origin")
    branch = getattr(config, "GIT_BRANCH", "main")
    while True:
        try:
            if _has_git_repo():
                _git(["git", "fetch", remote, branch])
                local = _git(["git", "rev-parse", "HEAD"])
                remote_head = _git(["git", "rev-parse", f"{remote}/{branch}"])
                if local != remote_head:
                    _git(["git", "pull", "--ff-only", remote, branch])
                    os._exit(0)  # systemd перезапустит
        except Exception:
            pass
        await asyncio.sleep(max(1, int(interval)) * 60)

# ======================== ТОЧКА ВХОДА =========================

async def main():
    global BOT_UN
    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    me = await bot.get_me()
    BOT_UN = (me.username or "").lower()

    dp = Dispatcher()
    dp.include_router(router)

    await bot.set_my_commands([
        BotCommand(command="start", description="Запуск"),
        BotCommand(command="howto", description="Как подключить к Business"),
        BotCommand(command="plans", description="Планы и оплата"),
        BotCommand(command="buy", description="Купить подписку"),
        BotCommand(command="gift", description="Подарить подписку (-25%)"),
        BotCommand(command="status", description="Статус подписки"),
        BotCommand(command="admin", description="Админ панель"),
    ])

    # авто-обновление из git
    asyncio.create_task(git_autoupdate_loop())

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
