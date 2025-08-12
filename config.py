# config.py

# === ОБЯЗАТЕЛЬНО ===
BOT_TOKEN = "8310535941:AAEK2boHsQuVOrtdng2l8KvhcT_G-YdIfj4"

# Админ — для прав доступа и админ-панели.
ADMIN_ID = 1403240690
ADMIN_USERNAME = "@w1nReach"

# Цены в Telegram Stars (XTR). Целые числа — 1 = 1 звезда.
PRICES_STARS = {
    "week": 15,
    "month": 50,
    "year": 500,
    "forever": 750,
}

# Скидка на ПОДАРОК (-25%) действует ТОЛЬКО если у дарителя уже есть активная подписка.
GIFT_DISCOUNT_PCT = 25

# Путь к SQLite базе
DB_PATH = "data/bot.sqlite3"
