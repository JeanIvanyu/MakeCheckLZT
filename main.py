import asyncio
import json
import logging
import html
from logging.handlers import RotatingFileHandler
import re
import secrets
import sqlite3
import string
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import requests
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "db.db"


DEFAULT_CONFIG = {
    "telegram": {
        "bot_token": "КОНФИГ_БОТА_ТЕЛЕГРАМ",
        "owner_id": 0,
        "owner_ids": [] # Указывать в формате списка, например: [12345678, 87654321], а если один - просто id без []
    },
    "lzt": {
        "forum_api_base": "https://prod-api.lolz.live",
        "market_api_base": "https://prod-api.lzt.market",
        "api_token": "API_ТОКЕН",
        "account_id": 0
    },
    "checks": {
        "code_length": 18,
        "currency": "RUB",
        "expire_hours": 72
    },
    "transfer": {
        "endpoint": "/balance/transfer",
        "amount_field": "amount",
        "user_id_field": "user_id",
        "currency_field": "currency",
        "comment_field": "comment",
        "comment_template": "Check payout {code}"
    },
    "polling": {
        "interval_sec": 8,
        "conversations_page": 1,
        "messages_page": 1,
        "conversations_pages": 1,
        "messages_pages": 1,
        "conversations_limit": 10,
        "only_with_check_in_last_message": True
    }
}


CHECK_REGEX = re.compile(r"Активировать чек\s+([A-Za-z0-9]+)", re.IGNORECASE)
INLINE_AMOUNT_REGEX = re.compile(r"^(\d+[.,]?\d*)$")
HTML_TAG_RE = re.compile(r"<[^>]+>")

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "bot.log"

_root = logging.getLogger()
_root.setLevel(logging.INFO)

_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

_stream_handler = logging.StreamHandler()
_stream_handler.setLevel(logging.INFO)
_stream_handler.setFormatter(_formatter)

_file_handler = RotatingFileHandler(str(LOG_FILE), maxBytes=2_000_000, backupCount=5, encoding="utf-8")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(_formatter)

_root.handlers.clear()
_root.addHandler(_stream_handler)
_root.addHandler(_file_handler)
logger = logging.getLogger("check-bot")

INLINE_CACHE_TTL_SEC = 30
INLINE_CACHE: Dict[Tuple[int, str], Tuple[str, float]] = {}
MISSING_TEXT_LOG_LIMIT = 10
_missing_text_logged = 0


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")
        raise SystemExit(
            "Заполните токены и id аккаунта в config.json, а затем запустите бота снова."
        )
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    logger.info("Конфиг загружен: owner_id=%s, account_id=%s",
        cfg.get("telegram", {}).get("owner_id"),
        cfg.get("lzt", {}).get("account_id"))
    return cfg


def get_owner_ids(cfg: Dict[str, Any]) -> List[int]:
    tg = cfg.get("telegram", {})
    owners: List[int] = []
    single = tg.get("owner_id")
    if isinstance(single, int) and single > 0:
        owners.append(single)
    many = tg.get("owner_ids", [])
    if isinstance(many, list):
        for item in many:
            if isinstance(item, int) and item > 0:
                owners.append(item)
    deduped: List[int] = []
    for oid in owners:
        if oid not in deduped:
            deduped.append(oid)
    return deduped


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            amount TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            activated_by_user_id INTEGER,
            activated_by_username TEXT,
            activated_at TEXT,
            transfer_status TEXT,
            transfer_response TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_messages (
            key TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS conversation_state (
            conv_id INTEGER PRIMARY KEY,
            last_seen_message_id INTEGER
        )
        """
    )
    conn.commit()
    conn.close()
    logger.info("DB initialized")


def db_execute(query: str, params: Tuple[Any, ...] = ()) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    conn.close()


def db_fetchone(query: str, params: Tuple[Any, ...] = ()) -> Optional[Tuple[Any, ...]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)
    row = cur.fetchone()
    conn.close()
    return row


def db_fetchall(query: str, params: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def get_last_seen_message_id(conv_id: int) -> Optional[int]:
    row = db_fetchone(
        "SELECT last_seen_message_id FROM conversation_state WHERE conv_id = ?",
        (conv_id,)
    )
    return row[0] if row and row[0] is not None else None


def set_last_seen_message_id(conv_id: int, msg_id: int) -> None:
    db_execute(
        """
        INSERT INTO conversation_state (conv_id, last_seen_message_id)
        VALUES (?, ?)
        ON CONFLICT(conv_id) DO UPDATE SET last_seen_message_id = excluded.last_seen_message_id
        """,
        (conv_id, msg_id)
    )


def generate_code(length: int) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def decimal_amount(text: str) -> Decimal:
    value = Decimal(text.replace(",", "."))
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def format_amount(amount: Decimal) -> str:
    return f"{amount:.2f}"


def create_check(amount: Decimal) -> str:
    code = generate_code(CONFIG["checks"]["code_length"])
    db_execute(
        "INSERT INTO checks (code, amount, status, created_at) VALUES (?, ?, ?, ?)",
        (code, format_amount(amount), "created", now_iso()),
    )
    return code


def mark_message_processed(key: str) -> None:
    db_execute(
        "INSERT OR IGNORE INTO processed_messages (key, processed_at) VALUES (?, ?)",
        (key, now_iso()),
    )


def message_already_processed(key: str) -> bool:
    row = db_fetchone("SELECT 1 FROM processed_messages WHERE key = ?", (key,))
    return row is not None


def forum_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "authorization": f"Bearer {CONFIG['lzt']['api_token']}",
        "user-agent": "check-bot/1.0"
    }


def market_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "authorization": f"Bearer {CONFIG['lzt']['api_token']}",
        "user-agent": "check-bot/1.0"
    }


def safe_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "conversations", "messages"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def extract_conversation_ids(payload: Any) -> List[int]:
    conversations = safe_list(payload)
    ids: List[int] = []
    for item in conversations:
        if not isinstance(item, dict):
            continue
        conv_id = item.get("id") or item.get("conversation_id") or item.get("conversationId")
        if isinstance(conv_id, int):
            ids.append(conv_id)
        elif isinstance(conv_id, str) and conv_id.isdigit():
            ids.append(int(conv_id))
    return ids


def extract_last_message_text(item: Dict[str, Any]) -> Optional[str]:
    last_msg = (
        item.get("last_message")
        or item.get("lastMessage")
        or item.get("last_message_text")
        or item.get("lastMessageText")
        or item.get("last_message_preview")
        or item.get("lastMessagePreview")
        or item.get("last_message_snippet")
        or item.get("lastMessageSnippet")
    )
    if isinstance(last_msg, dict):
        return (
            last_msg.get("message")
            or last_msg.get("text")
            or last_msg.get("content")
            or last_msg.get("message_body")
            or last_msg.get("message_body_plain_text")
            or last_msg.get("message_body_html")
            or last_msg.get("message_html")
            or last_msg.get("messageHtml")
            or last_msg.get("html")
            or last_msg.get("bbcode")
        )
    if isinstance(last_msg, str):
        return last_msg
    return None


def normalize_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = html.unescape(value)
    value = HTML_TAG_RE.sub(" ", value)
    return re.sub(r"\s+", " ", value).strip()


def extract_messages(payload: Any) -> List[Dict[str, Any]]:
    return safe_list(payload)


def extract_message_fields(msg: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    msg_id = msg.get("id") or msg.get("message_id") or msg.get("messageId")
    user_id = (
        msg.get("user_id")
        or msg.get("userId")
        or msg.get("creator_id")
        or msg.get("creator_user_id")
        or msg.get("sender_id")
    )
    username = msg.get("username") or msg.get("creator_username")
    text = (
        msg.get("message")
        or msg.get("text")
        or msg.get("content")
        or msg.get("message_body")
        or msg.get("message_body_plain_text")
        or msg.get("message_body_html")
    )
    if isinstance(text, dict):
        text = text.get("message") or text.get("text") or text.get("content")
    if not text:
        text = (
            msg.get("message_html")
            or msg.get("messageHtml")
            or msg.get("html")
            or msg.get("bbcode")
        )

    author = msg.get("author") or msg.get("creator") or msg.get("user")
    if isinstance(author, dict):
        user_id = user_id or author.get("user_id") or author.get("id")
        username = username or author.get("username")

    def to_int(value: Any) -> Optional[int]:
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    return to_int(msg_id), to_int(user_id), username, text


def forum_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{CONFIG['lzt']['forum_api_base'].rstrip('/')}{path}"
    try:
        response = requests.get(url, headers=forum_headers(), params=params, timeout=20)
        logger.debug("Forum GET %s -> %s", response.url, response.status_code)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        status = getattr(exc, "response", None)
        body = ""
        if status is not None:
            try:
                body = status.text[:1000]
            except Exception:
                body = ""
        logger.exception("Forum GET failed url=%s params=%s body=%s", url, params, body)
        raise


def market_post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{CONFIG['lzt']['market_api_base'].rstrip('/')}{path}"
    try:
        response = requests.post(url, headers=market_headers(), json=payload, timeout=20)
        logger.debug("Market POST %s -> %s", url, response.status_code)
        return response
    except Exception:
        logger.exception("Market POST failed url=%s payload=%s", url, payload)
        raise


def update_check_activation(code: str, user_id: int, username: Optional[str]) -> None:
    db_execute(
        """
        UPDATE checks
        SET status = ?, activated_by_user_id = ?, activated_by_username = ?, activated_at = ?
        WHERE code = ? AND status = 'created'
        """,
        ("activated", user_id, username, now_iso(), code),
    )


def update_check_transfer(code: str, status: str, response_text: str) -> None:
    db_execute(
        """
        UPDATE checks
        SET status = ?, transfer_status = ?, transfer_response = ?
        WHERE code = ?
        """,
        (status, status, response_text[:1000], code),
    )


def get_check_amount(code: str) -> Optional[Decimal]:
    row = db_fetchone("SELECT amount FROM checks WHERE code = ?", (code,))
    if not row:
        return None
    return Decimal(row[0])


def is_check_active(code: str) -> bool:
    row = db_fetchone("SELECT status FROM checks WHERE code = ?", (code,))
    return bool(row and row[0] == "created")


def transfer_funds(lzt_user_id: int, amount: Decimal, code: str) -> Tuple[bool, str]:
    transfer_cfg = CONFIG["transfer"]
    payload = {
        transfer_cfg["amount_field"]: format_amount(amount),
        transfer_cfg["user_id_field"]: lzt_user_id,
        transfer_cfg["currency_field"]: CONFIG["checks"]["currency"],
        transfer_cfg["comment_field"]: transfer_cfg["comment_template"].format(code=code),
    }
    logger.info("Transfer start user_id=%s amount=%s code=%s", lzt_user_id, format_amount(amount), code)
    response = market_post(transfer_cfg["endpoint"], payload)
    if response.ok:
        logger.info("Transfer ok code=%s status=%s", code, response.status_code)
        return True, response.text
    logger.warning("Transfer failed code=%s status=%s body=%s", code, response.status_code, response.text[:500])
    return False, response.text


def run_on_loop(loop: asyncio.AbstractEventLoop, coro: asyncio.Future) -> None:
    asyncio.run_coroutine_threadsafe(coro, loop)


def handle_activation_message(
    conv_id: int,
    msg_id: int,
    lzt_user_id: int,
    username: Optional[str],
    text: str,
    bot: Bot,
    owner_id: int,
    loop: asyncio.AbstractEventLoop
) -> None:
    key = f"{conv_id}:{msg_id}"
    if message_already_processed(key):
        return
    mark_message_processed(key)

    match = CHECK_REGEX.search(text or "")
    if not match:
        return

    code = match.group(1)
    logger.info("Activation message detected code=%s user_id=%s username=%s", code, lzt_user_id, username)
    if not is_check_active(code):
        return

    update_check_activation(code, lzt_user_id, username)
    amount = get_check_amount(code)

    success, resp_text = transfer_funds(lzt_user_id, amount, code)
    if success:
        update_check_transfer(code, "paid", resp_text)
        run_on_loop(
            loop,
            bot.send_message(
                owner_id,
                f"💸 Чек {code} активирован пользователем {username or lzt_user_id}. {format_amount(amount)}₽ успешно отправлены"
            )
        )
    else:
        update_check_transfer(code, "failed", resp_text)
        run_on_loop(
            loop,
            bot.send_message(
                owner_id,
                f"❌ Чек {code} активирован пользователем {username or lzt_user_id}, "
                f"но перевод не прошел. Ответ API: {resp_text[:500]}"
            )
        )


def poll_forum_once(bot: Bot, owner_id: int, loop: asyncio.AbstractEventLoop) -> None:
    conv_ids: List[int] = []
    conv_meta: Dict[int, Dict[str, Any]] = {}
    pages = CONFIG["polling"].get("conversations_pages", 1)
    start_page = CONFIG["polling"].get("conversations_page", 1)
    for page in range(start_page, start_page + pages):
        conversations_payload = forum_get(
            "/conversations",
            params={"page": page}
        )
        conversations = safe_list(conversations_payload)
        for item in conversations:
            if not isinstance(item, dict):
                continue
            conv_id = item.get("id") or item.get("conversation_id") or item.get("conversationId")
            last_text = normalize_text(extract_last_message_text(item))
            if isinstance(conv_id, str) and conv_id.isdigit():
                conv_id = int(conv_id)
            if isinstance(conv_id, int):
                conv_meta[conv_id] = {
                    "last_text": last_text,
                    "item": item,
                }
            if last_text and "чек" in last_text.lower():
                logger.info(
                    "Нашел смс, которое содержит 'чек' conv=%s text=%s",
                    conv_id, last_text[:200]
                )
    conv_ids = list(dict.fromkeys([cid for cid in conv_ids if isinstance(cid, int)]))
    limit = CONFIG["polling"].get("conversations_limit", 10)
    if isinstance(limit, int) and limit > 0:
        conv_ids = conv_ids[:limit]
    if not conv_ids:
        return

    for conv_id in conv_ids:
        if CONFIG["polling"].get("only_with_check_in_last_message", False):
            last_text = conv_meta.get(conv_id, {}).get("last_text")
            if not last_text or "чек" not in last_text.lower():
                continue
        msg_pages = CONFIG["polling"].get("messages_pages", 1)
        msg_start_page = CONFIG["polling"].get("messages_page", 1)
        last_seen = get_last_seen_message_id(conv_id)
        max_seen = last_seen or 0
        for page in range(msg_start_page, msg_start_page + msg_pages):
            messages_payload = forum_get(
                f"/conversations/{conv_id}/messages",
                params={"page": page}
            )
            messages = extract_messages(messages_payload)
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                msg_id, user_id, username, text = extract_message_fields(msg)
                global _missing_text_logged
                if text is None and _missing_text_logged < MISSING_TEXT_LOG_LIMIT:
                    _missing_text_logged += 1
                if not msg_id or not user_id or not text:
                    continue
                if last_seen and msg_id <= last_seen:
                    continue
                if user_id == CONFIG["lzt"]["account_id"]:
                    continue
                if "чек" in (text or "").lower():
                    logger.info(
                        "Смс с 'чек' conv=%s msg_id=%s user_id=%s text=%s",
                        conv_id, msg_id, user_id, (text or "")[:200]
                    )
                handle_activation_message(
                    conv_id=conv_id,
                    msg_id=msg_id,
                    lzt_user_id=user_id,
                    username=username,
                    text=text,
                    bot=bot,
                    owner_id=owner_id,
                    loop=loop
                )
                if msg_id > max_seen:
                    max_seen = msg_id
        if max_seen and max_seen != (last_seen or 0):
            set_last_seen_message_id(conv_id, max_seen)


async def poll_forum_loop(bot: Bot, owner_id: int) -> None:
    interval = CONFIG["polling"]["interval_sec"]
    while True:
        try:
            loop = asyncio.get_running_loop()
            await asyncio.to_thread(poll_forum_once, bot, owner_id, loop)
        except Exception as exc:
            logger.exception("Ошибка при коннекте к форуму: %s", exc)
        await asyncio.sleep(interval)


CONFIG = load_config()
OWNER_IDS = get_owner_ids(CONFIG)
init_db()

bot = Bot(token=CONFIG["telegram"]["bot_token"])
dp = Dispatcher()


@dp.inline_query()
async def inline_create_check(query: InlineQuery) -> None:
    if not is_owner(query.from_user.id):
        await query.answer([], cache_time=5, is_personal=True)
        return

    q = (query.query or "").strip()
    if not q or not INLINE_AMOUNT_REGEX.match(q):
        await query.answer([], cache_time=1, is_personal=True)
        return

    try:
        amount = decimal_amount(q)
    except (InvalidOperation, ValueError):
        await query.answer([], cache_time=1, is_personal=True)
        return

    if amount <= 0:
        await query.answer([], cache_time=1, is_personal=True)
        return

    cache_key = (query.from_user.id, format_amount(amount))
    cached = INLINE_CACHE.get(cache_key)
    now = time.time()
    if cached and now - cached[1] < INLINE_CACHE_TTL_SEC:
        code = cached[0]
    else:
        code = create_check(amount)
        INLINE_CACHE[cache_key] = (code, now)
    logger.info("Чек создан: amount=%s code=%s", format_amount(amount), code)

    activate_text = f"Активировать чек {code}"
    encoded = quote_plus(activate_text)
    lzt_id = CONFIG["lzt"]["account_id"]
    url = f"https://lolz.live/members/{lzt_id}/write?message={encoded}"

    text = (
        f"🧾 Чек на {format_amount(amount)} ₽\n"
        "Используйте кнопку ниже, чтобы забрать его."
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Получить", url=url)]
    ])

    title = f"Чек на {format_amount(amount)} ₽"
    results = [
        InlineQueryResultArticle(
            id=f"check:{code}",
            title=title,
            input_message_content=InputTextMessageContent(message_text=text),
            reply_markup=keyboard,
        )
    ]
    await query.answer(results, cache_time=1, is_personal=True)


async def main() -> None:
    owner_id = OWNER_IDS[0] if OWNER_IDS else 0
    asyncio.create_task(poll_forum_loop(bot, owner_id))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
