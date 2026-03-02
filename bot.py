import os
import time
import random
import threading
import sqlite3
import logging
import re
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from html import escape
import psycopg
from urllib.parse import urlparse


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

import telebot
from telebot import types

from openai import OpenAI

# -------------------------
# ЛОГИ
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("profi_bot")

# -------------------------
# ENV / CONFIG
# -------------------------
# Telegram
TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT_ID = int(os.getenv("TG_CHAT_ID", "0"))

# Profi
PROFI_URL = os.getenv("PROFI_URL", "https://profi.ru/backoffice/n.php")
PROFI_LOGIN = os.getenv("PROFI_LOGIN", "")
PROFI_PASSWORD = os.getenv("PROFI_PASSWORD", "")

# Sleep настройки как в исходнике
CLEAR_HISTORY_SEC = int(os.getenv("CLEAR_HISTORY_SEC", "3600"))
PAGE_REFRESH_MIN = int(os.getenv("PAGE_REFRESH_MIN", "60"))
PAGE_REFRESH_MAX = int(os.getenv("PAGE_REFRESH_MAX", "120"))

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SENT_RETENTION_DAYS = int(os.getenv("SENT_RETENTION_DAYS", "1"))  # сколько дней хранить ID заказов


# LLM (DeepSeek через openai SDK)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

WORK_START = os.getenv("WORK_START", "07:30")
WORK_END = os.getenv("WORK_END", "22:30")
BOT_TIMEZONE = os.getenv("BOT_TIMEZONE", "Europe/Moscow")


# -------------------------
# Глобальные состояния
# -------------------------
sent_links = set()
bot = telebot.TeleBot(TG_TOKEN)

is_running = False
driver = None
main_thread = None
clear_thread = None

def parse_hhmm(s: str) -> dtime:
    hh, mm = s.split(":")
    return dtime(hour=int(hh), minute=int(mm))

def get_work_window():
    start_s = (get_setting("work_start") or "07:30").strip()
    end_s = (get_setting("work_end") or "22:30").strip()
    return parse_hhmm(start_s), parse_hhmm(end_s), start_s, end_s

def get_bot_timezone() -> str:
    return (get_setting("bot_timezone") or "Europe/Moscow").strip()

def get_max_age_seconds() -> int:
    """
    Максимальный возраст заказа в секундах.
    Хранится как минуты в settings.max_age_minutes.
    0 или отрицательное значение = фильтр выключен.
    """
    raw = (get_setting("max_age_minutes") or "0").strip()
    try:
        minutes = int(raw)
    except ValueError:
        minutes = 0

    if minutes <= 0:
        return 0

    return minutes * 60


def is_within_work_hours() -> bool:
    tz_name = get_bot_timezone()
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Europe/Moscow")

    now = datetime.now(tz)
    start_t, end_t, _, _ = get_work_window()

    # окно не пересекает полночь
    return start_t <= now.time() <= end_t

def get_llm_model() -> str:
    # приоритет: БД -> ENV -> дефолт
    m = (get_setting("llm_model") or "").strip()
    if m:
        return m
    env_m = (os.getenv("DEEPSEEK_MODEL") or "").strip()
    return env_m or "deepseek-chat"


# -------------------------
# DB helpers
# -------------------------
def using_postgres() -> bool:
    return bool(DATABASE_URL)


def init_db():
    if using_postgres():
        with psycopg.connect(DATABASE_URL) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            con.execute("""
                INSERT INTO settings(key, value) VALUES ('system_prompt', '')
                ON CONFLICT (key) DO NOTHING
            """)
            # максимальный возраст заказа (в минутах), 0 = без фильтра
            con.execute("""
                INSERT INTO settings(key, value) VALUES ('max_age_minutes', '0')
                ON CONFLICT (key) DO NOTHING
            """)

            con.execute("""
                INSERT INTO settings(key, value) VALUES ('user_prompt', '')
                ON CONFLICT (key) DO NOTHING
            """)

            con.execute("""
                INSERT INTO settings(key, value) VALUES ('filter_keywords', '')
                ON CONFLICT (key) DO NOTHING
            """)
            
            # расписание и таймзона
            con.execute("""
                INSERT INTO settings(key, value) VALUES ('work_start', '07:30')
                ON CONFLICT (key) DO NOTHING
            """)
            con.execute("""
                INSERT INTO settings(key, value) VALUES ('work_end', '22:30')
                ON CONFLICT (key) DO NOTHING
            """)
            con.execute("""
                INSERT INTO settings(key, value) VALUES ('bot_timezone', 'Europe/Moscow')
                ON CONFLICT (key) DO NOTHING
            """)

            # модель LLM
            con.execute("""
                INSERT INTO settings(key, value) VALUES ('llm_model', 'deepseek-chat')
                ON CONFLICT (key) DO NOTHING
            """)
            con.commit()

            con.execute("""
            CREATE TABLE IF NOT EXISTS sent_orders (
                order_id TEXT PRIMARY KEY,
                sent_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

    else:
        with sqlite3.connect(DB_PATH) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            con.execute("""
            CREATE TABLE IF NOT EXISTS sent_orders (
                order_id TEXT PRIMARY KEY,
                sent_at  TEXT NOT NULL
                )
            """)
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('system_prompt', '')")
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('user_prompt', '')")
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('filter_keywords', '')")
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('work_start', '07:30')")
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('work_end', '22:30')")
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('bot_timezone', 'Europe/Moscow')")
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('max_age_minutes', '0')")
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('llm_model', 'deepseek-chat')")
            
            con.commit()




def get_setting(key: str) -> str:
    if using_postgres():
        with psycopg.connect(DATABASE_URL) as con:
            cur = con.execute("SELECT value FROM settings WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else ""
    else:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            return row[0] if row else ""


def set_setting(key: str, value: str):
    if using_postgres():
        with psycopg.connect(DATABASE_URL) as con:
            con.execute("""
                INSERT INTO settings(key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, value))
            con.commit()
    else:
        with sqlite3.connect(DB_PATH) as con:
            con.execute("""
                INSERT INTO settings(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, value))
            con.commit()

#--------------------------
# КЛЮЧЕВЫЕ СЛОВА
#--------------------------
def get_filter_keywords_raw() -> str:
    """
    Строка с ключевыми словами/фразами из settings.filter_keywords.
    Пользователь задаёт её через /filter_set.
    """
    return get_setting("filter_keywords") or ""


def parse_filter_keywords(raw: str) -> list[str]:
    """
    Разбираем строку в список фраз.
    Разделители: ';', запятая, перенос строки.
    Все приводим к нижнему регистру и обрезаем пробелы.
    """
    if not raw:
        return []

    parts = re.split(r"[;\n,]+", raw)
    phrases = [p.strip().lower() for p in parts if p.strip()]
    return phrases


def get_filter_keywords() -> list[str]:
    return parse_filter_keywords(get_filter_keywords_raw())


def is_blocked_by_keywords(order: dict) -> bool:
    """
    Возвращает True, если заказ нужно ОТФИЛЬТРОВАТЬ по ключевым словам.
    Если список пуст — фильтр выключен.
    """
    keywords = get_filter_keywords()
    if not keywords:
        return False

    text = (order.get("subject", "") + " " + order.get("description", "")).lower()

    for phrase in keywords:
        if phrase in text:
            return True

    return False



#Список уже отправленных заказов

def is_order_sent_db(order_id: str) -> bool:
    if not order_id:
        return False

    if using_postgres():
        with psycopg.connect(DATABASE_URL) as con:
            cur = con.execute(
                "SELECT 1 FROM sent_orders WHERE order_id = %s LIMIT 1",
                (order_id,)
            )
            return cur.fetchone() is not None
    else:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.execute(
                "SELECT 1 FROM sent_orders WHERE order_id = ? LIMIT 1",
                (order_id,)
            )
            return cur.fetchone() is not None


def mark_order_sent_db(order_id: str) -> None:
    if not order_id:
        return

    now_iso = datetime.utcnow().isoformat()

    if using_postgres():
        with psycopg.connect(DATABASE_URL) as con:
            con.execute(
                """
                INSERT INTO sent_orders(order_id, sent_at)
                VALUES (%s, NOW())
                ON CONFLICT (order_id) DO NOTHING
                """,
                (order_id,)
            )
            con.commit()
    else:
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                """
                INSERT OR IGNORE INTO sent_orders(order_id, sent_at)
                VALUES (?, ?)
                """,
                (order_id, now_iso)
            )
            con.commit()


def clear_old_sent_orders() -> None:
    days = SENT_RETENTION_DAYS
    if days <= 0:
        return

    cutoff = datetime.utcnow() - timedelta(days=days)

    if using_postgres():
        with psycopg.connect(DATABASE_URL) as con:
            cur = con.execute(
                "DELETE FROM sent_orders WHERE sent_at < %s",
                (cutoff,)
            )
            deleted = cur.rowcount  # берём rowcount с курсора, а не с con
            con.commit()
        logger.info("Очистка sent_orders (Postgres): удалено %s записей", deleted)
    else:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.execute(
                "DELETE FROM sent_orders WHERE sent_at < ?",
                (cutoff.isoformat(),)
            )
            deleted = cur.rowcount
            con.commit()
        logger.info("Очистка sent_orders (SQLite): удалено %s записей", deleted)



def clear_all_sent_orders() -> None:
    """
    Полная очистка истории отправленных заказов в БД (для /clear).
    """
    if using_postgres():
        with psycopg.connect(DATABASE_URL) as con:
            con.execute("TRUNCATE TABLE sent_orders")
            con.commit()
    else:
        with sqlite3.connect(DB_PATH) as con:
            con.execute("DELETE FROM sent_orders")
            con.commit()



# -------------------------
# Selenium
# -------------------------
def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--lang=ru-RU")

    chrome_bin = os.getenv("CHROME_BIN") or os.getenv("GOOGLE_CHROME_BIN")
    if chrome_bin:
        chrome_options.binary_location = chrome_bin

    # если CHROMEDRIVER_PATH задан и файл существует — используем его
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    if driver_path and os.path.exists(driver_path):
        service = Service(driver_path)
        return webdriver.Chrome(service=service, options=chrome_options)

    # иначе — ставим/подбираем драйвер автоматически
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def login(driver):
    """Авторизация на Profi.ru с учетом перехода через страницу регистрации"""
    try:
        wait = WebDriverWait(driver, 20)

        # 1) Главная страница
        driver.get("https://profi.ru/")
        time.sleep(2)

        # 2) Переход на "Сайт для специалистов"
        try:
            spec_link = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                "//a[contains(., 'Сайт для специалистов') or contains(., 'для специалистов')]"
            )))
            spec_link.click()
            time.sleep(2)
        except Exception as e:
            logger.error("Не нашёл ссылку 'Сайт для специалистов': %s", e)
            # пробуем сразу в backoffice
            driver.get("https://profi.ru/backoffice/")
            time.sleep(2)

        # 3) На странице для специалистов почти всегда сначала открывается регистрация.
        # Ищем ссылку "Вход для специалистов" / "Вход для профи" и кликаем её.
        try:
            login_link = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                "//a[contains(., 'Вход для специалистов') or contains(., 'Вход для профи') or contains(., 'Вход для профи')]"
            )))
            login_link.click()
            time.sleep(2)
        except Exception as e:
            logger.warning("Не удалось найти ссылку 'Вход для специалистов': %s", e)
            # ничего страшного, возможно мы уже на странице входа

        # 4) Теперь уже должны быть поля логина/пароля.
        #   Логин/телефон
        login_input = wait.until(EC.presence_of_element_located((
            By.XPATH,
            "//input[contains(@placeholder,'Логин') or contains(@placeholder,'телефон')]"
        )))

        login_input.clear()
        login_input.click()
        # триггерим появление поля пароля
        login_input.send_keys(PROFI_LOGIN)

        # 5) Ждём появления поля "Пароль"
        password_input = wait.until(EC.presence_of_element_located((
            By.XPATH,
            "//input[contains(@placeholder,'Пароль') or @type='password']"
        )))
        password_input.clear()
        password_input.click()
        password_input.send_keys(PROFI_PASSWORD)

        # 6) Кнопка "Продолжить"
        btn = None
        btn_variants = [
            (By.XPATH, "//button[contains(., 'Продолжить')]"),
            (By.CSS_SELECTOR, "button[type='submit']"),
        ]
        for by, sel in btn_variants:
            try:
                btn = driver.find_element(by, sel)
                if btn:
                    break
            except Exception:
                continue

        if not btn:
            logger.error("Не найдена кнопка входа на странице логина")
            return False

        btn.click()
        time.sleep(4)

        # 7) Переходим в backoffice с заказами
        driver.get(PROFI_URL)
        time.sleep(3)

        # Простая проверка: если снова видим текст "Вход и регистрация", значит не залогинились
        page = driver.page_source.lower()
        if "вход и регистрация для профи" in page and "логин или телефон" in page:
            logger.error("Похоже, авторизация не прошла: всё ещё страница входа")
            return False

        logger.info("Авторизация прошла успешно, current_url=%s", driver.current_url)
        return True

    except Exception as e:
        logger.exception("Ошибка авторизации: %s", e)
        return False



# -------------------------
# Парсинг (из оригинала)
# -------------------------

PRICE_RANGE_RE = re.compile(r"\d[\d\s]*\s*[-–]\s*\d[\d\s]*\s*₽")
PRICE_SINGLE_RE = re.compile(r"\d[\d\s]*\s*₽")
PRICE_PREFIX_RE = re.compile(r"\b(от|до)\s*(\d[\d\s]*\s*₽)", re.I)

TIME_REL_RE = re.compile(
    r"(только что|\d+\s*(?:минут|мин|час|часа|часов)\s*назад)",
    re.I
)

MONTHS_RU = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}

def parse_time_label(label: str, now: datetime) -> datetime | None:
    """
    Преобразует строку времени заказа в datetime (в таймзоне now.tzinfo).

    Поддерживает:
    - "только что"
    - "6 минут назад", "2 часа назад" и т.п.
    - "Вчера" / "Вчера в 18:34"
    - "9 декабря" / "9 декабря в 18:34" / "9 декабря 2025" ...
    """
    if not label:
        return None

    s = label.strip().lower()

    # только что
    if s == "только что":
        return now

    # N минут/часов назад
    m = re.match(r"(\d+)\s*(минут|минуты|минуту|мин|м)\s*назад", s)
    if m:
        mins = int(m.group(1))
        return now - timedelta(minutes=mins)

    m = re.match(r"(\d+)\s*(час|часа|часов)\s*назад", s)
    if m:
        hrs = int(m.group(1))
        return now - timedelta(hours=hrs)

    # Вчера / Вчера в HH:MM
    m = re.match(r"вчера(?:\s*в\s*(\d{1,2}):(\d{2}))?$", s)
    if m:
        h = int(m.group(1)) if m.group(1) else 12
        mi = int(m.group(2)) if m.group(2) else 0
        dt = (now - timedelta(days=1)).replace(hour=h, minute=mi, second=0, microsecond=0)
        return dt

    # Дата в виде "9 декабря" / "9 декабря в 18:34" / "9 декабря 2025" / ...
    m = re.match(
        r"(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?(?:\s*в\s*(\d{1,2}):(\d{2}))?$",
        s,
    )
    if m:
        day = int(m.group(1))
        mon_name = m.group(2)
        year = int(m.group(3)) if m.group(3) else now.year
        h = int(m.group(4)) if m.group(4) else 12
        mi = int(m.group(5)) if m.group(5) else 0

        month = MONTHS_RU.get(mon_name, None)
        if not month:
            return None

        try:
            dt = now.replace(year=year, month=month, day=day,
                             hour=h, minute=mi, second=0, microsecond=0)
        except ValueError:
            return None

        # если вдруг дата в будущем (например, сейчас январь, а дата "9 декабря" без года),
        # считаем, что речь о прошлом году
        if dt > now + timedelta(days=1):
            try:
                dt = dt.replace(year=year - 1)
            except ValueError:
                pass
        return dt

    return None


def extract_price(container) -> str:
    """
    Достает цену из карточки:
    - '1200 ₽'
    - '1200–1500 ₽'
    - 'от 900 ₽'
    - 'до 1800 ₽'
    приоритетно из aria-hidden блока (без 'false').
    """
    # 1) визуальный aria-hidden
    aria = container.select_one("[aria-hidden='true']")
    if aria:
        txt = aria.get_text(" ", strip=True)
        txt = re.sub(r"\s{2,}", " ", txt).strip()

        m = PRICE_PREFIX_RE.search(txt)
        if m:
            return f"{m.group(1).lower()} {m.group(2).strip()}"

        m = PRICE_RANGE_RE.search(txt)
        if m:
            return m.group(0).strip()

        m = PRICE_SINGLE_RE.search(txt)
        if m:
            return m.group(0).strip()

    # 2) fallback по всему тексту
    full_text = container.get_text(" ", strip=True)
    full_text = re.sub(r"\bfalse\b", "", full_text, flags=re.I)
    full_text = re.sub(r"\s{2,}", " ", full_text).strip()

    m = PRICE_PREFIX_RE.search(full_text)
    if m:
        return f"{m.group(1).lower()} {m.group(2).strip()}"

    m = PRICE_RANGE_RE.search(full_text)
    if m:
        return m.group(0).strip()

    m = PRICE_SINGLE_RE.search(full_text)
    if m:
        return m.group(0).strip()

    return ""


def extract_time_info(container) -> tuple[str, datetime | None]:
    """
    Ищем надпись про время заказа и сразу считаем datetime.

    Возвращает:
      (label, created_at_datetime or None)
    """
    tz = ZoneInfo(BOT_TIMEZONE)
    now = datetime.now(tz)

    # Сначала пробуем по span-ам (обычно отдельный блок наверху/слева)
    for span in container.find_all("span"):
        txt = span.get_text(" ", strip=True)
        if not txt:
            continue
        dt = parse_time_label(txt, now)
        if dt is not None:
            return txt, dt

    # Если не нашли — можно попробовать общий текст (редкий случай)
    full_text = container.get_text(" ", strip=True)
    for piece in re.split(r"[•·\n]", full_text):
        t = piece.strip()
        if not t:
            continue
        dt = parse_time_label(t, now)
        if dt is not None:
            return t, dt

    return "", None


def extract_description(container, subject: str, price: str, time_label: str) -> str:
    """
    Собираем описание:
    - основной <p> с текстом задачи
    - элементы списка (дистанционно/город/расписание/имя)
    + fallback: общий текст минус subject/price/time_label.
    """
    parts = []

    main_p = container.find("p")
    if main_p:
        parts.append(main_p.get_text(" ", strip=True))

    for span in container.select("li[role='listitem'] span"):
        parts.append(span.get_text(" ", strip=True))

    desc = " ".join(parts)
    desc = re.sub(r"\s{2,}", " ", desc).strip()

    if not desc:
        # fallback — общий текст
        full_text = container.get_text(" ", strip=True)
        desc = full_text

        if subject:
            desc = desc.replace(subject, "", 1).strip()
        if price:
            desc = desc.replace(price, "", 1).strip()
        if time_label:
            desc = desc.replace(time_label, "", 1).strip()

    # убрать 'false' только в начале
    desc = re.sub(r"^\s*false\b\s*", "", desc, flags=re.I)
    # убрать висячие 'от'/'до' в начале (если вдруг цена не вычлась точь-в-точь)
    desc = re.sub(r"^\s*(?:от|до)\b\s*", "", desc, flags=re.I)

    desc = re.sub(r"\s{2,}", " ", desc).strip()
    return desc

def parse_order(container):
    try:
        data_testid = container.get("data-testid", "")
        href = container.get("href", "")

        if not data_testid or "_order-snippet" not in data_testid or not href:
            return None

        order_id = data_testid.split("_")[0].strip()

        # Заголовок
        subject = (container.get("aria-label", "") or "").strip()
        if not subject:
            h3 = container.find("h3")
            if h3:
                subject = h3.get_text(" ", strip=True)
        if not subject:
            subject = "Новый заказ"

        # Цена
        price = extract_price(container)

        # Время (строка + точный datetime)
        time_label, created_at = extract_time_info(container)

        # Описание
        description = extract_description(container, subject, price, time_label)

        if not order_id or not subject:
            return None

        order = {
            "link": order_id,
            "subject": subject,
            "description": description,
            "price": price,
            "time_info": time_label,  # как раньше — для отображения
        }

        # дополнительные поля для будущей фильтрации
        if created_at is not None:
            order["created_at"] = created_at.isoformat()
            order["age_seconds"] = int((datetime.now(created_at.tzinfo) - created_at).total_seconds())

        return order

    except Exception as e:
        logger.exception("Ошибка парсинга: %s", str(e))
        return None




# -------------------------
# LLM
# -------------------------
def get_llm_client():
    if not OPENAI_API_KEY:
        return None
    return OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)


def default_user_prompt_template() -> str:
    return (
        "Составь персонализированный отклик репетитора на заказ.\n"
        "Формат:\n"
        "- 3–6 предложений\n"
        "- дружелюбно и профессионально\n"
        "- конкретно по запросу\n"
        "- без лишних обещаний\n"
        "- в конце можно 1 уточняющий вопрос, если нужно.\n\n"
        "Предмет/тема: {subject}\n"
        "Описание: {description}\n"
        "Бюджет: {price}\n"
        "Время/дата: {time_info}\n"
    ).strip()


def build_user_prompt(order: dict) -> str:
    tpl = get_setting("user_prompt") or ""

    # если шаблон пустой — используем базовый дефолт
    if not tpl.strip():
        tpl = default_user_prompt_template()

    # безопасная подстановка
    data = {
        "subject": order.get("subject", ""),
        "description": order.get("description", ""),
        "price": order.get("price", ""),
        "time_info": order.get("time_info", ""),
    }

    try:
        return tpl.format(**data)
    except Exception:
        # если пользователь случайно сломал форматирование
        return default_user_prompt_template().format(**data)

def build_system_prompt() -> str:
    """
    Возвращает системный промт для LLM.

    Приоритет:
    1) settings.system_prompt в БД
    2) SYSTEM_PROMPT из переменных окружения (если задан)
    3) пустая строка
    """
    sp = (get_setting("system_prompt") or "").strip()
    if sp:
        return sp

    sp_env = (os.getenv("SYSTEM_PROMPT", "") or "").strip()
    if sp_env:
        return sp_env

    return ""



def generate_personal_reply(order: dict) -> str:
    system_prompt = build_system_prompt()
    user_prompt = build_user_prompt(order)

    client = get_llm_client()
    if client is None:
        # Фоллбек, чтобы бот не ломался без ключа
        return (
            "Здравствуйте! Меня заинтересовал ваш запрос. "
            "Готов(а) помочь и предложить удобный формат занятий. "
            "Подскажите, пожалуйста, текущий уровень и цель обучения?"
        )

    try:
        model = get_llm_model()

        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.6,
        )
        text = (resp.choices[0].message.content or "").strip()
        return text or "Здравствуйте! Готов(а) помочь по вашему запросу."
    except Exception as e:
        logger.exception("Ошибка LLM: %s", str(e))
        return "Здравствуйте! Готов(а) помочь по вашему запросу."


# -------------------------
# Telegram send
# -------------------------
def send_telegram_message(order, reply_text: str):
    """Отправка сообщения в Telegram + текст отклика"""
    try:
        message = f"<b>{order['subject']}</b>\n"
        if order.get('price'):
            message += f"<b>{order['price']}</b>\n"
        message += f"\n{order['description']}\n\n<i>{order['time_info']}</i>\n"
        message += "\n<b>Персонализированный отклик:</b>\n"
        message += f"<pre>{escape(reply_text)}</pre>"


        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(
            text="Откликнуться",
            url=f"https://profi.ru/backoffice/n.php?o={order['link']}"
        ))

        bot.send_message(
            chat_id=TG_CHAT_ID,
            text=message,
            reply_markup=markup,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.exception("Ошибка отправки сообщения: %s", str(e))


# -------------------------
# History cleaner
# -------------------------
def clear_history():
    """Очистка истории отправленных ссылок в памяти и старых записей в БД"""
    global sent_links
    while is_running:
        time.sleep(CLEAR_HISTORY_SEC)
        sent_links.clear()
        logger.info("Кэш отправленных ссылок в памяти очищен")
        try:
            clear_old_sent_orders()
        except Exception as e:
            logger.exception("Ошибка при очистке старых записей sent_orders: %s", e)


def restart_driver() -> bool:
    """
    Перезапускает браузер Selenium и повторно логинится на Profi.ru.
    Возвращает True при успехе, False при неудаче.
    """
    global driver

    # Пытаемся корректно закрыть старый драйвер
    try:
        if driver is not None:
            driver.quit()
    except Exception:
        logger.exception("Ошибка при закрытии старого WebDriver")

    driver = None

    logger.info("Перезапуск WebDriver...")

    try:
        driver = init_driver()
    except Exception:
        logger.exception("Не удалось создать новый WebDriver")
        return False

    if not login(driver):
        logger.error("Не удалось авторизоваться на Profi.ru после перезапуска браузера")
        try:
            bot.send_message(
                TG_CHAT_ID,
                "❌ Не удалось авторизоваться на Profi.ru после перезапуска браузера. Мониторинг остановлен."
            )
        except Exception:
            logger.exception("Не удалось отправить сообщение в Telegram о проблеме авторизации")
        return False

    logger.info("WebDriver успешно перезапущен и авторизован на Profi.ru")
    return True


# -------------------------
# Main loop
# -------------------------
def main_loop():
    """Основной цикл обработки заказов"""
    global driver, is_running, sent_links

    driver = init_driver()

    if not login(driver):
        bot.send_message(
            TG_CHAT_ID,
            "❌ Ошибка авторизации на Profi.ru!"
        )
        is_running = False
        return

    while is_running:
        # Проверка расписания
        if not is_within_work_hours():
            start_t, end_t, start_s, end_s = get_work_window()
            tz_name = get_bot_timezone()
            logger.info("Вне рабочего окна (%s-%s, %s). Сплю...", start_s, end_s, tz_name)
            time.sleep(300)  # 5 минут
            continue

        try:
            # Обновление страницы
            driver.refresh()
            time.sleep(10)

            # Парсинг страницы
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            containers = soup.select("a[data-testid$='_order-snippet']")

            logger.info("order-snippet anchors found=%s", len(containers))

            if not containers:
                logger.info("Заказы не найдены")
                time.sleep(random.randint(PAGE_REFRESH_MIN, PAGE_REFRESH_MAX))
                continue

            max_age = get_max_age_seconds()

            # Обработка заказов
            for container in containers:
                if not is_running:
                    break

                order = parse_order(container)
                if not order:
                    continue

                # Фильтр по ключевым словам
                if is_blocked_by_keywords(order):
                    logger.info("Заказ %s отфильтрован по ключевым словам", order.get("link"))
                    continue

                order_id = order.get("link")
                if not order_id:
                    continue

                # Фильтр по возрасту
                age_sec = order.get("age_seconds")
                if max_age > 0 and age_sec is not None and age_sec > max_age:
                    logger.info(
                        "Пропускаем заказ %s: слишком старый (%s сек, лимит %s)",
                        order_id, age_sec, max_age
                    )
                    continue

                # Антидубли: память + БД
                if order_id in sent_links or is_order_sent_db(order_id):
                    sent_links.add(order_id)
                    continue

                try:
                    reply_text = generate_personal_reply(order)
                    send_telegram_message(order, reply_text)

                    sent_links.add(order_id)
                    mark_order_sent_db(order_id)

                    logger.info("Отправлен: %s (%s)", order.get("subject"), order_id)
                except Exception as e:
                    logger.exception("Ошибка обработки заказа %s: %s", order_id, e)

            # Случайная задержка перед следующим обновлением
            time.sleep(random.randint(PAGE_REFRESH_MIN, PAGE_REFRESH_MAX))

        except WebDriverException as e:
            # кейс "tab crashed"
            logger.exception("Ошибка WebDriver (вкладка браузера упала): %s", e)

            if not restart_driver():
                logger.error("Не удалось перезапустить браузер, останавливаем основной цикл")
                is_running = False
                break

            time.sleep(15)
            continue

        except Exception as e:
            logger.exception("Критическая ошибка в main_loop: %s", str(e))
            time.sleep(60)

    # Выходим из цикла — гасим драйвер
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
        driver = None



# -------------------------
# Bot commands (run control)
# -------------------------
@bot.message_handler(commands=['start'])
def start_command(message):
    global is_running, main_thread, clear_thread

    if is_running:
        bot.send_message(message.chat.id, "Бот уже запущен!")
        return

    is_running = True
    main_thread = threading.Thread(target=main_loop, daemon=True)
    clear_thread = threading.Thread(target=clear_history, daemon=True)

    main_thread.start()
    clear_thread.start()

    bot.send_message(message.chat.id, "Бот запущен и начал мониторинг заказов!")


@bot.message_handler(commands=['stop'])
def stop_command(message):
    global is_running, main_thread, clear_thread, driver

    if not is_running:
        bot.send_message(message.chat.id, "Бот уже остановлен!")
        return

    is_running = False

    # аккуратно ждём завершения потока
    if main_thread:
        main_thread.join(timeout=10)
    if clear_thread:
        clear_thread.join(timeout=10)

    if driver:
        try:
            driver.quit()
        except Exception:
            pass
        driver = None

    bot.send_message(message.chat.id, "Бот остановлен!")


@bot.message_handler(commands=['clear'])
def clear_command(message):
    global sent_links
    sent_links.clear()
    clear_all_sent_orders()
    bot.send_message(
        message.chat.id,
        "История отправленных заказов очищена (память + БД)."
    )

# -------------------------
# Prompt management commands
# -------------------------
@bot.message_handler(commands=['prompt_show'])
def prompt_show_command(message):
    sp = get_setting("system_prompt")
    if sp:
        bot.send_message(message.chat.id, f"Текущий системный промт:\n\n{sp}")
    else:
        bot.send_message(message.chat.id, "Системный промт сейчас пустой.")


@bot.message_handler(commands=['prompt_clear'])
def prompt_clear_command(message):
    set_setting("system_prompt", "")
    bot.send_message(message.chat.id, "Системный промт очищен.")


@bot.message_handler(commands=['prompt_set'])
def prompt_set_command(message):
    # /prompt_set <текст>
    text = (message.text or "").strip()
    parts = text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        bot.send_message(message.chat.id, "Использование: /prompt_set <текст>")
        return

    new_prompt = parts[1].strip()
    set_setting("system_prompt", new_prompt)
    bot.send_message(message.chat.id, "Системный промт обновлён.")

@bot.message_handler(commands=['uprompt_show'])
def uprompt_show_command(message):
    up = get_setting("user_prompt")
    if up:
        bot.send_message(message.chat.id, f"Текущий user prompt:\n\n{up}")
    else:
        bot.send_message(message.chat.id, "User prompt сейчас пустой (используется базовый дефолт).")

@bot.message_handler(commands=['uprompt_set'])
def uprompt_set_command(message):
    text = (message.text or "").strip()
    parts = text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        bot.send_message(
            message.chat.id,
            "Использование: /uprompt_set <текст>\n\n"
            "Можно использовать переменные:\n"
            "{subject}, {description}, {price}, {time_info}"
        )
        return

    new_prompt = parts[1].strip()
    set_setting("user_prompt", new_prompt)
    bot.send_message(message.chat.id, "User prompt обновлён.")

@bot.message_handler(commands=['uprompt_clear'])
def uprompt_clear_command(message):
    set_setting("user_prompt", "")
    bot.send_message(message.chat.id, "User prompt очищен. Будет использоваться базовый дефолт.")


@bot.message_handler(commands=['schedule_show'])
def schedule_show_command(message):
    start_s = get_setting("work_start") or "07:30"
    end_s = get_setting("work_end") or "22:30"
    tz = get_bot_timezone()
    bot.send_message(
        message.chat.id,
        f"Текущее расписание работы:\n"
        f"С {start_s} до {end_s}\n"
        f"Таймзона: {tz}"
    )

@bot.message_handler(commands=['schedule_set'])
def schedule_set_command(message):
    # формат: /schedule_set 07:30 22:30
    text = (message.text or "").strip()
    parts = text.split()

    if len(parts) != 3:
        bot.send_message(message.chat.id, "Использование: /schedule_set 07:30 22:30")
        return

    start_s, end_s = parts[1], parts[2]
    if not re.match(r"^\d{2}:\d{2}$", start_s) or not re.match(r"^\d{2}:\d{2}$", end_s):
        bot.send_message(message.chat.id, "Неверный формат времени. Нужно HH:MM")
        return

    try:
        parse_hhmm(start_s)
        parse_hhmm(end_s)
    except Exception:
        bot.send_message(message.chat.id, "Некорректное время.")
        return

    set_setting("work_start", start_s)
    set_setting("work_end", end_s)

    tz = get_bot_timezone()
    bot.send_message(message.chat.id, f"Ок! Новое расписание: {start_s}–{end_s} ({tz})")

@bot.message_handler(commands=['model_show'])
def model_show_command(message):
    m = get_llm_model()
    bot.send_message(message.chat.id, f"Текущая модель LLM: {m}\n"
                                      f"Доступно: deepseek-chat, deepseek-reasoner")

@bot.message_handler(commands=['model_set'])
def model_set_command(message):
    # формат: /model_set chat  или /model_set reasoner
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)

    if len(parts) != 2:
        bot.send_message(message.chat.id, "Использование: /model_set chat|reasoner")
        return

    val = parts[1].strip().lower()

    mapping = {
        "chat": "deepseek-chat",
        "reasoner": "deepseek-reasoner",
        "deepseek-chat": "deepseek-chat",
        "deepseek-reasoner": "deepseek-reasoner",
    }

    if val not in mapping:
        bot.send_message(message.chat.id, "Неизвестная модель. Используй chat или reasoner.")
        return

    model = mapping[val]
    set_setting("llm_model", model)
    bot.send_message(message.chat.id, f"Модель обновлена: {model}")

@bot.message_handler(commands=['age_show'])
def age_show_command(message):
    raw = get_setting("max_age_minutes") or "0"
    try:
        minutes = int(raw)
    except ValueError:
        minutes = 0

    if minutes <= 0:
        bot.send_message(
            message.chat.id,
            "Фильтр по возрасту заказов сейчас выключен.\n"
            "Приходят все заказы."
        )
    else:
        bot.send_message(
            message.chat.id,
            f"Сейчас приходят только заказы не старше {minutes} мин."
        )


@bot.message_handler(commands=['age_set'])
def age_set_command(message):
    # формат: /age_set 60  или /age_set 20
    text = (message.text or "").strip()
    parts = text.split()

    if len(parts) != 2:
        bot.send_message(
            message.chat.id,
            "Использование: /age_set <минуты>\n"
            "Например: /age_set 60"
        )
        return

    try:
        minutes = int(parts[1])
    except ValueError:
        bot.send_message(
            message.chat.id,
            "Нужно указать целое число минут, например: 60"
        )
        return

    if minutes < 0:
        minutes = 0

    set_setting("max_age_minutes", str(minutes))

    if minutes == 0:
        bot.send_message(
            message.chat.id,
            "Фильтр по возрасту заказов выключен.\n"
            "Будут приходить все заказы."
        )
    else:
        bot.send_message(
            message.chat.id,
            f"Ок! Теперь будут приходить только заказы моложе {minutes} минут."
        )

@bot.message_handler(commands=['filter_show'])
def filter_show_command(message):
    raw = get_filter_keywords_raw()
    phrases = get_filter_keywords()

    if not phrases:
        bot.send_message(
            message.chat.id,
            "Список фильтрующих ключевых слов пуст.\n"
            "Фильтр по словам выключен."
        )
        return

    pretty = "\n".join(f"- {p}" for p in phrases)
    bot.send_message(
        message.chat.id,
        "Сейчас заказы ОТФИЛЬТРОВЫВАЮТСЯ, если содержат любую из фраз:\n\n" + pretty
    )


@bot.message_handler(commands=['filter_set'])
def filter_set_command(message):
    """
    /filter_set слово1; фраза 2; другое слово
    Можно писать через ; или каждый с новой строки.
    """
    text = (message.text or "").strip()
    parts = text.split(" ", 1)

    if len(parts) < 2 or not parts[1].strip():
        bot.send_message(
            message.chat.id,
            "Использование:\n"
            "/filter_set слово1; фраза 2; слово3\n\n"
            "Разделяй слова/фразы через ';' или перенос строки.\n"
            "Если хотя бы одна фраза встречается в заказе — он НЕ приходит."
        )
        return

    raw = parts[1].strip()
    set_setting("filter_keywords", raw)
    phrases = get_filter_keywords()
    pretty = "\n".join(f"- {p}" for p in phrases) if phrases else "нет"

    bot.send_message(
        message.chat.id,
        "Фильтр обновлён.\n"
        "Теперь заказы будут отфильтровываться по следующим фразам:\n\n" + pretty
    )


@bot.message_handler(commands=['filter_clear'])
def filter_clear_command(message):
    set_setting("filter_keywords", "")
    bot.send_message(
        message.chat.id,
        "Фильтр по ключевым словам очищен.\n"
        "Теперь приходят все заказы (кроме других фильтров, если они есть)."
    )


@bot.message_handler(commands=['help'])
def help_command(message):
    bot.send_message(
        message.chat.id,
        "Команды:\n"
        "/start — запустить мониторинг\n"
        "/stop — остановить\n"
        "/clear — очистить историю отправленных заказов\n"
        "/get — показать отправленные ID\n\n"
        "Промты:\n"
        "/prompt_show\n"
        "/prompt_set <текст>\n"
        "/prompt_clear\n"
        "/uprompt_show\n"
        "/uprompt_set <текст с {subject} {description} {price} {time_info}>\n"
        "/uprompt_clear\n\n"
        "Фильтр по ключевым словам:\n"
        "/filter_show — показать текущий список\n"
        "/filter_set слово1; фраза 2; слово3 — задать список\n"
        "/filter_clear — удалить список (выключить фильтр)\n\n"
        "Расписание:\n"
        "/schedule_show\n"
        "/schedule_set 07:30 22:30\n"
        "/tz_set Europe/Moscow\n\n"
        "LLM:\n"
        "/model_show\n"
        "/model_set chat|reasoner"
    )



# -------------------------
# Entrypoint
# -------------------------
if __name__ == "__main__":
    init_db()
    logger.info("Бот запущен. Ожидание команд...")
    bot.infinity_polling()

























