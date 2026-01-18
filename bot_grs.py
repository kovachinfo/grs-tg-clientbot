import os
import logging
import requests
import json
import time
import threading
import re

from database import DatabasePool, get_db_connection
from flask import Flask, request
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------
# Логирование
# ---------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("grs-tg-bot")

# ---------------------------------------------
# Flask приложение
# ---------------------------------------------
app = Flask(__name__)

# ---------------------------------------------
# Ключи и токены
# ---------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

client = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------------------------
# Тексты и настройки
# ---------------------------------------------
MAX_FREE_REQUESTS = 25
MAX_HISTORY_MESSAGES = 10
NEWS_CACHE_TTL_SEC = 24 * 60 * 60

TEXTS = {
    "ru": {
        "welcome": "Добро пожаловать в GRS Bot! 🌍\nПожалуйста, выберите язык:",
        "menu_title": "Главное меню:",
        "btn_news": "📰 Актуальные новости",
        "btn_contact": "📝 Написать менеджеру",
        "btn_limit": "📊 Проверить лимит",
        "news_prompt": (
            "Подготовь сводку новостей (6–10 пунктов) ТОЛЬКО по миграционному праву и политике, "
            "актуальных для граждан РФ (релоканты: ВНЖ/ПМЖ, визы, гражданство, убежище, "
            "трудовая/предпринимательская миграция, учеба, цифровые кочевники, репатриация). "
            "Фокус: страны, популярные у релокантов из России, и Россия. Учитывай санкционные ограничения "
            "и изменения правил въезда/проживания. "
            "Период: весь 2025 год. Используй web_search. "
            "Для каждого пункта укажи дату и источник в формате: "
            "\"Источник: Название статьи, домен\" (без прямых ссылок). "
            "Исключай нерелевантные новости (экономика, спорт, криминал и т.п.). "
            "Не используй Wikipedia или вики-источники. "
            "Формат ответа: простой текст без Markdown; можно добавить тематические эмодзи. "
            "Если в 2025 году по теме меньше 6 значимых новостей, дай меньше и укажи это."
        ),
        "contact_info": "Связаться с менеджером GRS: @globalrelocationsolutions_cz\nБоты и автоматизация: @kovachinfo",
        "limit_info": "Использовано запросов: {count} из {max}.",
        "limit_reached": "🚫 Вы исчерпали лимит бесплатных запросов ({max}).\nПожалуйста, свяжитесь с менеджером для консультации: @manager_username",
        "lang_selected": "🇷🇺 Язык установлен: Русский",
        "searching": "🔍 Ищу информацию, это может занять минуту...",
        "error": "❌ Произошла ошибка сервиса.",
        "rate_limited": "⚠️ Запрос временно недоступен. Попробуйте снова через минуту.",
        "btn_ru": "🇷🇺 Русский",
        "btn_en": "🇬🇧 English"
    },
    "en": {
        "welcome": "Welcome to GRS Bot! 🌍\nPlease select your language:",
        "menu_title": "Main menu:",
        "btn_news": "📰 Latest News",
        "btn_contact": "📝 Contact Manager",
        "btn_limit": "📊 Check Limit",
        "news_prompt": (
            "Prepare a summary (6–10 items) ONLY about migration law and policy relevant to Russian citizens "
            "(visas, residence permits, citizenship, asylum, labor/business migration, study, digital nomads, "
            "repatriation). Focus on countries popular with relocators from Russia and Russia itself; "
            "consider sanctions and entry/residency rule changes. "
            "Time period: the whole of 2025. Use web_search. "
            "For each item include date and source in format: "
            "\"Source: Article title, domain\" (no direct links). "
            "Exclude unrelated news (economy, sports, crime, etc.). "
            "Do not use Wikipedia or wiki sources. "
            "Answer in plain text, no Markdown; you may add thematic emojis. "
            "If fewer than 6 relevant 2025 items exist, provide fewer and state that."
        ),
        "contact_info": "Contact GRS manager: @globalrelocationsolutions_cz\nBots & automation: @kovachinfo",
        "limit_info": "Requests used: {count} of {max}.",
        "limit_reached": "🚫 You have reached the free request limit ({max}).\nPlease contact the manager: @manager_username",
        "lang_selected": "🇬🇧 Language set: English",
        "searching": "🔍 Searching...",
        "error": "❌ Service error.",
        "rate_limited": "⚠️ Request is temporarily unavailable. Please try again in a minute.",
        "btn_ru": "🇷🇺 Русский",
        "btn_en": "🇬🇧 English"
    }
}

# ---------------------------------------------
# Функции работы с пользователями (БД)
# ---------------------------------------------
def get_user(chat_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE chat_id = %s", (chat_id,))
                return cur.fetchone()
    except Exception as e:
        logger.error(f"Error getting user: {e}")
        return None

def create_user(chat_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (chat_id, language_code, request_count) VALUES (%s, 'ru', 0) ON CONFLICT (chat_id) DO NOTHING",
                    (chat_id,)
                )
                conn.commit()
        return get_user(chat_id)
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        return None

def update_user_language(chat_id, lang_code):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET language_code = %s WHERE chat_id = %s", (lang_code, chat_id))
                conn.commit()
    except Exception as e:
        logger.error(f"Error updating language: {e}")

def increment_request_count(chat_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET request_count = request_count + 1 WHERE chat_id = %s", (chat_id,))
                conn.commit()
    except Exception as e:
        logger.error(f"Error incrementing count: {e}")

# Функции работы с историей сообщений (сохранены)
def save_message(chat_id, role, content):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO chat_history (chat_id, role, content) VALUES (%s, %s, %s)",
                    (chat_id, role, content)
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Error saving message: {e}")

def load_history(chat_id, limit=20):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role, content FROM chat_history WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s",
                    (chat_id, limit)
                )
                rows = cur.fetchall()
        return list(reversed(rows))
    except Exception as e:
        logger.error(f"Error loading history: {e}")
        return []

# ---------------------------------------------
# Кэш новостей
# ---------------------------------------------
def get_cached_news(lang):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT content, created_at
                    FROM news_cache
                    WHERE language_code = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (lang,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                created_at = row["created_at"]
                age_sec = (time.time() - created_at.timestamp())
                if age_sec <= NEWS_CACHE_TTL_SEC:
                    return row["content"]
                return None
    except Exception as e:
        logger.error(f"Error getting cached news: {e}")
        return None

def save_cached_news(lang, content):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO news_cache (language_code, content) VALUES (%s, %s)",
                    (lang, content)
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Error saving cached news: {e}")

def clear_cached_news(lang=None):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if lang:
                    cur.execute("DELETE FROM news_cache WHERE language_code = %s", (lang,))
                else:
                    cur.execute("DELETE FROM news_cache")
                conn.commit()
    except Exception as e:
        logger.error(f"Error clearing cached news: {e}")

# ---------------------------------------------
# Очистка простого текста (без Markdown)
# ---------------------------------------------
def sanitize_plain_text(text):
    if not text:
        return text

    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1 — \2", text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"^\s*[-*]\s+", "- ", text, flags=re.M)
    return text.strip()

def needs_news_retry(text):
    if not text:
        return True
    lower = text.lower()
    if "wikipedia.org" in lower or "wikipedia" in lower or "wiki" in lower:
        return True
    return False

# ---------------------------------------------
# Генерация ответа (Native Search)
# ---------------------------------------------
def generate_answer(chat_id, user_message, lang="ru", use_history=True, news_mode=False):
    history = load_history(chat_id, limit=MAX_HISTORY_MESSAGES) if use_history else []

    system_prompt = """Ты — миграционный консультант компании Global Relocation Solutions.
Правила:
1. Отвечай кратко (3–5 предложений).
2. Используй ПОИСК (web_search) для актуальных данных.
3. Язык ответа: {language}.
""".format(language="Русский" if lang == "ru" else "English")
    if news_mode:
        system_prompt += "\n4. Формат ответа: простой текст без Markdown."

    messages = [{"role": "system", "content": system_prompt}]
    for row in history:
        messages.append({"role": row["role"], "content": row["content"]})
    messages.append({"role": "user", "content": user_message})

    # В preview-моделях поиск работает нативно (implicit), без явного указания tools
    # Model: gpt-4o-mini-search-preview
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini-search-preview",
            messages=messages
        )
        content = response.choices[0].message.content.strip()
        content_l = content.lower()

        if (
            "нет доступа" in content_l
            or "no access" in content_l
            or "don't have access" in content_l
            or "do not have access" in content_l
        ):
            retry_rule = (
                "4. Обязательно используй web_search и не сообщай об ограничениях доступа."
                if lang == "ru"
                else "4. You must use web_search and do not mention access limitations."
            )
            retry_prompt = system_prompt + "\n" + retry_rule
            messages[0]["content"] = retry_prompt
            retry = client.chat.completions.create(
                model="gpt-4o-mini-search-preview",
                messages=messages
            )
            return retry.choices[0].message.content.strip()

        if news_mode and needs_news_retry(content):
            retry_rule = (
                "5. Не используй Wikipedia/вики-источники и дай только миграционные новости."
                if lang == "ru"
                else "5. Do not use Wikipedia/wiki sources and only provide migration-related news."
            )
            messages[0]["content"] = system_prompt + "\n" + retry_rule
            retry = client.chat.completions.create(
                model="gpt-4o-mini-search-preview",
                messages=messages
            )
            content = retry.choices[0].message.content.strip()

        return sanitize_plain_text(content) if news_mode else content

    except Exception as e:
        err_text = str(e)
        logger.error(f"Error OpenAI (Search Preview): {err_text}")

        # Попытка fallback без поиска, если превысили лимиты
        try:
            fb = client.chat.completions.create(model="gpt-4o-mini", messages=messages)
            return fb.choices[0].message.content.strip()
        except Exception as fb_err:
            logger.error(f"Fallback error: {fb_err}")
            if "rate_limit" in err_text or "token" in err_text.lower():
                return TEXTS[lang]["rate_limited"]
            return TEXTS[lang]["error"]

# ---------------------------------------------
# Отправка сообщений (с клавиатурой)
# ---------------------------------------------
def send_message(chat_id, text, keyboard=None):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        
        if keyboard:
            payload["reply_markup"] = json.dumps(keyboard)

        resp = requests.post(url, json=payload)
        if not resp.ok:
            logger.error("Send Error: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error(f"Send Error: {e}")

def send_chat_action(chat_id, action="typing"):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendChatAction"
        payload = {"chat_id": chat_id, "action": action}
        resp = requests.post(url, json=payload)
        if not resp.ok:
            logger.error("Chat Action Error: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error(f"Chat Action Error: {e}")

def run_typing(chat_id, stop_event, interval_sec=2):
    while not stop_event.is_set():
        send_chat_action(chat_id, "typing")
        stop_event.wait(interval_sec)

def get_main_keyboard(lang):
    t = TEXTS[lang]
    return {
        "keyboard": [
            [{"text": t["btn_news"]}, {"text": t["btn_contact"]}],
            [{"text": t["btn_limit"]}]
        ],
        "resize_keyboard": True
    }

def get_lang_keyboard():
    return {
        "keyboard": [
            [{"text": TEXTS["ru"]["btn_ru"]}, {"text": TEXTS["en"]["btn_en"]}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

# ---------------------------------------------
# Webhook
# ---------------------------------------------
@app.route(f"/webhook/{TELEGRAM_TOKEN}", methods=["POST"])
def webhook():
    data = request.get_json()
    if not data or "message" not in data:
        return "ok"

    msg = data["message"]
    chat_id = msg.get("chat", {}).get("id")
    text = msg.get("text", "")

    if not chat_id or not text:
        return "ok"

    # 1. Получаем/Создаем пользователя
    user = get_user(chat_id)
    if not user:
        user = create_user(chat_id)
        # Если новый пользователь - просим выбрать язык
        send_message(chat_id, TEXTS["ru"]["welcome"], get_lang_keyboard())
        return "ok"

    lang = user.get("language_code", "ru")
    if lang not in ["ru", "en"]: lang = "ru" # fallback

    # 2. Обработка команд и кнопок
    if text == "/start":
        send_message(chat_id, TEXTS[lang]["welcome"], get_lang_keyboard())
        return "ok"

    if text == "/refresh_news":
        clear_cached_news(lang)
        send_message(chat_id, t["searching"])
        text = t["btn_news"]

    # Смена языка
    if text == TEXTS["ru"]["btn_ru"] or text == "🇷🇺 Русский":
        update_user_language(chat_id, "ru")
        send_message(chat_id, TEXTS["ru"]["lang_selected"], get_main_keyboard("ru"))
        return "ok"
    
    if text == TEXTS["en"]["btn_en"] or text == "🇬🇧 English":
        update_user_language(chat_id, "en")
        send_message(chat_id, TEXTS["en"]["lang_selected"], get_main_keyboard("en"))
        return "ok"

    # Кнопки меню (проверяем оба языка, чтобы избежать рассинхрона)
    t = TEXTS[lang]
    ru_t = TEXTS["ru"]
    en_t = TEXTS["en"]
    
    if text in [ru_t["btn_contact"], en_t["btn_contact"]]:
        send_message(chat_id, t["contact_info"])
        return "ok"
    
    if text in [ru_t["btn_limit"], en_t["btn_limit"]]:
        limit_msg = t["limit_info"].format(count=user['request_count'], max=MAX_FREE_REQUESTS)
        send_message(chat_id, limit_msg)
        return "ok"

    if text in [ru_t["btn_news"], en_t["btn_news"]]:
        # Проверяем лимит перед новостями (это тоже запрос)
        if user['request_count'] >= MAX_FREE_REQUESTS and not user.get('is_premium'):
            send_message(chat_id, t["limit_reached"])
            return "ok"
        
        send_message(chat_id, t["searching"])
        increment_request_count(chat_id)

        send_chat_action(chat_id, "typing")
        stop_event = threading.Event()
        typing_thread = threading.Thread(
            target=run_typing,
            args=(chat_id, stop_event),
            daemon=True
        )
        typing_thread.start()

        try:
            cached = get_cached_news(lang)
            if cached:
                ans = cached
            else:
                # Если нажали русскую кнопку - отвечаем на русском, даже если в БД eng (опционально, но логично)
                # Но пока оставим логику по настройке в БД, чтобы не путать
                ans = generate_answer(chat_id, t["news_prompt"], lang, use_history=False, news_mode=True)
                save_cached_news(lang, ans)
        finally:
            stop_event.set()
        
        save_message(chat_id, "user", text) 
        save_message(chat_id, "assistant", ans)
        send_message(chat_id, ans)
        return "ok"

    # 3. Обработка обычного текстового запроса (ChatGPT)
    
    # Проверка лимита
    if user['request_count'] >= MAX_FREE_REQUESTS and not user.get('is_premium'):
        send_message(chat_id, t["limit_reached"])
        return "ok"

    increment_request_count(chat_id)
    save_message(chat_id, "user", text)
    
    # Можно отправить "печатает..." или уведомление
    ans = generate_answer(chat_id, text, lang)
    save_message(chat_id, "assistant", ans)
    send_message(chat_id, ans)

    return "ok"

if __name__ == "__main__":
    DatabasePool.initialize()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
