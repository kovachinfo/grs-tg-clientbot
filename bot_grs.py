import os
import logging
import requests
import requests
from database import DatabasePool, get_db_connection
from flask import Flask, request
from openai import OpenAI

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
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

client = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------------------------
# Инициализация базы данных (через database.py)
# ---------------------------------------------
# init_db теперь в отдельном файле init_db.py или вызывается отдельно


# ---------------------------------------------
# Сохранение сообщения в БД
# ---------------------------------------------
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
        logger.error(f"Ошибка сохранения сообщения: {e}")

# ---------------------------------------------
# Загрузка последних сообщений из БД
# ---------------------------------------------
def load_history(chat_id, limit=20):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role, content FROM chat_history WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s",
                    (chat_id, limit)
                )
                rows = cur.fetchall()
        return list(reversed(rows))  # от старых к новым
    except Exception as e:
        logger.error(f"Ошибка загрузки истории: {e}")
        return []

# ---------------------------------------------
# Запрос к Tavily API (поиск)
# ---------------------------------------------
def tavily_search(query):
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            headers={"Authorization": f"Bearer {TAVILY_API_KEY}"},
            json={"query": query, "num_results": 3}
        )
        data = resp.json()
        if "results" in data:
            return "\n".join([r["content"] for r in data["results"]])
        return None
    except Exception as e:
        logger.error(f"Tavily error: {e}")
        return None

# ---------------------------------------------
# Генерация ответа через OpenAI
# ---------------------------------------------
def generate_answer(chat_id, user_message):
    # Загружаем историю
    history = load_history(chat_id)

    # Формируем список сообщений
    messages = [{"role": "system", "content": (
        """Ты — миграционный консультант компании Global Relocation Solutions.

Правила ответов:
1. Отвечай кратко и структурировано (3–5 предложений).
2. Излагай только проверенные факты. Если есть сомнение → используй поиск.
3. Избегай двусмысленных формулировок. Пиши так, чтобы ответ был однозначным.
4. Если вопрос связан с законодательством, указывай источник информации.
5. Если вопрос выходит за рамки миграции — отвечай вежливо и перенаправляй."""
    )}]

    for row in history:
        messages.append({"role": row["role"], "content": row["content"]})

    messages.append({"role": "user", "content": user_message})

    # Попробуем поискать в Tavily
    tavily_info = tavily_search(user_message)
    if tavily_info:
        messages.append({"role": "system", "content": f"Актуальная информация из поиска:\n{tavily_info}"})

    try:
        response = client.chat.completions.create(
            model="gpt-4.1",
            messages=messages,
            max_completion_tokens=800
        )
        answer = response.choices[0].message.content.strip()
        return answer
    except Exception as e:
        logger.error(f"Ошибка OpenAI: {e}")
        return "Извините, я не смог сгенерировать ответ."

# ---------------------------------------------
# Telegram webhook (с учётом reply_to_message)
# ---------------------------------------------
@app.route(f"/webhook/{TELEGRAM_TOKEN}", methods=["POST"])
def webhook():
    data = request.get_json()
    logger.info(f"Update: {data}")

    if "message" in data and "text" in data["message"]:
        chat_id = data["message"]["chat"]["id"]
        user_message = data["message"]["text"]

        # Проверяем, было ли это reply на другое сообщение
        if "reply_to_message" in data["message"]:
            original_text = data["message"]["reply_to_message"].get("text", "")
            if original_text:
                user_message = f"(Ответ на сообщение: '{original_text}') {user_message}"

        # Сохраняем сообщение пользователя
        save_message(chat_id, "user", user_message)

        # Генерируем ответ
        answer = generate_answer(chat_id, user_message)

        # Сохраняем ответ ассистента
        save_message(chat_id, "assistant", answer)

        # Отправляем ответ в Telegram
        send_message(chat_id, answer)

    return "ok"
# ---------------------------------------------
# Функция отправки ответа в Telegram
# ---------------------------------------------
def send_message(chat_id, text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        resp = requests.post(url, json=payload)
        logger.info(f"Отправлено сообщение длиной {len(text)} символов")
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")

# ---------------------------------------------
# Точка входа
# ---------------------------------------------
if __name__ == "__main__":
    DatabasePool.initialize()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
