import psycopg2
import os
from psycopg2.extras import RealDictCursor
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("grs-init")

DATABASE_URL = os.getenv("DATABASE_URL")

# Manual fallback for .env if load_dotenv failed
if not DATABASE_URL and os.path.exists(".env"):
    print("Trying manual .env parse...")
    with open(".env", "r") as f:
        for line in f:
            if line.startswith("DATABASE_URL="):
                DATABASE_URL = line.strip().split("=", 1)[1]
                print(f"Manually loaded DATABASE_URL: {DATABASE_URL[:10]}...")
                break

print(f"CWD: {os.getcwd()}")
print(f"DATABASE_URL: {DATABASE_URL}")

def init_db():
    if not DATABASE_URL:
        # Попытка жесткого чтения для отладки (не рекомендуется в проде, но здесь нужно починить)
        print("Files in dir:", os.listdir())
        raise RuntimeError("❌ DATABASE_URL не задан")

    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cur = conn.cursor()

        # Таблица истории
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id BIGSERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                role VARCHAR(16) NOT NULL CHECK (role IN ('user','assistant','system')),
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # Индекс для быстрых выборок
        cur.execute("""
            CREATE INDEX IF NOT EXISTS chat_history_chat_created_idx
            ON chat_history (chat_id, created_at DESC);
        """)

        # Таблица пользователей
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id BIGINT PRIMARY KEY,
                language_code VARCHAR(10) DEFAULT 'ru',
                request_count INT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                is_premium BOOLEAN DEFAULT FALSE
            );
        """)

        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Таблица chat_history создана или уже существовала.")

    except Exception as e:
        logger.error(f"Ошибка при инициализации базы: {e}")

if __name__ == "__main__":
    init_db()
