import os
import logging

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("grs-init")

DATABASE_URL = os.getenv("DATABASE_URL")

def init_db():
    if not DATABASE_URL:
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

        # Кэш новостей (по языку)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_cache (
                id BIGSERIAL PRIMARY KEY,
                language_code VARCHAR(10) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS news_cache_lang_created_idx
            ON news_cache (language_code, created_at DESC);
        """)

        # Daily snapshots news digests
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_digests (
                id BIGSERIAL PRIMARY KEY,
                language_code VARCHAR(10) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'ready',
                items_json JSONB,
                rendered_html TEXT,
                raw_response TEXT,
                model_used VARCHAR(64),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS news_digests_lang_created_idx
            ON news_digests (language_code, created_at DESC);
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_digest_pool (
                id BIGSERIAL PRIMARY KEY,
                language_code VARCHAR(10) NOT NULL,
                source_url TEXT NOT NULL,
                source_domain VARCHAR(255) NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                country VARCHAR(255),
                article_date_raw TEXT,
                article_date DATE,
                discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                is_active BOOLEAN NOT NULL DEFAULT FALSE
            );
        """)

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS news_digest_pool_lang_url_uidx
            ON news_digest_pool (language_code, source_url);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS news_digest_pool_active_idx
            ON news_digest_pool (language_code, is_active, article_date DESC, discovered_at DESC);
        """)

        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Таблица chat_history создана или уже существовала.")

    except Exception as e:
        logger.error(f"Ошибка при инициализации базы: {e}")

if __name__ == "__main__":
    init_db()
