import os
import logging
import requests
import time
import threading
import re
from datetime import datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError, version
from urllib.parse import urlparse

from dotenv import load_dotenv
from flask import Flask, request
from openai import OpenAI

from database import DatabasePool, get_db_connection

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
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = (os.getenv("OPENAI_MODEL") or "gpt-5-mini").strip()
OPENAI_NEWS_MODEL = (os.getenv("OPENAI_NEWS_MODEL") or "gpt-5").strip()
OPENAI_ENABLE_NEWS_FILTERS = os.getenv("OPENAI_ENABLE_NEWS_FILTERS", "false").lower() == "true"
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "globalrelocationsolutions_cz").lstrip("@")
OPENAI_FALLBACK_MODELS_RAW = os.getenv("OPENAI_FALLBACK_MODELS") or "gpt-5,gpt-4.1,gpt-4o"

try:
    OPENAI_TIMEOUT_SEC = float(os.getenv("OPENAI_TIMEOUT_SEC", "45"))
except ValueError:
    OPENAI_TIMEOUT_SEC = 45.0

client = OpenAI(api_key=OPENAI_API_KEY, timeout=OPENAI_TIMEOUT_SEC)

# ---------------------------------------------
# Тексты и настройки
# ---------------------------------------------
MAX_FREE_REQUESTS = 25
MAX_HISTORY_MESSAGES = 10
NEWS_CACHE_TTL_SEC = 24 * 60 * 60
TELEGRAM_MAX_MESSAGE_LEN = 4096
REQUEST_TIMEOUT_SEC = 15
PROCESSED_UPDATE_TTL_SEC = 10 * 60

processed_updates = {}
processed_updates_lock = threading.Lock()
active_news_jobs = set()
active_news_jobs_lock = threading.Lock()


def get_int_env(name, default):
    try:
        value = int(os.getenv(name, str(default)))
        return max(1, value)
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r, using default=%s", name, os.getenv(name), default)
        return default


NEWS_LOOKBACK_DAYS = get_int_env("NEWS_LOOKBACK_DAYS", 120)
NEWS_ALLOWED_DOMAINS_RAW = os.getenv("NEWS_ALLOWED_DOMAINS", "")
NEWS_SOURCE_URLS_RAW = os.getenv("NEWS_SOURCE_URLS", "")

DEFAULT_NEWS_SOURCE_PROFILES = [
    {
        "domain": "immigrantinvest.com",
        "label": "Immigrant Invest",
        "allowed_paths": [
            "/ru/blog/category/residence-permit/",
            "/ru/blog/category/citizenship/",
            "/ru/blog/category/permanent-residency/"
        ],
        "section_hints": ["ВНЖ", "Гражданство", "ПМЖ"],
        "positive_keywords": [
            "внж", "пмж", "гражданство", "золотая виза", "golden visa",
            "residence permit", "permanent residency", "digital nomad",
            "инвестиции", "натурализация", "воссоединение семьи"
        ],
        "negative_keywords": [
            "истории клиентов", "недвижимость для отдыха", "налоги", "банки",
            "медицина", "туризм", "путешествия", "рейтинг без события"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "espassport.pro",
        "label": "Первый иммиграционный центр",
        "allowed_paths": ["/news/"],
        "section_hints": ["Блог > Новости"],
        "positive_keywords": [
            "виза", "внж", "пмж", "гражданство", "золотая виза",
            "digital nomad", "иммиграц", "легализац", "релокац",
            "россиян", "граждан рф"
        ],
        "negative_keywords": [
            "роды", "работа в испании без привязки к правилам", "отзывы",
            "кейсы клиентов", "услуги компании", "гарантия", "стоимость услуг"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "confidencegroup.ru",
        "label": "Confidence Group",
        "allowed_paths": ["/info/articles/"],
        "section_hints": [
            "Статьи > Миграционные услуги",
            "Статьи > Визовая поддержка",
            "Статьи > Высококвалифицированные специалисты"
        ],
        "positive_keywords": [
            "виза", "внж", "рвп", "гражданство", "уведомлен", "консульств",
            "проживающих за рубежом", "иностранн", "миграционн", "закон",
            "мвд", "минтруд", "законопроект"
        ],
        "negative_keywords": [
            "патент на работу", "привлечение иностранных работников в рф",
            "кадровый учет", "сим-карта", "регистрация работодателя",
            "аккредитация", "въезд в рф без связи с релокантами"
        ],
        "requires_russian_relocator_angle": True,
    },
    {
        "domain": "pravo.ru",
        "label": "Право.ру",
        "allowed_paths": ["/news/", "/story/"],
        "section_hints": ["Законодательство", "Практика", "Международная практика"],
        "positive_keywords": [
            "внж", "второе гражданство", "гражданство", "уведомлен",
            "законопроект", "закон", "миграцион", "консульств", "проживающих за рубежом"
        ],
        "negative_keywords": [
            "корпоратив", "банкрот", "арбитраж", "налоговый спор",
            "реклама", "ip", "фарма", "ваканси"
        ],
        "requires_russian_relocator_angle": True,
    },
    {
        "domain": "rbc.ru",
        "label": "РБК",
        "allowed_paths": ["/politics/", "/society/", "/rbcfreenews/", "/economics/", "/radio/"],
        "section_hints": ["Политика", "Общество", "Экономика", "Радио"],
        "positive_keywords": [
            "внж", "гражданство", "золотая виза", "виза", "релокац",
            "россиян", "иностранц", "депортац", "вид на жительство",
            "второе гражданство", "миграцион"
        ],
        "negative_keywords": [
            "companies.rbc.ru", "pro.rbc.ru", "недвижимость без регуляторного изменения",
            "рынок жилья без визового эффекта", "криминал", "спорт"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "kommersant.ru",
        "label": "Коммерсантъ",
        "allowed_paths": ["/doc/"],
        "section_hints": ["Новости / doc"],
        "positive_keywords": [
            "внж", "второе гражданство", "гражданство", "уведомлен",
            "миграцион", "законопроект", "закон", "проживающих за рубежом"
        ],
        "negative_keywords": [
            "бизнес", "нефть", "спорт", "криминал", "рынки", "суд без миграционного сюжета"
        ],
        "requires_russian_relocator_angle": True,
    },
    {
        "domain": "dw.com",
        "label": "DW на русском",
        "allowed_paths": ["/ru/"],
        "section_hints": ["Иностранцы", "Грузия", "Европа", "Политика", "Общество"],
        "positive_keywords": [
            "внж", "гражданство", "виза", "депортац", "выдворен",
            "иностранц", "грузи", "латви", "финлянд", "европа",
            "россиян", "проживание", "легализац"
        ],
        "negative_keywords": [
            "культура", "медиа", "выборы без миграционного эффекта",
            "протесты без изменения правил пребывания"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "rus.err.ee",
        "label": "ERR на русском",
        "allowed_paths": ["/"],
        "section_hints": ["За рубежом", "Эстония"],
        "positive_keywords": [
            "внж", "гражданство", "миграционн", "квота", "депортац",
            "россиян", "иностранц", "эстони", "латви", "финлянд",
            "уведомлен", "вид на жительство"
        ],
        "negative_keywords": [
            "спорт", "культура", "экономика без миграции",
            "внутриполитическая новость без визового эффекта"
        ],
        "requires_event_language": True,
    },
]

GLOBAL_NEWS_POSITIVE_KEYWORDS = [
    "виза", "визовый", "внж", "пмж", "гражданство", "второе гражданство",
    "residence permit", "permanent residency", "citizenship", "digital nomad",
    "golden visa", "натурализация", "репатриация", "воссоединение семьи",
    "убежище", "санкции", "консульство", "уведомление", "легализация"
]

GLOBAL_NEWS_NEGATIVE_KEYWORDS = [
    "криминал", "происшествие", "катастрофа", "спорт", "вакансии",
    "рынок недвижимости без изменения правил", "туризм без миграционного эффекта",
    "реклама услуг", "кейсы клиентов", "отзывы", "маркетинговый материал"
]


def get_package_version(name):
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


def format_limit_reached_message(lang):
    return TEXTS[lang]["limit_reached"].format(
        max=MAX_FREE_REQUESTS,
        manager_username=f"@{MANAGER_USERNAME}"
    )


print(
    "Startup config "
    f"openai_sdk={get_package_version('openai')} "
    f"openai_model={OPENAI_MODEL} "
    f"openai_news_model={OPENAI_NEWS_MODEL or '<empty>'} "
    f"openai_fallback_models={OPENAI_FALLBACK_MODELS_RAW} "
    f"news_lookback_days={NEWS_LOOKBACK_DAYS} "
    f"openai_timeout_sec={OPENAI_TIMEOUT_SEC} "
    f"news_filters_enabled={OPENAI_ENABLE_NEWS_FILTERS}",
    flush=True,
)


TEXTS = {
    "ru": {
        "welcome": "Добро пожаловать в GRS Bot! 🌍\nПожалуйста, выберите язык:",
        "menu_title": "Главное меню:",
        "btn_news": "📰 Актуальные новости",
        "btn_contact": "📝 Написать менеджеру",
        "btn_limit": "📊 Проверить лимит",
        "contact_info": "Связаться с менеджером GRS: @globalrelocationsolutions_cz\nБоты и автоматизация: @kovachinfo",
        "limit_info": "Использовано запросов: {count} из {max}.",
        "limit_reached": "🚫 Вы исчерпали лимит бесплатных запросов ({max}).\nПожалуйста, свяжитесь с менеджером для консультации: {manager_username}",
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
        "contact_info": "Contact GRS manager: @globalrelocationsolutions_cz\nBots & automation: @kovachinfo",
        "limit_info": "Requests used: {count} of {max}.",
        "limit_reached": "🚫 You have reached the free request limit ({max}).\nPlease contact the manager: {manager_username}",
        "lang_selected": "🇬🇧 Language set: English",
        "searching": "🔍 Searching...",
        "error": "❌ Service error.",
        "rate_limited": "⚠️ Request is temporarily unavailable. Please try again in a minute.",
        "btn_ru": "🇷🇺 Русский",
        "btn_en": "🇬🇧 English"
    }
}


def parse_config_list(raw_value):
    return [item.strip() for item in re.split(r"[\n,;]+", raw_value or "") if item.strip()]


def get_fallback_models():
    return parse_config_list(OPENAI_FALLBACK_MODELS_RAW)


def cleanup_processed_updates(now_ts=None):
    now_ts = now_ts or time.time()
    expired = [
        update_id for update_id, ts in processed_updates.items()
        if now_ts - ts > PROCESSED_UPDATE_TTL_SEC
    ]
    for update_id in expired:
        processed_updates.pop(update_id, None)


def is_duplicate_update(update_id):
    if update_id is None:
        return False

    now_ts = time.time()
    with processed_updates_lock:
        cleanup_processed_updates(now_ts)
        if update_id in processed_updates:
            return True
        processed_updates[update_id] = now_ts
    return False


def normalize_domain(value):
    if not value:
        return []

    candidate = value.strip().lower()
    if "://" not in candidate:
        candidate = f"https://{candidate}"

    parsed = urlparse(candidate)
    host = (parsed.netloc or parsed.path).strip().lower()
    host = host.split("/")[0].split(":")[0]
    if not host:
        return []

    domains = [host]
    if host.startswith("www."):
        domains.append(host[4:])
    return domains


def get_news_source_profiles():
    profiles = list(DEFAULT_NEWS_SOURCE_PROFILES)
    allowed_domains = get_allowed_news_domains_from_env()

    for domain in allowed_domains:
        if any(profile["domain"] == domain for profile in profiles):
            continue
        profiles.append(
            {
                "domain": domain,
                "label": domain,
                "allowed_paths": ["/"],
                "section_hints": [],
                "positive_keywords": [],
                "negative_keywords": [],
            }
        )
    return profiles


def get_allowed_news_domains_from_env():
    domains = []

    for item in parse_config_list(NEWS_ALLOWED_DOMAINS_RAW):
        domains.extend(normalize_domain(item))

    for item in parse_config_list(NEWS_SOURCE_URLS_RAW):
        domains.extend(normalize_domain(item))

    unique = []
    seen = set()
    for domain in domains:
        if domain not in seen:
            seen.add(domain)
            unique.append(domain)
    return unique


def get_allowed_news_domains():
    domains = [profile["domain"] for profile in get_news_source_profiles()]
    unique = []
    seen = set()
    for domain in domains:
        if domain not in seen:
            seen.add(domain)
            unique.append(domain)
    return unique


def build_source_profile_prompt(lang, compact=False):
    profiles = get_news_source_profiles()
    lines = []

    for profile in profiles:
        path_hint = ", ".join(profile.get("allowed_paths", [])) or "/"
        section_hint = ", ".join(profile.get("section_hints", [])) or "-"
        positive_keywords = ", ".join(profile.get("positive_keywords", [])[:8]) or "-"
        negative_keywords = ", ".join(profile.get("negative_keywords", [])[:6]) or "-"

        if compact:
            if lang == "ru":
                lines.append(
                    f"- {profile['label']} ({profile['domain']}): path {path_hint}; разделы {section_hint}."
                )
            else:
                lines.append(
                    f"- {profile['label']} ({profile['domain']}): path {path_hint}; sections {section_hint}."
                )
            continue

        if lang == "ru":
            rule_parts = [
                f"- {profile['label']} ({profile['domain']}):",
                f"  допустимые URL/path: {path_hint};",
                f"  разделы/рубрики: {section_hint};",
                f"  позитивные ключевые слова: {positive_keywords};",
                f"  исключать: {negative_keywords}."
            ]
            if profile.get("requires_event_language"):
                rule_parts.append("  Используй только статьи о конкретном изменении правил, закона или процедуры.")
            if profile.get("requires_russian_relocator_angle"):
                rule_parts.append("  Бери материал только если он явно касается россиян за рубежом или планирующих переезд.")
        else:
            rule_parts = [
                f"- {profile['label']} ({profile['domain']}):",
                f"  allowed URL/path: {path_hint};",
                f"  sections/topics: {section_hint};",
                f"  positive keywords: {positive_keywords};",
                f"  exclude: {negative_keywords}."
            ]
            if profile.get("requires_event_language"):
                rule_parts.append("  Use only articles about a concrete rule, law, or procedure change.")
            if profile.get("requires_russian_relocator_angle"):
                rule_parts.append("  Keep only items clearly relevant to Russians abroad or planning relocation.")

        lines.append(" ".join(rule_parts))

    return "\n".join(lines)


def build_news_prompt(lang, compact=False):
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=NEWS_LOOKBACK_DAYS)
    allowed_domains = get_allowed_news_domains()
    source_profiles = build_source_profile_prompt(lang, compact=compact)
    positive_keywords = ", ".join(GLOBAL_NEWS_POSITIVE_KEYWORDS)
    negative_keywords = ", ".join(GLOBAL_NEWS_NEGATIVE_KEYWORDS)

    if lang == "ru":
        domain_rule = (
            "Используй эти домены как основной пул источников: "
            f"{', '.join(allowed_domains)}. Старайся брать новости минимум с 3 разных доменов, "
            "если по теме есть достаточно материалов. Не давай больше 2 пунктов с одного домена, "
            "если можно собрать более разнообразную выборку. Если релевантных материалов мало, "
            "прямо напиши, что выборка ограничена указанными источниками."
            if allowed_domains else
            "Используй несколько независимых новостных или официальных источников, а не один сайт."
        )
        return (
            "Подготовь сводку из 6-8 пунктов только по миграционному праву, миграционной политике и "
            "процедурам легализации, которые важны для релокантов из России. "
            f"Период публикации: с {start_date.isoformat()} по {today.isoformat()}. "
            "Включай только новости об изменениях виз, ВНЖ/ПМЖ, гражданства, убежища, трудовой или "
            "предпринимательской миграции, учебы, digital nomad программ, воссоединения семьи, "
            "репатриации, консульских и санкционных ограничений, если они влияют на выезд, въезд, "
            "легализацию или проживание граждан РФ за рубежом. "
            "Исключай криминал, происшествия, экономику, спорт, вакансии, общую внутреннюю политику без "
            "миграционного эффекта, а также новости, не связанные с релокантами из РФ. "
            f"{domain_rule} "
            "Сначала отфильтруй кандидатов по источнику, URL/path, рубрике и ключевым словам. "
            f"Общие позитивные ключевые слова: {positive_keywords}. "
            f"Общие негативные ключевые слова: {negative_keywords}. "
            "Статические гайды, SEO-обзоры, маркетинговые статьи, рейтинги и evergreen-материалы не включай, "
            "если в них нет конкретного свежего изменения закона, процедуры или официального режима. "
            + (
                "Короткий список допустимых источников и разделов:\n"
                if compact else
                "Профили источников и допустимые разделы:\n"
            )
            + f"{source_profiles}\n"
            "Для каждого пункта укажи: дату, страну, краткое объяснение, почему это важно релокантам из РФ, "
            "и источник в формате: Источник: Название статьи, домен. "
            "Формат ответа: простой текст без Markdown, нумерованный список вида "
            "\"1) Заголовок — дата. Короткое описание. Почему важно: ... Источник: Название статьи, домен\". "
            "Пиши компактно: каждый пункт максимум 2 коротких предложения после заголовка, примерно на 20% короче обычной новости, "
            "без потери ключевого смысла. Не дублируй один и тот же сюжет, страну+событие или один и тот же источник по одной теме. "
            "Не добавляй вступление, преамбулу, общий абзац перед списком или заключение после списка. Выведи только нумерованный список. "
            "Если статья не проходит хотя бы по двум позитивным признакам или попадает под негативные признаки, не включай ее. "
            "Не используй Wikipedia или любые вики-источники. Не давай прямые ссылки. "
            "Если релевантных новостей меньше 6, верни меньше и явно сообщи, что значимых материалов мало."
        )

    domain_rule = (
        "Use these domains as the primary source pool: "
        f"{', '.join(allowed_domains)}. Try to use at least 3 different domains when enough relevant material exists. "
        "Do not use more than 2 items from the same domain if a more diverse selection is available. "
        "If the pool is too limited, explicitly say so instead of filling the list with unrelated items."
        if allowed_domains else
        "Use several independent news or official sources instead of relying on a single site."
    )
    return (
        "Prepare a 6-8 item summary only about migration law, migration policy, and legal status changes "
        "relevant to Russian relocators. "
        f"Publication date range: {start_date.isoformat()} to {today.isoformat()}. "
        "Include only changes to visas, residence permits, permanent residence, citizenship, asylum, work or "
        "business migration, study routes, digital nomad programs, family reunion, repatriation, and consular "
        "or sanctions-related restrictions if they affect travel, entry, legalization, or residence for Russian "
        "citizens abroad. Exclude crime, accidents, economy, sports, vacancies, generic domestic politics, and "
        "anything not materially relevant to Russian relocators. "
        f"{domain_rule} "
        "First filter candidates by source, URL/path, section, and keywords. "
        f"Global positive keywords: {positive_keywords}. "
        f"Global negative keywords: {negative_keywords}. "
        "Exclude evergreen guides, SEO explainers, service-marketing pages, rankings, and static overviews unless "
        "they clearly describe a fresh law, policy, or procedure change in the target date range. "
        + (
            "Compact source list and allowed sections:\n"
            if compact else
            "Source profiles and allowed sections:\n"
        )
        + f"{source_profiles}\n"
        "For each item provide the date, country, a short explanation of why it matters to Russian relocators, "
        "and a source in the format: Source: Article title, domain. "
        "Answer in plain text without Markdown as a numbered list like "
        "\"1) Title — date. Short description. Why it matters: ... Source: Article title, domain\". "
        "Be concise: each item should be at most 2 short sentences after the title, about 20% shorter than a typical news brief, "
        "without losing the key meaning. Do not include duplicate events, duplicate country+event pairs, or the same story twice. "
        "Do not add any introduction, preamble, summary paragraph before the list, or closing paragraph after the list. Output only the numbered list. "
        "If an article does not satisfy at least two positive signals or hits negative signals, exclude it. "
        "Do not use Wikipedia or other wiki sources. Do not include direct links. "
        "If fewer than 6 relevant items exist, return fewer and explicitly say the pool was limited."
    )

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
                    if is_invalid_cached_news(row["content"]):
                        logger.warning("Ignoring invalid cached news for lang=%s", lang)
                        print(f"Ignoring invalid cached news for lang={lang}", flush=True)
                        return None
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

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1 — \2", text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"^\s*[-*]\s+", "- ", text, flags=re.M)
    text = re.sub(r"\b([a-z0-9.-]+\.[a-z]{2,})\.\s+\(\1\b", r"\1 (", text, flags=re.I)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_news_items(text):
    if not text:
        return []

    normalized = sanitize_plain_text(text)
    normalized = re.sub(r"(?<!\n)\s+(?=\d+[\).]\s+)", "\n", normalized)
    lines = [ln.strip() for ln in normalized.splitlines() if ln.strip()]
    items = []
    current = []
    seen_numbered_item = False
    for ln in lines:
        if re.match(r"^\d+[\).]\s+", ln):
            seen_numbered_item = True
            if current:
                items.append(" ".join(current).strip())
                current = []
        elif not seen_numbered_item:
            continue
        current.append(ln)

    if current:
        items.append(" ".join(current).strip())

    return items


def normalize_news_item_key(item_text):
    text = re.sub(r"^\d+[\).]\s*", "", item_text.strip().lower())
    title = re.split(r"\s+почему важно:|\s+источник:", text, maxsplit=1)[0]
    title = re.sub(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", " ", title)
    title = re.sub(r"\b\d{4}\b", " ", title)
    title = re.sub(r"[^a-zа-я0-9]+", " ", title, flags=re.I)
    title = re.sub(r"\s+", " ", title).strip()
    words = title.split()
    return " ".join(words[:12])


def extract_news_item_domain(item_text):
    match = re.search(r"(?:источник|source):.*?([a-z0-9.-]+\.[a-z]{2,})", item_text, flags=re.I)
    if match:
        return match.group(1).lower()
    fallback = re.findall(r"\b([a-z0-9.-]+\.[a-z]{2,})\b", item_text, flags=re.I)
    return fallback[-1].lower() if fallback else ""


def dedupe_news_items(items):
    deduped = []
    seen = set()
    domain_counts = {}

    for item in items:
        key = normalize_news_item_key(item)
        if not key or key in seen:
            continue
        domain = extract_news_item_domain(item)
        if domain and domain_counts.get(domain, 0) >= 2 and len(deduped) >= 4:
            continue
        seen.add(key)
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        deduped.append(item)

    return deduped[:8]


def compress_news_item_text(item_text):
    text = item_text
    sentence_patterns = [
        r"[^.!?]*финальн[^.!?]*решени[^.!?]*[.!?]?",
        r"[^.!?]*решени[яе]\s+принима(ет|ют)[^.!?]*[.!?]?",
        r"[^.!?]*компетентн[^.!?]*орган[^.!?]*[.!?]?",
    ]
    for pattern in sentence_patterns:
        text = re.sub(pattern, " ", text, flags=re.I)

    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"\s+([,.;:])", r"\1", text)
    return text.strip()


def enforce_news_spacing(text):
    items = [compress_news_item_text(item) for item in dedupe_news_items(parse_news_items(text))]
    if not items:
        return text.strip()
    return "\n\n".join(items).strip()

def needs_news_retry(text):
    if not text:
        return True
    lower = text.lower()
    if "wikipedia.org" in lower or "wikipedia" in lower or "wiki" in lower:
        return True
    return False


def is_service_message(text, lang):
    return text in {TEXTS[lang]["error"], TEXTS[lang]["rate_limited"]}


def is_invalid_cached_news(text):
    if not text:
        return True

    error_markers = [
        TEXTS["ru"]["error"],
        TEXTS["ru"]["rate_limited"],
        TEXTS["en"]["error"],
        TEXTS["en"]["rate_limited"],
        "service error",
        "rate limited",
        "rate limit",
    ]
    lower = text.lower()
    return any(marker.lower() in lower for marker in error_markers)


def build_web_search_tool(news_mode=False, include_filters=True):
    tool = {"type": "web_search", "search_context_size": "medium"}
    if news_mode and include_filters and OPENAI_ENABLE_NEWS_FILTERS:
        allowed_domains = get_allowed_news_domains()
        if allowed_domains:
            tool["filters"] = {"allowed_domains": allowed_domains}
    return tool


def get_response_models(news_mode=False):
    candidates = []
    preferred = [OPENAI_MODEL]
    if news_mode:
        preferred = [OPENAI_NEWS_MODEL, OPENAI_MODEL]

    for model in preferred + get_fallback_models():
        if model and model not in candidates:
            candidates.append(model)
    return candidates


def get_tool_variants(news_mode=False):
    if not news_mode:
        return [("default", build_web_search_tool(news_mode=False))]

    if OPENAI_ENABLE_NEWS_FILTERS:
        return [
            ("filtered", build_web_search_tool(news_mode=True, include_filters=True)),
            ("unfiltered", build_web_search_tool(news_mode=True, include_filters=False)),
        ]

    return [("unfiltered", build_web_search_tool(news_mode=True, include_filters=False))]


def create_response(messages, lang="ru", news_mode=False):
    last_error = None

    for variant_name, web_search_tool in get_tool_variants(news_mode=news_mode):
        allowed_domains = web_search_tool.get("filters", {}).get("allowed_domains", [])

        for model in get_response_models(news_mode=news_mode):
            try:
                request_payload = {
                    "model": model,
                    "input": messages,
                    "tools": [web_search_tool]
                }
                msg = (
                    "OpenAI request start "
                    f"model={model} news_mode={news_mode} variant={variant_name} "
                    f"messages={len(messages)} domains={len(allowed_domains)} "
                    f"last_user_chars={len(str(messages[-1].get('content', ''))) if messages else 0}"
                )
                logger.info(msg)
                print(msg, flush=True)
                response = client.responses.create(**request_payload)
                return response, model
            except Exception as exc:
                last_error = exc
                err_msg = (
                    "OpenAI request failed "
                    f"model={model} news_mode={news_mode} variant={variant_name} "
                    f"exc_type={exc.__class__.__name__} domains={len(allowed_domains)} err={exc}"
                )
                logger.exception(err_msg)
                print(err_msg, flush=True)

    raise last_error

def escape_html(text):
    if text is None:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def bold_title(item_text):
    numbered_prefix = ""
    body = item_text.strip()

    match = re.match(r"^(\d+[\).]\s+)(.+)$", body)
    if match:
        numbered_prefix = match.group(1)
        body = match.group(2).strip()

    split_match = re.match(
        r"^(.*?)(\s+[—-]\s+\d{1,2}[./ ]\d{1,2}[./ ]\d{2,4}\.?.*)$",
        body
    )
    if split_match:
        title = split_match.group(1).strip()
        rest = split_match.group(2).strip()
        return f"{numbered_prefix}<b>{title}</b> {rest}".strip()

    for marker in [" Почему важно:", " Источник:"]:
        if marker in body:
            title, rest = body.split(marker, 1)
            return f"{numbered_prefix}<b>{title.strip()}</b>{marker}{rest.strip()}".strip()

    for delim in [" — ", " - "]:
        if delim in body:
            title, rest = body.split(delim, 1)
            return f"{numbered_prefix}<b>{title.strip()}</b>{delim}{rest.strip()}".strip()

    return f"{numbered_prefix}<b>{body}</b>".strip()

def format_news_html(text, lang):
    header = (
        "🧭 <b>Новости для релокантов из России</b>"
        if lang == "ru"
        else "🧭 <b>News for Russian Relocators</b>"
    )
    footer = (
        "Примечание: окончательные решения по визам и статусам принимают государственные органы соответствующих стран."
        if lang == "ru"
        else "Note: final decisions on visas and status matters are made by the relevant state authorities."
    )
    if not text:
        return f"{header}\n\n{escape_html(footer)}"

    items = dedupe_news_items(parse_news_items(text))

    formatted = []
    for raw in items:
        escaped = escape_html(raw)
        formatted.append(bold_title(escaped))

    body = "\n\n".join(formatted) if formatted else escape_html(text)
    return f"{header}\n\n{body}\n\n{escape_html(footer)}".strip()


def split_message_chunks(text, limit=TELEGRAM_MAX_MESSAGE_LEN):
    if not text:
        return [""]

    if len(text) <= limit:
        return [text]

    chunks = []
    current = []
    current_len = 0

    for line in text.splitlines(keepends=True):
        line_len = len(line)
        if line_len > limit:
            if current:
                chunks.append("".join(current).rstrip())
                current = []
                current_len = 0

            start = 0
            while start < line_len:
                end = min(start + limit, line_len)
                chunks.append(line[start:end].rstrip())
                start = end
            continue

        if current_len + line_len > limit:
            chunks.append("".join(current).rstrip())
            current = [line]
            current_len = line_len
            continue

        current.append(line)
        current_len += line_len

    if current:
        chunks.append("".join(current).rstrip())

    return chunks

# ---------------------------------------------
# Генерация ответа (Responses API + web_search)
# ---------------------------------------------
def generate_answer(chat_id, user_message, lang="ru", use_history=True, news_mode=False):
    history = load_history(chat_id, limit=MAX_HISTORY_MESSAGES) if use_history else []

    system_prompt = """Ты — AI-консультант по вопросам миграционного права и ВНЖ/ПМЖ в странах ЕС.

Твоя задача — предоставлять актуальную, проверяемую информацию,
учитывая, что миграционное законодательство часто меняется.

Обязательные правила:
- Никогда не гарантируй получение ВНЖ или гражданства.
- Всегда указывай, что финальное решение принимает государственный орган.
- Если информация может быть устаревшей — прямо укажи это.
- При недостатке данных задавай уточняющие вопросы.
- Не предлагай выполнить какие-то действия, ты - информационная служба.
- Не делай преамбул и пост-скриптумов, переходи сразу к сути

Стиль:
- профессиональный
- нейтральный
- без эмоциональных оценок
- без давления

Коммерческая часть:
- Ты можешь ненавязчиво упоминать, что существуют профессиональные услуги сопровождения,
  ТОЛЬКО если это логично вытекает из запроса пользователя.
- Никогда не начинай ответ с рекламы.
- Основной фокус — полезная информация для пользователя.
"""
    if news_mode:
        system_prompt += "\nФормат ответа: простой текст без Markdown."

    messages = [{"role": "system", "content": system_prompt}]
    for row in history:
        messages.append({"role": row["role"], "content": row["content"]})
    messages.append({"role": "user", "content": user_message})
    gen_msg = (
        "Generate answer "
        f"chat_id={chat_id} lang={lang} news_mode={news_mode} "
        f"use_history={use_history} history_messages={len(history)} "
        f"prompt_chars={len(user_message or '')}"
    )
    logger.info(gen_msg)
    print(gen_msg, flush=True)

    try:
        response, model_used = create_response(messages, lang=lang, news_mode=news_mode)
        content = (response.output_text or "").strip()
        content_l = content.lower()

        if (
            "нет доступа" in content_l
            or "no access" in content_l
            or "don't have access" in content_l
            or "do not have access" in content_l
        ):
            retry_rule = (
                "Пожалуйста, используй web_search и не упоминай ограничения доступа."
                if lang == "ru"
                else "Please use web_search and do not mention access limitations."
            )
            retry_messages = messages + [{"role": "user", "content": retry_rule}]
            retry, _ = create_response(retry_messages, lang=lang, news_mode=news_mode)
            return (retry.output_text or "").strip()

        if news_mode and needs_news_retry(content):
            retry_rule = (
                "Не используй Wikipedia/вики-источники. Дай только релевантные миграционные новости "
                "для релокантов из РФ и не включай нерелевантный общий новостной шум."
                if lang == "ru"
                else "Do not use Wikipedia/wiki sources. Only return migration news relevant to "
                     "Russian relocators and exclude generic news noise."
            )
            retry_messages = messages + [{"role": "user", "content": retry_rule}]
            retry, _ = create_response(retry_messages, lang=lang, news_mode=news_mode)
            content = (retry.output_text or "").strip()

        logger.info("OpenAI response completed with model=%s news_mode=%s", model_used, news_mode)
        return sanitize_plain_text(content) if news_mode else content

    except Exception as e:
        err_text = str(e)
        logger.exception("Error OpenAI (Responses API): %s", err_text)
        print(f"Error OpenAI (Responses API): {err_text}", flush=True)

        last_fb_error = None
        response_models = get_response_models(news_mode=news_mode)
        fallback_candidates = response_models[1:] if len(response_models) > 1 else response_models
        for fallback_model in fallback_candidates:
            try:
                fb = client.responses.create(model=fallback_model, input=messages)
                fb_text = (fb.output_text or "").strip()
                return sanitize_plain_text(fb_text) if news_mode else fb_text
            except Exception as fb_err:
                last_fb_error = fb_err
                logger.exception("Fallback error model=%s: %s", fallback_model, fb_err)
                print(f"Fallback error model={fallback_model}: {fb_err}", flush=True)

        if "rate_limit" in err_text or "token" in err_text.lower():
            return TEXTS[lang]["rate_limited"]
        if last_fb_error:
            return TEXTS[lang]["error"]
        return TEXTS[lang]["error"]

# ---------------------------------------------
# Отправка сообщений (с клавиатурой)
# ---------------------------------------------
def send_message(chat_id, text, keyboard=None, parse_mode=None):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        chunks = split_message_chunks(text)

        for index, chunk in enumerate(chunks):
            payload = {"chat_id": chat_id, "text": chunk}

            if keyboard and index == 0:
                payload["reply_markup"] = keyboard
            if parse_mode:
                payload["parse_mode"] = parse_mode

            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
            if not resp.ok:
                logger.error("Send Error: %s %s", resp.status_code, resp.text)
                break
    except Exception as e:
        logger.error(f"Send Error: {e}")

def send_chat_action(chat_id, action="typing"):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendChatAction"
        payload = {"chat_id": chat_id, "action": action}
        resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
        if not resp.ok:
            if resp.status_code == 429:
                logger.warning("Chat Action rate limited: %s", resp.text)
            else:
                logger.error("Chat Action Error: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error(f"Chat Action Error: {e}")

def run_typing(chat_id, stop_event, interval_sec=4):
    while not stop_event.is_set():
        send_chat_action(chat_id, "typing")
        stop_event.wait(interval_sec)


def make_news_job_key(chat_id, lang):
    return f"{chat_id}:{lang}"


def process_news_request(chat_id, lang, trigger_text):
    job_key = make_news_job_key(chat_id, lang)
    stop_event = threading.Event()
    typing_thread = threading.Thread(
        target=run_typing,
        args=(chat_id, stop_event),
        daemon=True
    )
    typing_thread.start()

    try:
        try:
            cached = get_cached_news(lang)
            if cached:
                ans = cached
            else:
                raw_ans = generate_answer(
                    chat_id,
                    build_news_prompt(lang),
                    lang,
                    use_history=False,
                    news_mode=True
                )
                if is_service_message(raw_ans, lang):
                    compact_prompt = build_news_prompt(lang, compact=True)
                    print(
                        "News retry with compact prompt "
                        f"chat_id={chat_id} lang={lang} prompt_chars={len(compact_prompt)}",
                        flush=True,
                    )
                    raw_ans = generate_answer(
                        chat_id,
                        compact_prompt,
                        lang,
                        use_history=False,
                        news_mode=True
                    )
                if is_service_message(raw_ans, lang):
                    ans = raw_ans
                else:
                    normalized_news = enforce_news_spacing(sanitize_plain_text(raw_ans))
                    ans = format_news_html(normalized_news, lang)
                    save_cached_news(lang, ans)
        except Exception:
            logger.exception("Unhandled error in process_news_request chat_id=%s lang=%s", chat_id, lang)
            ans = TEXTS[lang]["error"]

        save_message(chat_id, "user", trigger_text)
        save_message(chat_id, "assistant", ans)
        send_message(chat_id, ans, parse_mode="HTML")
    finally:
        stop_event.set()
        with active_news_jobs_lock:
            active_news_jobs.discard(job_key)

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
    if TELEGRAM_WEBHOOK_SECRET:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret != TELEGRAM_WEBHOOK_SECRET:
            logger.warning("Webhook rejected due to invalid secret token.")
            return "forbidden", 403

    data = request.get_json(silent=True)
    if not data or "message" not in data:
        return "ok"

    update_id = data.get("update_id")
    if is_duplicate_update(update_id):
        logger.info("Skipping duplicate Telegram update_id=%s", update_id)
        print(f"Skipping duplicate Telegram update_id={update_id}", flush=True)
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

    t = TEXTS[lang]
    ru_t = TEXTS["ru"]
    en_t = TEXTS["en"]
    skip_search_notice = False

    # 2. Обработка команд и кнопок
    if text == "/start":
        send_message(chat_id, t["welcome"], get_lang_keyboard())
        return "ok"

    if text == "/refresh_news":
        clear_cached_news(lang)
        send_message(chat_id, t["searching"])
        text = t["btn_news"]
        skip_search_notice = True

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
            send_message(chat_id, format_limit_reached_message(lang))
            return "ok"

        job_key = make_news_job_key(chat_id, lang)
        with active_news_jobs_lock:
            if job_key in active_news_jobs:
                logger.info("News job already active for %s", job_key)
                print(f"News job already active for {job_key}", flush=True)
                return "ok"
            active_news_jobs.add(job_key)

        if not skip_search_notice:
            send_message(chat_id, t["searching"])
        increment_request_count(chat_id)

        worker = threading.Thread(
            target=process_news_request,
            args=(chat_id, lang, text),
            daemon=True
        )
        worker.start()
        return "ok"

    # 3. Обработка обычного текстового запроса (ChatGPT)
    
    # Проверка лимита
    if user['request_count'] >= MAX_FREE_REQUESTS and not user.get('is_premium'):
        send_message(chat_id, format_limit_reached_message(lang))
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
