import os
import logging
import requests
import time
import threading
import re
import json
from datetime import datetime, timedelta, timezone, date
from importlib.metadata import PackageNotFoundError, version
from urllib.parse import urlparse

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from openai import OpenAI
from psycopg2.extras import Json

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
OPENAI_NEWS_MODEL = (os.getenv("OPENAI_NEWS_MODEL") or "gpt-4.1").strip()
OPENAI_ENABLE_NEWS_FILTERS = os.getenv("OPENAI_ENABLE_NEWS_FILTERS", "false").lower() == "true"
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "globalrelocationsolutions_cz").lstrip("@")
OPENAI_FALLBACK_MODELS_RAW = os.getenv("OPENAI_FALLBACK_MODELS") or "gpt-5,gpt-4.1,gpt-4o"
NEWS_CRON_TOKEN = os.getenv("NEWS_CRON_TOKEN", "")
NEWS_ADMIN_CHAT_ID = int(os.getenv("NEWS_ADMIN_CHAT_ID", "1111827435"))

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
TARGET_NEWS_ITEMS = 10
CANDIDATE_NEWS_ITEMS = 16
MAX_NEWS_PER_DOMAIN = 2
READY_NEWS_MIN_ITEMS = 10

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
    {
        "domain": "astons.com",
        "label": "Astons",
        "allowed_paths": ["/ru/blog/"],
        "section_hints": ["Блог", "ВНЖ за инвестиции", "Гражданство", "Golden Visa"],
        "positive_keywords": [
            "внж", "пмж", "гражданство", "золотая виза", "golden visa",
            "инвестиции", "residence permit", "citizenship", "digital nomad",
            "россиян", "белорусов"
        ],
        "negative_keywords": [
            "услуги компании", "кейсы клиентов", "консультация", "маркетинговый материал",
            "недвижимость без изменения программы", "налоги без миграционного эффекта"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "iworld.com",
        "label": "iWorld",
        "allowed_paths": ["/ru/blog/"],
        "section_hints": ["Блог", "ВНЖ", "Гражданство за инвестиции", "Digital Nomad"],
        "positive_keywords": [
            "внж", "гражданство", "золотая виза", "digital nomad",
            "residence permit", "citizenship by investment", "инвестиции",
            "оаэ", "турция", "хорватия"
        ],
        "negative_keywords": [
            "услуги компании", "кейсы клиентов", "отзывы", "реклама",
            "общий гид без свежего изменения", "сравнение программ без новостного повода"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "migron.ru",
        "label": "Migron",
        "allowed_paths": ["/news/", "/articles/"],
        "section_hints": ["Новости", "Миграционное законодательство", "Россия и СНГ"],
        "positive_keywords": [
            "миграцион", "внж", "рвп", "гражданство", "цифровизац",
            "контроль", "статистика мигрантов", "закон", "мвд", "россия"
        ],
        "negative_keywords": [
            "общий справочник", "услуги компании", "реклама", "трудоустройство без правового изменения",
            "маркетинговый материал"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "passportivity.com",
        "label": "Passportivity",
        "allowed_paths": ["/ru/"],
        "section_hints": ["Гражданство", "ВНЖ за инвестиции", "Digital Nomad"],
        "positive_keywords": [
            "гражданство", "внж", "золотая виза", "паспорт", "инвестиции",
            "digital nomad", "citizenship", "residence permit", "россиян"
        ],
        "negative_keywords": [
            "общий гид", "маркетинговый материал", "услуги компании", "реклама",
            "сравнение без новостного события"
        ],
        "requires_event_language": True,
    },
    {
        "domain": "visa-digital-nomad.com",
        "label": "Visa Digital Nomad",
        "allowed_paths": ["/ru/"],
        "section_hints": ["Digital Nomad Visa", "Ежемесячные дайджесты", "Изменения программ"],
        "positive_keywords": [
            "digital nomad", "цифровой кочевник", "виза", "внж",
            "ежемесячный дайджест", "изменения программы", "испания", "казахстан"
        ],
        "negative_keywords": [
            "общий гайд без изменений", "реклама", "услуги компании", "маркетинговый материал"
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
    source_profiles = build_source_profile_prompt(lang, compact=True)

    if lang == "ru":
        domain_rule = (
            "Работай только по этому пулу доменов: "
            f"{', '.join(allowed_domains)}. Сначала попробуй собрать подборку с разных сайтов и разных стран."
            if allowed_domains else
            "Используй несколько независимых источников, а не один сайт."
        )
        return (
            f"Подготовь сводку из {TARGET_NEWS_ITEMS} новостей для релокантов из России. "
            f"Период публикации: с {start_date.isoformat()} по {today.isoformat()}. "
            "Темы: визы, ВНЖ/ПМЖ, гражданство, правила въезда, трудовая и учебная миграция, digital nomad, "
            "воссоединение семьи, легализация, консульские ограничения. "
            "Приоритет: страны за пределами РФ. Новости из России включай только если они прямо влияют на тех, "
            "кто уже живет за рубежом или собирается переезжать. "
            f"{domain_rule} "
            "Не включай сухие справки, вечнозеленые гайды, рекламу услуг, криминал, спорт, вакансии, общую политику "
            "без прямого миграционного эффекта. "
            "Если по одной стране есть два сильных изменения, это допустимо, но в целом сначала стремись к разным странам. "
            "Короткий список допустимых источников и разделов:\n"
            + f"{source_profiles}\n"
            "Верни только нумерованный список без вступления и без заключения. "
            "Строгий формат каждого пункта:\n"
            "1) Страна: Заголовок — дата. Одно короткое описание в 1-2 предложениях.\n"
            "Оригинал статьи: домен, https://полная-ссылка-на-статью\n"
            "Не пиши 'Кратко', 'Почему важно', 'Примечание', 'не найдено', 'выборка ограничена'. "
            "Не переноси год на отдельную строку и не разрывай дату. "
            "Если релевантных новостей меньше 10, верни сколько есть, но сначала постарайся собрать 10."
        )

    domain_rule = (
        "Use only this source pool: "
        f"{', '.join(allowed_domains)}. Try to build the list from different sites and countries first."
        if allowed_domains else
        "Use several independent sources instead of one site."
    )
    return (
        f"Prepare a summary of {TARGET_NEWS_ITEMS} news items for Russian relocators. "
        f"Publication date range: {start_date.isoformat()} to {today.isoformat()}. "
        "Topics: visas, residence permits, citizenship, entry rules, work and study migration, digital nomads, "
        "family reunion, legalization, and consular restrictions. Prioritize countries outside Russia. "
        "Include Russia-based news only when it directly affects Russians already abroad or preparing relocation. "
        f"{domain_rule} "
        "Exclude evergreen guides, service marketing, crime, sports, vacancies, and generic politics without a direct migration impact. "
        "If one country has two strong developments, that is acceptable, but aim for country diversity first. "
        + "Allowed source list and sections:\n"
        + f"{source_profiles}\n"
        "Return only a numbered list with no intro and no closing note. "
        "Strict format for each item:\n"
        "1) Country: Title — date. One short description in 1-2 sentences.\n"
        "Original article: domain, https://full-article-url\n"
        "Do not write 'Summary', 'Why it matters', 'Note', or 'limited source pool'. "
        "Do not put the year on a separate line and do not break the date. "
        "If fewer than 10 relevant items exist, return fewer, but first try to collect 10."
    )


def build_news_snapshot_prompt(lang):
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=NEWS_LOOKBACK_DAYS)
    allowed_domains = get_allowed_news_domains()
    source_profiles = build_source_profile_prompt(lang, compact=True)

    if lang == "ru":
        domain_rule = (
            "Работай только с этим пулом доменов: " + ", ".join(allowed_domains) + ". "
            if allowed_domains else
            "Используй несколько независимых источников. "
        )
        return (
            f"Найди {CANDIDATE_NEWS_ITEMS - 2}-{CANDIDATE_NEWS_ITEMS} кандидатов для новостного дайджеста релокантов из России за период "
            f"с {start_date.isoformat()} по {today.isoformat()}. "
            "Темы: визы, ВНЖ/ПМЖ, гражданство, правила въезда, трудовая и учебная миграция, digital nomad, "
            "воссоединение семьи, легализация, консульские ограничения. "
            "Приоритет: разные страны и разные домены; не больше 2 новостей с одного домена, если есть альтернатива. "
            "Не включай общие гайды, обзоры услуг, внутренние новости РФ без прямого влияния на релокацию, спорт, криминал, вакансии. "
            + domain_rule
            + "Если точной статьи нет, не придумывай ее. Нужна только оригинальная статья, а не главная страница сайта. "
            "Верни ТОЛЬКО JSON-массив объектов без markdown и без пояснений. "
            "Поля каждого объекта: country, title, date, summary, source_domain, source_url. "
            "summary: информативное описание в 2-3 предложениях, примерно в 2 раза подробнее короткой заметки. "
            "date: точная дата публикации или изменения, строкой, без формулировок вроде '3 месяца назад'. "
            "source_url: полный URL оригинальной статьи. "
            "Не используй homepage, category page, evergreen guide или маркетинговую страницу, если нет конкретного свежего изменения. "
            "Короткий список допустимых источников и разделов:\n"
            + f"{source_profiles}"
        )

    domain_rule = (
        "Use only this source pool: " + ", ".join(allowed_domains) + ". "
        if allowed_domains else
        "Use several independent sources. "
    )
    return (
        f"Find {CANDIDATE_NEWS_ITEMS - 2}-{CANDIDATE_NEWS_ITEMS} candidate news items for a Russian relocator digest from "
        f"{start_date.isoformat()} to {today.isoformat()}. "
        "Topics: visas, residence permits, citizenship, entry rules, work and study migration, digital nomads, "
        "family reunion, legalization, consular restrictions. Prioritize different countries and different domains; "
        "avoid more than 2 items from one domain if alternatives exist. "
        "Exclude generic guides, service pages, Russia-only domestic news without relocation impact, sports, crime, vacancies. "
        + domain_rule
        + "If there is no exact article, do not invent it. Only original article URLs, not site homepages. "
        "Return ONLY a JSON array of objects, no markdown and no explanations. "
        "Fields for each object: country, title, date, summary, source_domain, source_url. "
        "summary: informative 2-3 sentence description, about twice as detailed as a short brief. "
        "date: exact publication or change date as a string, without relative dates like '3 months ago'. "
        "source_url: full original article URL. "
        "Do not use site homepages, category pages, evergreen guides, or marketing pages unless they clearly contain a fresh policy change. "
        "Allowed source list and sections:\n"
        + f"{source_profiles}"
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


def get_latest_news_digest(lang, allow_stale=False):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rendered_html, items_json, raw_response, model_used, created_at
                    FROM news_digests
                    WHERE language_code = %s AND status = 'ready'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (lang,),
                )
                row = cur.fetchone()
                if not row:
                    return None

                age_sec = time.time() - row["created_at"].timestamp()
                if age_sec > NEWS_CACHE_TTL_SEC and not allow_stale:
                    return None
                row["age_sec"] = age_sec
                return row
    except Exception as e:
        logger.error(f"Error getting latest news digest: {e}")
        return None


def save_news_digest(lang, items, rendered_html, raw_response, model_used, status="ready"):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO news_digests (
                        language_code,
                        status,
                        items_json,
                        rendered_html,
                        raw_response,
                        model_used
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (lang, status, Json(items), rendered_html, raw_response, model_used),
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Error saving news digest: {e}")


def clear_news_digest(lang=None):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if lang:
                    cur.execute("DELETE FROM news_digests WHERE language_code = %s", (lang,))
                else:
                    cur.execute("DELETE FROM news_digests")
                conn.commit()
    except Exception as e:
        logger.error(f"Error clearing news digest: {e}")


def is_admin_news_chat(chat_id):
    return chat_id == NEWS_ADMIN_CHAT_ID


def pending_news_message(lang):
    if lang == "ru":
        return "🛠 Новостной дайджест ещё обновляется. Пока показываю только последний готовый snapshot."
    return "🛠 The news digest is still refreshing. Only the last ready snapshot is available for now."


def parse_article_date(raw_value):
    if not raw_value:
        return None

    value = raw_value.strip()
    value = re.sub(r"\s+", " ", value)
    value = re.sub(
        r"(?:≈|~|около|примерно|about|around)\s*\d+\s*"
        r"(?:дн(?:я|ей)?|недел(?:я|и|ь)?|месяц(?:а|ев)?|год(?:а|ов)?|лет|"
        r"day(?:s)?|week(?:s)?|month(?:s)?|year(?:s)?)\s*(?:назад|ago)?",
        "",
        value,
        flags=re.I,
    ).strip(" -—,.;")
    if not value:
        return None

    month_map = {
        "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
        "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    }

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass

    match = re.search(r"(\d{1,2})\s+([A-Za-zА-Яа-яЁё]+)\s+(\d{4})", value)
    if match:
        day_num = int(match.group(1))
        month_name = match.group(2).lower()
        year_num = int(match.group(3))
        month_num = month_map.get(month_name)
        if month_num:
            try:
                return date(year_num, month_num, day_num)
            except ValueError:
                return None

    return None


def get_news_pool_rows(lang, active_only=False):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if active_only:
                    cur.execute(
                        """
                        SELECT *
                        FROM news_digest_pool
                        WHERE language_code = %s AND is_active = TRUE
                        ORDER BY article_date DESC NULLS LAST, discovered_at DESC
                        """,
                        (lang,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM news_digest_pool
                        WHERE language_code = %s
                        ORDER BY article_date DESC NULLS LAST, discovered_at DESC
                        """,
                        (lang,),
                    )
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error loading news pool rows: {e}")
        return []


def upsert_news_pool_item(lang, item):
    article_date = parse_article_date(item.get("date", ""))
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO news_digest_pool (
                        language_code,
                        source_url,
                        source_domain,
                        title,
                        summary,
                        country,
                        article_date_raw,
                        article_date,
                        is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                    ON CONFLICT (language_code, source_url)
                    DO UPDATE SET
                        source_domain = EXCLUDED.source_domain,
                        title = EXCLUDED.title,
                        summary = EXCLUDED.summary,
                        country = EXCLUDED.country,
                        article_date_raw = EXCLUDED.article_date_raw,
                        article_date = EXCLUDED.article_date,
                        updated_at = NOW()
                    RETURNING id, discovered_at, updated_at, is_active
                    """,
                    (
                        lang,
                        item["source_url"],
                        item["source_domain"],
                        item["title"],
                        item["summary"],
                        item.get("country"),
                        item.get("date"),
                        article_date,
                    ),
                )
                row = cur.fetchone()
                conn.commit()
                return row
    except Exception as e:
        logger.error(f"Error upserting news pool item: {e}")
        return None


def set_active_news_pool_items(lang, active_urls):
    active_urls = list(active_urls)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE news_digest_pool SET is_active = FALSE, updated_at = NOW() WHERE language_code = %s",
                    (lang,),
                )
                if active_urls:
                    cur.execute(
                        """
                        UPDATE news_digest_pool
                        SET is_active = TRUE, updated_at = NOW()
                        WHERE language_code = %s AND source_url = ANY(%s)
                        """,
                        (lang, active_urls),
                    )
                conn.commit()
    except Exception as e:
        logger.error(f"Error updating active news pool items: {e}")

# ---------------------------------------------
# Очистка простого текста (без Markdown)
# ---------------------------------------------
def sanitize_plain_text(text, preserve_urls=False):
    if not text:
        return text

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1 — \2", text)
    if not preserve_urls:
        text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"^\s*[-*]\s+", "- ", text, flags=re.M)
    text = re.sub(r"\b([a-z0-9.-]+\.[a-z]{2,})\.\s+\(\1\b", r"\1 (", text, flags=re.I)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_news_response_text(text):
    if not text:
        return ""

    text = sanitize_plain_text(text, preserve_urls=True)
    text = re.sub(r"\b(Источник|Оригинал статьи|Source|Original article):\s+", r"\1: ", text, flags=re.I)
    text = re.sub(
        r"—\s*(\d{1,2}\s+[A-Za-zА-Яа-яЁё]+)\s*\n+\s*(20\d{2})\b",
        r"— \1 \2",
        text,
        flags=re.I,
    )
    text = re.sub(r"\(([a-z0-9.-]+\.[a-z]{2,})", r"\1", text, flags=re.I)
    first_item = re.search(r"^\s*\d+[\).]\s+", text, flags=re.M)
    if first_item:
        text = text[first_item.start():]
    return text.strip()


def split_numbered_news_blocks(text):
    normalized = normalize_news_response_text(text)
    if not normalized:
        return []
    return [
        match.group(0).strip()
        for match in re.finditer(r"(?ms)^\s*\d+[\).]\s+.*?(?=^\s*\d+[\).]\s+|\Z)", normalized)
    ]


def parse_news_items(text):
    if not text:
        return []

    blocks = split_numbered_news_blocks(text)
    if not blocks:
        return []

    items = []
    for block in blocks:
        normalized_block = re.sub(r"\s*\n+\s*", " ", block).strip()
        if normalized_block:
            items.append(normalized_block)
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


def extract_news_item_country(item_text):
    text = re.sub(r"^\d+[\).]\s*", "", item_text.strip())

    title_part = re.split(r"\s+Почему важно:|\s+Источник:|\s+Why it matters:|\s+Source:", text, maxsplit=1)[0]
    if ":" in title_part:
        country_candidate = title_part.split(":", 1)[0].strip()
        country_candidate = re.sub(r"[^A-Za-zА-Яа-яЁё0-9/—\- ]+", "", country_candidate).strip()
        if 2 <= len(country_candidate) <= 40:
            return country_candidate.lower()

    known_countries = [
        "сша", "польша", "румыния", "финляндия", "грузия", "япония", "канада",
        "черногория", "китай", "германия", "испания", "черногория", "швеция",
        "норвегия", "латвия", "литва", "эстония", "чехия", "дания", "франция",
        "исландия", "греция", "кипр", "сербия", "португалия", "италия",
        "венгрия", "хорватия", "черногория", "нидерланды", "бельгия",
        "евросоюз", "ес", "шенген", "румыния/шенген", "россия—китай"
    ]
    lower = title_part.lower()
    for country in known_countries:
        if lower.startswith(country):
            return country
    return ""


def get_news_item_domains(text):
    return [extract_news_item_domain(item) for item in parse_news_items(text) if extract_news_item_domain(item)]


def dedupe_news_items(items):
    deduped = []
    seen = set()
    domain_counts = {}
    country_counts = {}

    for item in items:
        key = normalize_news_item_key(item)
        if not key or key in seen:
            continue
        domain = extract_news_item_domain(item)
        country = extract_news_item_country(item)
        if country and country_counts.get(country, 0) >= 1 and len(deduped) < 6:
            continue
        if domain and domain_counts.get(domain, 0) >= 2 and len(deduped) >= 4:
            continue
        seen.add(key)
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        if country:
            country_counts[country] = country_counts.get(country, 0) + 1
        deduped.append(item)

    if len(deduped) < min(4, len(items)):
        deduped = []
        seen = set()
        domain_counts = {}
        country_counts = {}
        for item in items:
            key = normalize_news_item_key(item)
            if not key or key in seen:
                continue
            domain = extract_news_item_domain(item)
            country = extract_news_item_country(item)
            if country and country_counts.get(country, 0) >= 2 and len(deduped) >= 4:
                continue
            if domain and domain_counts.get(domain, 0) >= 2 and len(deduped) >= 4:
                continue
            seen.add(key)
            if domain:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
            if country:
                country_counts[country] = country_counts.get(country, 0) + 1
            deduped.append(item)

    return deduped[:10]


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


def needs_news_diversity_retry(text):
    items = parse_news_items(text)
    domains = {domain for domain in get_news_item_domains(text) if domain}
    if len(items) < 8:
        return True
    if len(domains) < 2 and len(items) >= 2:
        return True
    return False


def needs_news_format_retry(text):
    items = parse_news_items(text)
    if len(items) < 8:
        return True

    malformed = 0
    missing_links = 0
    for item in items:
        cleaned = clean_news_item_text(item)
        source = extract_source_info(item)
        if not cleaned or re.match(r"^(19|20)\d{2}\b", cleaned):
            malformed += 1
        if not source.get("url"):
            missing_links += 1

    if malformed > 0:
        return True
    if missing_links >= len(items):
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


def should_use_chat_web_search(messages):
    if not messages:
        return False

    last_content = str(messages[-1].get("content", "")).lower()
    temporal_markers = [
        "сегодня", "сейчас", "актуал", "последн", "новост", "свеж",
        "today", "current", "latest", "recent", "news", "updated",
    ]
    migration_markers = [
        "виза", "визы", "внж", "пмж", "гражданств", "релокац", "эмиграц",
        "иммиграц", "миграц", "убежищ", "цифров", "digital nomad", "nomad",
        "residence permit", "permanent residence", "citizenship", "visa",
        "asylum", "relocation", "migration",
    ]
    return any(marker in last_content for marker in temporal_markers + migration_markers)


def get_response_models(news_mode=False):
    candidates = []
    preferred = [OPENAI_MODEL]
    if news_mode:
        preferred = [OPENAI_NEWS_MODEL, OPENAI_MODEL]

    for model in preferred + get_fallback_models():
        if model and model not in candidates:
            candidates.append(model)
    return candidates


def get_tool_variants(news_mode=False, messages=None):
    if not news_mode:
        if should_use_chat_web_search(messages):
            return [("default", build_web_search_tool(news_mode=False))]
        return [("no_search", None), ("default", build_web_search_tool(news_mode=False))]

    if OPENAI_ENABLE_NEWS_FILTERS:
        return [
            ("filtered", build_web_search_tool(news_mode=True, include_filters=True)),
            ("unfiltered", build_web_search_tool(news_mode=True, include_filters=False)),
        ]

    return [("unfiltered", build_web_search_tool(news_mode=True, include_filters=False))]


def create_response(messages, lang="ru", news_mode=False):
    last_error = None

    for variant_name, web_search_tool in get_tool_variants(news_mode=news_mode, messages=messages):
        allowed_domains = []
        if web_search_tool:
            allowed_domains = web_search_tool.get("filters", {}).get("allowed_domains", [])

        for model in get_response_models(news_mode=news_mode):
            try:
                request_payload = {
                    "model": model,
                    "input": messages,
                }
                if web_search_tool:
                    request_payload["tools"] = [web_search_tool]
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


def response_to_dict(response):
    if response is None:
        return {}
    if hasattr(response, "model_dump"):
        try:
            return response.model_dump()
        except Exception:
            pass
    if isinstance(response, dict):
        return response
    return {}


def normalize_host(value):
    if not value:
        return ""
    host = value.strip().lower()
    if "://" in host:
        parsed = urlparse(host)
        host = parsed.netloc.lower()
    host = host.split("/")[0].split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def comparable_domain(value):
    host = normalize_host(value)
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def collect_response_citations(response):
    payload = response_to_dict(response)
    citations = []
    seen = set()

    def visit(node):
        if isinstance(node, dict):
            url = node.get("url")
            if isinstance(url, str) and url.startswith(("http://", "https://")):
                domain = normalize_host(url)
                if domain and domain != "api.openai.com":
                    key = (url, domain)
                    if key not in seen:
                        seen.add(key)
                        citations.append({"url": url, "domain": domain})
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(payload)
    return citations


def collect_output_text_annotations(response):
    payload = response_to_dict(response)
    output = payload.get("output", [])
    collected = []

    for item in output:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") != "output_text":
                continue
            text = content.get("text") or ""
            annotations = content.get("annotations") or []
            collected.append({"text": text, "annotations": annotations})

    return collected


def map_item_urls_from_annotations(response):
    text_parts = collect_output_text_annotations(response)
    if not text_parts:
        return []

    full_text = ""
    annotations = []
    offset = 0

    for part in text_parts:
        part_text = part["text"]
        full_text += part_text
        for ann in part["annotations"]:
            url = ann.get("url")
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                continue
            start = ann.get("start_index")
            end = ann.get("end_index")
            if isinstance(start, int):
                start += offset
            if isinstance(end, int):
                end += offset
            annotations.append({"url": url, "start": start, "end": end, "domain": normalize_host(url)})
        offset += len(part_text)

    blocks = [
        (match.start(), match.end(), match.group(0).strip())
        for match in re.finditer(r"(?ms)^\s*\d+[\).]\s+.*?(?=^\s*\d+[\).]\s+|\Z)", full_text)
    ]
    item_urls = []
    for start, end, _block in blocks:
        chosen = None
        for ann in annotations:
            ann_start = ann.get("start")
            if isinstance(ann_start, int) and start <= ann_start < end:
                chosen = ann
                break
        item_urls.append(chosen)

    return item_urls


def add_url_to_news_item(item_text, citation_url, citation_domain):
    if not citation_url or citation_url in item_text:
        return item_text

    source_pattern = r"((?:Оригинал статьи|Источник|Original article|Source):\s*[^,\n]+)"
    if re.search(source_pattern, item_text, flags=re.I):
        return re.sub(
            source_pattern,
            lambda m: f"{m.group(1)}, {citation_url}",
            item_text,
            count=1,
            flags=re.I,
        )

    label = "Оригинал статьи" if re.search(r"[А-Яа-яЁё]", item_text) else "Original article"
    suffix = "" if item_text.rstrip().endswith((".", "!", "?")) else "."
    return f"{item_text}{suffix} {label}: {citation_domain}, {citation_url}"


def enrich_news_text_with_citations(text, response):
    if not text:
        return text

    item_citations = map_item_urls_from_annotations(response)
    citations = collect_response_citations(response)
    if not citations and not item_citations:
        return text

    items = parse_news_items(text)
    if not items:
        return text

    used_urls = set()
    enriched_items = []

    for index, item in enumerate(items):
        if re.search(r"https?://", item, flags=re.I):
            enriched_items.append(item)
            continue

        matched = item_citations[index] if index < len(item_citations) else None
        if matched and matched.get("url"):
            used_urls.add(matched["url"])
            enriched_items.append(add_url_to_news_item(item, matched["url"], matched["domain"]))
            continue

        item_domain = extract_news_item_domain(item)
        item_host = normalize_host(item_domain)
        item_cmp = comparable_domain(item_domain)
        matched = None

        for citation in citations:
            if citation["url"] in used_urls:
                continue
            citation_host = normalize_host(citation["domain"])
            citation_cmp = comparable_domain(citation["domain"])
            if item_host and (item_host == citation_host or (item_cmp and item_cmp == citation_cmp)):
                matched = citation
                break

        if matched:
            used_urls.add(matched["url"])
            enriched_items.append(add_url_to_news_item(item, matched["url"], matched["domain"]))
        else:
            enriched_items.append(item)

    return "\n\n".join(enriched_items).strip()


def extract_response_text(response, news_mode=False):
    text = (response.output_text or "").strip()
    if news_mode:
        text = enrich_news_text_with_citations(text, response)
    return text


def extract_json_array_from_text(text):
    if not text:
        return []

    candidate = text.strip()
    fenced = re.search(r"```(?:json)?\s*(\[.*\])\s*```", candidate, flags=re.S | re.I)
    if fenced:
        candidate = fenced.group(1).strip()
    else:
        start = candidate.find("[")
        end = candidate.rfind("]")
        if start != -1 and end != -1 and end > start:
            candidate = candidate[start:end + 1]

    try:
        data = json.loads(candidate)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def normalize_digest_item(item):
    if not isinstance(item, dict):
        return None

    country = str(item.get("country", "")).strip()
    title = str(item.get("title", "")).strip()
    date = str(item.get("date", "")).strip()
    summary = str(item.get("summary", "")).strip()
    source_domain = normalize_host(str(item.get("source_domain", "")).strip())
    source_url = str(item.get("source_url", "")).strip()

    if source_url and not source_url.startswith(("http://", "https://")):
        source_url = ""
    if source_url and not source_domain:
        source_domain = normalize_host(source_url)

    if not title or not summary:
        return None

    relative_date_pattern = (
        r"(?:≈|~|около|примерно|about|around)?\s*\d+\s*"
        r"(?:дн(?:я|ей)?|недел(?:я|и|ь)?|месяц(?:а|ев)?|год(?:а|ов)?|лет|"
        r"day(?:s)?|week(?:s)?|month(?:s)?|year(?:s)?)\s*(?:назад|ago)?"
    )

    title = re.sub(relative_date_pattern, "", title, flags=re.I).strip(" \n\t-—,;.")
    title = re.sub(r"^[A-Za-zА-Яа-яЁё0-9/ —-]{2,40}:\s+", "", title).strip()
    date = re.sub(relative_date_pattern, "", date, flags=re.I).strip(" \n\t-—,;.")

    if not country:
        country = extract_news_item_country(f"{title}") or ""
    if not country:
        country = "Страна" if re.search(r"[А-Яа-яЁё]", title) else "Country"

    summary = sanitize_plain_text(summary, preserve_urls=True)
    summary = re.sub(r"\s{2,}", " ", summary).strip(" \n\t-—,;.")
    summary = re.sub(relative_date_pattern, "", summary, flags=re.I).strip(" \n\t-—,;.")
    sentences = [sentence.strip() for sentence in re.split(r"(?<=[.!?])\s+", summary) if sentence.strip()]
    if len(sentences) > 3:
        summary = " ".join(sentences[:3]).strip()
    if len(summary) < 90 or not source_url:
        return None

    article_date = parse_article_date(date)

    return {
        "country": country,
        "title": title,
        "date": date,
        "summary": summary,
        "source_domain": source_domain,
        "source_url": source_url,
        "article_date": article_date,
        "normalized_title_key": normalize_news_item_key(f"{source_domain} {title}"),
    }


def enrich_digest_items_with_citations(items, response):
    citations = collect_response_citations(response)
    if not citations:
        return items

    used_urls = {item["source_url"] for item in items if item.get("source_url")}
    enriched = []
    for item in items:
        current = dict(item)
        if current.get("source_url"):
            enriched.append(current)
            continue

        item_domain = comparable_domain(current.get("source_domain"))
        matched = None
        for citation in citations:
            if citation["url"] in used_urls:
                continue
            if item_domain and comparable_domain(citation["domain"]) == item_domain:
                matched = citation
                break

        if matched:
            current["source_url"] = matched["url"]
            current["source_domain"] = current.get("source_domain") or matched["domain"]
            used_urls.add(matched["url"])
        enriched.append(current)
    return enriched


def dedupe_digest_items(items):
    deduped = []
    seen_urls = set()
    seen_fallback = set()
    domain_counts = {}

    def sort_key(item):
        article_date = item.get("article_date")
        if article_date:
            return (0, -article_date.toordinal(), item.get("source_url", ""))
        return (1, 0, item.get("source_url", ""))

    for item in sorted(items, key=sort_key):
        source_url = item.get("source_url", "")
        fallback_key = item.get("normalized_title_key") or normalize_news_item_key(
            f"{item.get('source_domain', '')} {item.get('title', '')}"
        )
        if not source_url and not fallback_key:
            continue
        if source_url and source_url in seen_urls:
            continue
        if fallback_key and fallback_key in seen_fallback:
            continue

        domain = item.get("source_domain", "").strip().lower()
        if domain and domain_counts.get(domain, 0) >= MAX_NEWS_PER_DOMAIN:
            continue

        if source_url:
            seen_urls.add(source_url)
        if fallback_key:
            seen_fallback.add(fallback_key)
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

        deduped.append(dict(item))

    return deduped[:TARGET_NEWS_ITEMS]


def evaluate_digest_quality(items):
    domains = {item.get("source_domain") for item in items if item.get("source_domain")}
    urls = sum(1 for item in items if item.get("source_url"))
    domain_limit_ok = all(
        sum(1 for item in items if item.get("source_domain") == domain) <= MAX_NEWS_PER_DOMAIN
        for domain in domains
    )
    has_relative_dates = any(
        re.search(
            r"(?:≈|~|около|примерно|about|around)?\s*\d+\s*"
            r"(?:дн(?:я|ей)?|недел(?:я|и|ь)?|месяц(?:а|ев)?|год(?:а|ов)?|лет|"
            r"day(?:s)?|week(?:s)?|month(?:s)?|year(?:s)?)\s*(?:назад|ago)?",
            item.get("date", ""),
            flags=re.I,
        )
        for item in items
    )
    return {
        "item_count": len(items),
        "domains": len(domains),
        "urls": urls,
        "domain_limit_ok": domain_limit_ok,
        "has_relative_dates": has_relative_dates,
    }


def is_digest_ready(items):
    quality = evaluate_digest_quality(items)
    return (
        quality["item_count"] == READY_NEWS_MIN_ITEMS
        and quality["domains"] >= 3
        and quality["urls"] == READY_NEWS_MIN_ITEMS
        and quality["domain_limit_ok"]
        and not quality["has_relative_dates"]
    )


def row_to_digest_item(row):
    article_date = row.get("article_date")
    if article_date and isinstance(article_date, datetime):
        article_date = article_date.date()
    return {
        "id": row.get("id"),
        "country": row.get("country") or "",
        "title": row.get("title") or "",
        "date": row.get("article_date_raw") or "",
        "summary": row.get("summary") or "",
        "source_domain": row.get("source_domain") or "",
        "source_url": row.get("source_url") or "",
        "article_date": article_date if isinstance(article_date, date) else None,
        "normalized_title_key": normalize_news_item_key(
            f"{row.get('source_domain', '')} {row.get('title', '')}"
        ),
    }


def merge_news_pool_items(existing_active_items, new_candidate_items):
    if not new_candidate_items:
        return dedupe_digest_items(existing_active_items)

    merged = {}
    for item in existing_active_items:
        if item.get("source_url"):
            merged[item["source_url"]] = dict(item)

    for item in new_candidate_items:
        if item.get("source_url"):
            merged[item["source_url"]] = dict(item)

    return dedupe_digest_items(list(merged.values()))


def render_news_digest_html(items, lang):
    header = (
        "🧭 Новости для релокантов из России"
        if lang == "ru"
        else "🧭 News for Russian Relocators"
    )

    formatted = []
    source_label = "Оригинал статьи" if lang == "ru" else "Original article"

    for index, item in enumerate(items, start=1):
        title = escape_html(item.get("title", "").strip())
        date = escape_html(item.get("date", "").strip())
        summary = escape_html(item.get("summary", "").strip())
        lead = f"{index}) {title}"
        if summary:
            lead += f". {summary}"

        domain = escape_html(item.get("source_domain", "").strip())
        url = item.get("source_url", "").strip()

        if url and domain:
            source_line = f"{source_label}: <a href=\"{escape_html_attr(url)}\">{domain}</a>"
        elif url:
            source_line = f"{source_label}: <a href=\"{escape_html_attr(url)}\">{escape_html(url)}</a>"
        elif domain:
            source_line = f"{source_label}: {domain}"
        else:
            source_line = ""

        block_lines = [lead]
        if source_line:
            block_lines.append(source_line)
        if date:
            block_lines.append(date)
        formatted.append("\n".join(block_lines).strip())

    return f"{escape_html(header)}\n\n" + "\n\n".join(formatted)


def build_news_digest(chat_id, lang):
    system_content = (
        "Ты собираешь ежедневный миграционный дайджест. "
        "Отвечай только валидным JSON-массивом без markdown и без пояснений."
        if lang == "ru" else
        "You build a daily migration digest. Respond only with a valid JSON array, no markdown, no commentary."
    )

    prompt_variants = [build_news_snapshot_prompt(lang)]
    if lang == "ru":
        prompt_variants.append(
            build_news_snapshot_prompt(lang)
            + "\nСобери более широкую и интересную подборку: стремись к 10 пунктам, минимум к 8, "
              "и сначала ищи разные страны и разные домены. Не останавливайся на первых 2-3 совпадениях."
        )
    else:
        prompt_variants.append(
            build_news_snapshot_prompt(lang)
            + "\nBuild a broader and more interesting selection: aim for 10 items, minimum 8, "
              "and prioritize different countries and domains before repeating one source."
        )

    best_result = {"items": [], "rendered_html": "", "raw_response": "", "model_used": ""}

    for prompt in prompt_variants:
        messages = [
            {"role": "system", "content": system_content},
            {"role": "user", "content": prompt},
        ]
        response, model_used = create_response(messages, lang=lang, news_mode=True)
        raw_text = (response.output_text or "").strip()
        items = [normalize_digest_item(item) for item in extract_json_array_from_text(raw_text)]
        items = [item for item in items if item]
        items = enrich_digest_items_with_citations(items, response)
        items = dedupe_digest_items(items)

        candidate = {
            "items": items,
            "rendered_html": render_news_digest_html(items, lang) if items else "",
            "raw_response": raw_text,
            "model_used": model_used,
        }
        if len(candidate["items"]) > len(best_result["items"]):
            best_result = candidate
        if len(candidate["items"]) >= READY_NEWS_MIN_ITEMS:
            return candidate

    return best_result


def refresh_news_digest(lang="ru", force=False, chat_id=None):
    latest_ready = get_latest_news_digest(lang, allow_stale=True)
    if latest_ready and not force and latest_ready.get("age_sec", NEWS_CACHE_TTL_SEC + 1) < NEWS_CACHE_TTL_SEC:
        return {
            "status": "skipped",
            "reason": "ready_digest_is_fresh",
            "item_count": len(latest_ready.get("items_json") or []),
            "updated": False,
        }

    existing_pool_rows = get_news_pool_rows(lang, active_only=False)
    existing_pool_items = [row_to_digest_item(row) for row in existing_pool_rows]
    existing_pool_urls = {item["source_url"] for item in existing_pool_items if item.get("source_url")}

    digest = build_news_digest(chat_id or 0, lang)
    candidate_items = digest["items"]
    new_candidate_items = [item for item in candidate_items if item.get("source_url") not in existing_pool_urls]

    for item in candidate_items:
        upsert_news_pool_item(lang, item)

    refreshed_pool_rows = get_news_pool_rows(lang, active_only=False)
    refreshed_pool_items = [row_to_digest_item(row) for row in refreshed_pool_rows]
    final_items = merge_news_pool_items(refreshed_pool_items, [])
    quality = evaluate_digest_quality(final_items)

    if is_digest_ready(final_items):
        set_active_news_pool_items(
            lang,
            [item["source_url"] for item in final_items if item.get("source_url")],
        )
        rendered_html = render_news_digest_html(final_items, lang)
        save_news_digest(
            lang,
            final_items,
            rendered_html,
            digest["raw_response"],
            digest["model_used"],
            status="ready",
        )
        return {
            "status": "ready",
            "updated": bool(new_candidate_items),
            "new_count": len(new_candidate_items),
            "item_count": len(final_items),
            "domains": quality["domains"],
            "urls": quality["urls"],
        }

    if not new_candidate_items and refreshed_pool_items:
        return {
            "status": "unchanged",
            "updated": False,
            "new_count": 0,
            "item_count": quality["item_count"],
            "domains": quality["domains"],
            "urls": quality["urls"],
        }

    save_news_digest(
        lang,
        final_items or candidate_items,
        render_news_digest_html(final_items or candidate_items, lang) if (final_items or candidate_items) else "",
        digest["raw_response"],
        digest["model_used"],
        status="draft" if candidate_items else "failed",
    )
    return {
        "status": "draft" if candidate_items else "failed",
        "updated": False,
        "new_count": len(new_candidate_items),
        "item_count": quality["item_count"],
        "domains": quality["domains"],
        "urls": quality["urls"],
    }


def escape_html(text):
    if text is None:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def escape_html_attr(text):
    return escape_html(text).replace('"', "&quot;")


def extract_source_info(item_text):
    source_match = re.search(
        r"(?:оригинал статьи|источник|original article|source):\s*(.+)$",
        item_text,
        flags=re.I,
    )
    source_text = source_match.group(1).strip() if source_match else ""
    url_match = re.search(r"https?://\S+", source_text or item_text, flags=re.I)
    url = url_match.group(0).rstrip(").,;") if url_match else ""
    domain = extract_news_item_domain(source_text or item_text).strip("()[]{}.,;: ")

    if not domain and url:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().removeprefix("www.")

    return {"url": url, "domain": domain}


def strip_item_number(item_text):
    return re.sub(r"^\s*\d+[\).]\s*", "", item_text or "").strip()


def clean_news_item_text(item_text):
    text = strip_item_number(sanitize_plain_text(item_text, preserve_urls=True))
    text = re.sub(r"\s*(Кратко|Summary):\s*", " ", text, flags=re.I)
    text = re.sub(
        r"\s*(Почему важно|Why it matters):.*?(?=(Оригинал статьи|Источник|Original article|Source):|$)",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(
        r"\s*(Оригинал статьи|Источник|Original article|Source):.*$",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"^\s*Ниже представлены все найденные подходящие публикации:\s*", "", text, flags=re.I)
    text = re.sub(r"^\s*Выбранных источников.*?:\s*", "", text, flags=re.I)
    text = re.sub(r"^\s*Количество релевантных материалов.*?:\s*", "", text, flags=re.I)
    text = re.sub(r"\bУвы,.*$", "", text, flags=re.I)
    text = re.sub(r"\bПримечание:.*$", "", text, flags=re.I)
    text = re.sub(
        r"—\s*(\d{1,2}\s+[A-Za-zА-Яа-яЁё]+)\s+(20\d{2})\.",
        r"— \1 \2.",
        text,
        flags=re.I,
    )
    text = re.sub(r"\b20\d{2}\.\s+(?=[А-ЯЁA-Z])", "", text)
    text = re.sub(r"\s+\(([a-z0-9.-]+\.[a-z]{2,})\)?", "", text, flags=re.I)
    text = re.sub(r"\s{2,}", " ", text)
    text = text.strip(" \n\t-—,;.")

    sentences = re.split(r"(?<=[.!?])\s+", text)
    if len(sentences) > 2:
        text = " ".join(sentences[:2]).strip()

    return text


def render_news_item_html(item_text, lang, index):
    cleaned = clean_news_item_text(item_text)
    source = extract_source_info(item_text)
    body = escape_html(f"{index}) {cleaned}")
    source_label = "Оригинал статьи" if lang == "ru" else "Original article"
    link_label = "ссылка на оригинал" if lang == "ru" else "original link"

    if source["url"] and source["domain"]:
        source_html = (
            f"{source_label}: {escape_html(source['domain'])} - "
            f"<a href=\"{escape_html_attr(source['url'])}\">{link_label}</a>"
        )
    elif source["url"]:
        source_html = f"{source_label}: <a href=\"{escape_html_attr(source['url'])}\">{link_label}</a>"
    elif source["domain"]:
        source_html = f"{source_label}: {escape_html(source['domain'])}"
    else:
        source_html = ""

    return f"{body}\n{source_html}".strip() if source_html else body

def format_news_html(text, lang):
    header = (
        "🧭 Новости для релокантов из России"
        if lang == "ru"
        else "🧭 News for Russian Relocators"
    )
    if not text:
        return escape_html(header)

    items = dedupe_news_items(parse_news_items(text))

    formatted = []
    for index, raw in enumerate(items, start=1):
        rendered = render_news_item_html(raw, lang, index)
        if rendered:
            formatted.append(rendered)

    body = "\n\n".join(formatted) if formatted else escape_html(text)
    return f"{escape_html(header)}\n\n{body}".strip()


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

    system_prompt = """Ты — AI-консультант по вопросам миграционного права, виз, ВНЖ/ПМЖ и релокации.

Задача:
- давать актуальный и практичный ответ по сути вопроса;
- при необходимости использовать web_search для проверки текущих правил и процедур;
- не придумывать факты, даты, требования, суммы и названия программ.

Правила ответа:
- переходи сразу к сути, без вступительных и заключительных фраз;
- не добавляй рекламные блоки, предложения услуг и необязательные follow-up фразы;
- не вставляй общие дисклеймеры вроде «правила могут меняться» и «окончательное решение принимает госорган»,
  если пользователь прямо об этом не спрашивал и если это не критично для понимания ответа;
- если по источникам есть неопределенность или расхождение, коротко укажи это по делу;
- если вопрос требует актуальных данных, опирайся на web_search, а не на общие знания;
- пиши профессионально, понятно и без канцелярита.
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
        content = extract_response_text(response, news_mode=news_mode)
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
            return extract_response_text(retry, news_mode=news_mode)

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
            content = extract_response_text(retry, news_mode=news_mode)

        logger.info("OpenAI response completed with model=%s news_mode=%s", model_used, news_mode)
        return sanitize_plain_text(content, preserve_urls=news_mode) if news_mode else content

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
                fb_text = extract_response_text(fb, news_mode=news_mode)
                return sanitize_plain_text(fb_text, preserve_urls=news_mode) if news_mode else fb_text
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


def process_news_refresh_request(chat_id, lang, trigger_text, force=False):
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
            result = refresh_news_digest(lang=lang, force=force, chat_id=chat_id)
            if lang == "ru":
                ans = (
                    "🛠 Обновление дайджеста завершено.\n"
                    f"Статус: {result['status']}\n"
                    f"Новых новостей: {result.get('new_count', 0)}\n"
                    f"Пунктов в подборке: {result.get('item_count', 0)}\n"
                    f"Доменов: {result.get('domains', 0)}"
                )
            else:
                ans = (
                    "🛠 Digest refresh completed.\n"
                    f"Status: {result['status']}\n"
                    f"New items: {result.get('new_count', 0)}\n"
                    f"Items in digest: {result.get('item_count', 0)}\n"
                    f"Domains: {result.get('domains', 0)}"
                )
        except Exception:
            logger.exception("Unhandled error in process_news_refresh_request chat_id=%s lang=%s", chat_id, lang)
            ans = TEXTS[lang]["error"]

        save_message(chat_id, "user", trigger_text)
        save_message(chat_id, "assistant", ans)
        send_message(chat_id, ans)
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


@app.route("/tasks/refresh-news-digest", methods=["POST", "GET"])
def refresh_news_digest_task():
    token = request.headers.get("X-News-Cron-Token") or request.args.get("token")
    if not NEWS_CRON_TOKEN or token != NEWS_CRON_TOKEN:
        return jsonify({"ok": False, "error": "forbidden"}), 403

    lang = request.args.get("lang", "ru")
    force = request.args.get("force", "0").lower() in {"1", "true", "yes"}
    result = refresh_news_digest(lang=lang, force=force)
    return jsonify({"ok": True, **result})

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
        if not is_admin_news_chat(chat_id):
            send_message(chat_id, pending_news_message(lang))
            return "ok"

        job_key = make_news_job_key(chat_id, lang)
        with active_news_jobs_lock:
            if job_key in active_news_jobs:
                logger.info("News refresh job already active for %s", job_key)
                print(f"News refresh job already active for {job_key}", flush=True)
                return "ok"
            active_news_jobs.add(job_key)

        send_message(chat_id, t["searching"])
        worker = threading.Thread(
            target=process_news_refresh_request,
            args=(chat_id, lang, text, True),
            daemon=True
        )
        worker.start()
        return "ok"

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
        ready_digest = get_latest_news_digest(lang, allow_stale=True)
        if ready_digest and ready_digest.get("rendered_html"):
            send_message(chat_id, ready_digest["rendered_html"], parse_mode="HTML")
        else:
            send_message(chat_id, pending_news_message(lang))
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
