# Repository Guidelines

## Project Structure & Module Organization
The Telegram-facing Flask service lives in `bot_grs.py`; it exposes webhook handlers, Tavily/OpenAI integrations, and Postgres helpers. Database bootstrap logic is split into `init_db.py` for Railway one-off runs. Deployment metadata sits in `Procfile` (`web: python bot_grs.py`). Python dependencies live in `requirements.txt`, while `.env` (ignored) stores secrets such as `TELEGRAM_TOKEN`, `OPENAI_API_KEY`, `TAVILY_API_KEY`, and `DATABASE_URL`. Keep assets or experiments out of the root; prefer creating `tests/` or `docs/` folders as needed.

## Build, Test, and Development Commands
Create an isolated runtime: `python -m venv .venv && source .venv/bin/activate` (use `./.venv/Scripts/activate` on Windows). Install deps via `pip install -r requirements.txt`. Initialize the Postgres schema with `python init_db.py`, which is idempotent. Run the bot locally using `python bot_grs.py` (or `FLASK_APP=bot_grs.py flask run --debug` while iterating). Railway picks up `Procfile`, so keep it synchronized with local startup instructions.

## Coding Style & Naming Conventions
Stick to Python 3.11+ syntax, PEP 8 spacing (4 spaces, 100-char lines), and descriptive snake_case identifiers. Group related helpers with clear separators as done in `bot_grs.py`, and keep logging statements informative but non-sensitive. Prefer explicit env lookups at the top of each module and wrap external API calls in small functions (`tavily_search`, `generate_answer`) so they stay testable.

## Testing Guidelines
Automated tests are expected under `tests/`, mirroring module names as `test_<module>.py`. Use `pytest -q` for fast runs and add fixtures that stub OpenAI/Tavily responses plus psycopg connections. Include regression cases for webhook parsing (reply chains) and history limits before shipping features. Manual verification through a staging Telegram bot is still required after every deploy.

## Commit & Pull Request Guidelines
Recent history shows short, present-tense messages (`systempromt fix`, `reply_to_message_fix`). Follow that style but add context when touching infra (e.g., `db: add retry logging`). Every PR should include a concise summary, linked Railway issue or ticket, testing evidence (`pytest`, manual bot check), and screenshots/log excerpts when user-facing behavior changes. Mention new env vars or schema tweaks explicitly to unblock deployers.

## Security & Configuration Tips
Never hardcode API tokens or URLs; rely on `.env` locally and Railway variables remotely. Rotate keys in tandem with config changes, and verify `DATABASE_URL` permissions before running migrations. Avoid logging raw user messages when diagnosing issues—mask PII and truncate large payloads to keep application logs compliant.
