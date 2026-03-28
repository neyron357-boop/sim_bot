import os
import threading

import bot
from web_app import app as web_app


def run_web() -> None:
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    debug = os.getenv("WEB_DEBUG", "0") == "1"

    web_app.run(
        host=host,
        port=port,
        debug=debug,
        use_reloader=False,
    )


def main() -> None:
    bot.init_db()
    bot.migrate_legacy_json()

    web_thread = threading.Thread(target=run_web, daemon=True, name="flask-web")
    web_thread.start()

    print("Веб-приложение и Telegram-бот запускаются...", flush=True)
    bot.main()


if __name__ == "__main__":
    main()
