import os
import time
import psycopg2
import asyncio
from datetime import datetime, timezone
from telegram import Bot

# --- ГЛАВНАЯ КОНФИГУРАЦИЯ ---
DATABASE_URL = os.environ.get('DATABASE_URL')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

OI_GROWTH_ALERT_THRESHOLD = 5.0 
CHECK_INTERVAL_SECONDS = 60 * 5 # Каждые 5 минут

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ---

def get_db_connection():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"[DB Error] Не удалось подключиться к базе данных: {e}")
        return None

def get_tokens_with_new_data(conn):
    """Находит токены, у которых есть 2 или более записей."""
    query = "SELECT token_symbol FROM oi_data GROUP BY token_symbol HAVING COUNT(*) >= 2;"
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            tokens = [item[0] for item in cur.fetchall()]
            return tokens
    except Exception as e:
        print(f"[DB Error] Не удалось получить список токенов: {e}")
        return []

def get_last_two_records(conn, token_symbol):
    """Получает две последние записи для указанного токена."""
    query = "SELECT id, token_name, oi_growth_4h FROM oi_data WHERE token_symbol = %s ORDER BY scan_time DESC LIMIT 2;"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (token_symbol,))
            records = cur.fetchall()
            if len(records) == 2:
                # records[0] - самая новая, records[1] - предыдущая
                return records[1], records[0] 
            return None, None
    except Exception as e:
        print(f"[DB Error] Не удалось получить записи для {token_symbol}: {e}")
        return None, None

def delete_older_records(conn, newest_record_id, token_symbol):
    """Удаляет все записи для токена, КРОМЕ самой новой."""
    query = "DELETE FROM oi_data WHERE token_symbol = %s AND id != %s;"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (token_symbol, newest_record_id))
            deleted_count = cur.rowcount
            conn.commit()
            if deleted_count > 0:
                print(f"[DB] Успешно удалено {deleted_count} старых записей для {token_symbol}.")
    except Exception as e:
        print(f"[DB Error] Не удалось удалить старые записи для {token_symbol}: {e}")
        conn.rollback()

# --- ФУНКЦИЯ ДЛЯ ОТПРАВКИ В TELEGRAM ---

async def send_telegram_alert(message):
    """Асинхронно отправляет сообщение в Telegram."""
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
        print(f"[Telegram] Успешно отправлено уведомление.")
    except Exception as e:
        print(f"[Telegram Error] Не удалось отправить уведомление: {e}")

# --- ОСНОВНОЙ ЦИКЛ АНАЛИЗАТОРА ---
if __name__ == "__main__":
    print("--- ЗАПУСК СЕРВИСА-АНАЛИЗАТОРА OI ---")
    
    if not all([DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        print("[CRITICAL] Не все переменные окружения (DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) установлены.")
    else:
        while True:
            print(f"\n--- {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} | Начало цикла анализа ---")
            conn = get_db_connection()
            if conn:
                tokens_to_analyze = get_tokens_with_new_data(conn)
                print(f"Найдено {len(tokens_to_analyze)} токенов для анализа.")

                for token in tokens_to_analyze:
                    previous_record, latest_record = get_last_two_records(conn, token)
                    
                    if previous_record and latest_record:
                        prev_id, prev_name, prev_oi = previous_record
                        latest_id, latest_name, latest_oi = latest_record

                        print(f"  > Анализ {token}: Новое значение OI Growth = {latest_oi:.2f}%")
                        if latest_oi >= OI_GROWTH_ALERT_THRESHOLD:
                            message = (
                                f"🚀 *Алерт по росту OI* 🚀\n\n"
                                f"Токен: *{latest_name} ({token})*\n"
                                f"Рост OI за последние 4 часа: *{latest_oi:.2f}%*\n"
                                f"_(Предыдущее значение: {prev_oi:.2f}%)_"
                            )
                            asyncio.run(send_telegram_alert(message))
                        
                        # Удаляем все записи для этого токена, кроме самой последней
                        delete_older_records(conn, latest_id, token)

                conn.close()

            print(f"--- Анализ завершен. Следующая проверка через {CHECK_INTERVAL_SECONDS / 60:.0f} минут. ---")
            time.sleep(CHECK_INTERVAL_SECONDS)