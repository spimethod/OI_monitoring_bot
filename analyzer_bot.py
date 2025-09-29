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

# Порог для отправки алерта (берется из переменных, по умолчанию 10.0%)
# Сравниваться будет ИЗМЕНЕНИЕ (дельта) роста OI
OI_DELTA_THRESHOLD = float(os.environ.get('OI_DELTA_THRESHOLD', 10.0))

# Как часто бот будет проверять базу данных
CHECK_INTERVAL_SECONDS = 60 * 1 # Каждую минуту

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ---

def get_db_connection():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"[DB Error] Не удалось подключиться к базе данных: {e}")
        return None

def get_tokens_to_process(conn):
    """Находит токены, у которых есть 2 или более записей для анализа."""
    query = "SELECT token_symbol FROM oi_data GROUP BY token_symbol HAVING COUNT(*) >= 2;"
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            tokens = [item[0] for item in cur.fetchall()]
            return tokens
    except Exception as e:
        print(f"[DB Error] Не удалось получить список токенов: {e}")
        return []

def get_latest_records(conn, token_symbol):
    """Получает ДВЕ самые последние записи для указанного токена."""
    query = "SELECT id, token_name, oi_growth_4h FROM oi_data WHERE token_symbol = %s ORDER BY scan_time DESC LIMIT 2;"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (token_symbol,))
            records = cur.fetchall()
            if len(records) == 2:
                # records[0] - самая новая (current)
                # records[1] - предыдущая (previous)
                return records[0], records[1] 
            return None, None
    except Exception as e:
        print(f"[DB Error] Не удалось получить записи для {token_symbol}: {e}")
        return None, None

def cleanup_old_records(conn, newest_record_id, token_symbol):
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

# --- ОСНОВНОЙ ЦИКЛ АНАЛИЗАТОРА (PULL-МОДЕЛЬ) ---
if __name__ == "__main__":
    print("--- ЗАПУСК СЕРВИСА-АНАЛИЗАТОРА ДИНАМИКИ OI (v2.1 - Корректное удаление) ---")
    
    if not all([DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        print("[CRITICAL] Не все переменные окружения (DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) установлены.")
    else:
        while True:
            print(f"\n--- {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} | Начало цикла анализа ---")
            conn = get_db_connection()
            if conn:
                tokens_to_process = get_tokens_to_process(conn)
                print(f"Найдено {len(tokens_to_process)} токенов для анализа.")

                for token in tokens_to_process:
                    current_record, previous_record = get_latest_records(conn, token)
                    
                    if current_record and previous_record:
                        current_id, current_name, current_oi = current_record
                        prev_id, _, prev_oi = previous_record

                        # Вычисляем дельту (разницу)
                        oi_delta = current_oi - prev_oi

                        print(f"  > Анализ {token}: Текущий рост OI {current_oi:.2f}%, Предыдущий {prev_oi:.2f}%. Дельта: {oi_delta:.2f}%")
                        
                        # Сравниваем ДЕЛЬТУ с порогом
                        if oi_delta >= OI_DELTA_THRESHOLD:
                            message = (
                                f"🚀 *Алерт по УСКОРЕНИЮ роста OI* 🚀\n\n"
                                f"Токен: *{current_name} ({token})*\n\n"
                                f"🔥 Изменение роста OI за 4 часа: *{oi_delta:+.2f}%*\n"
                                f"_(Текущий рост: {current_oi:.2f}%, Предыдущий: {prev_oi:.2f}%)_"
                            )
                            asyncio.run(send_telegram_alert(message))
                        
                        # Удаляем ВСЕ записи для этого токена, кроме самой последней
                        cleanup_old_records(conn, current_id, token)

                conn.close()

            print(f"--- Анализ завершен. Следующая проверка через {CHECK_INTERVAL_SECONDS / 60:.0f} мин. ---")
            time.sleep(CHECK_INTERVAL_SECONDS)