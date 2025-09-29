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
OI_DELTA_THRESHOLD = float(os.environ.get('OI_DELTA_THRESHOLD', 10.0))

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ---

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"[DB Error] Не удалось подключиться к базе данных: {e}")
        return None

def get_tokens_to_process(conn):
    """Находит токены, у которых есть РОВНО 2 записи для анализа."""
    # Мы ищем только те токены, по которым только что пришло второе значение
    query = "SELECT token_symbol FROM oi_data GROUP BY token_symbol HAVING COUNT(*) = 2;"
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            tokens = [item[0] for item in cur.fetchall()]
            return tokens
    except Exception as e:
        print(f"[DB Error] Не удалось получить список токенов: {e}")
        return []

def get_latest_records(conn, token_symbol):
    query = "SELECT id, token_name, oi_growth_4h, scan_time FROM oi_data WHERE token_symbol = %s ORDER BY scan_time DESC LIMIT 2;"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (token_symbol,))
            records = cur.fetchall()
            if len(records) == 2:
                return records[0], records[1] # [0] = current, [1] = previous
            return None, None
    except Exception as e:
        print(f"[DB Error] Не удалось получить записи для {token_symbol}: {e}")
        return None, None

def cleanup_old_records(conn, previous_record_id, token_symbol):
    """Удаляет самую старую (предыдущую) запись для токена."""
    query = "DELETE FROM oi_data WHERE id = %s AND token_symbol = %s;"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (previous_record_id, token_symbol))
            conn.commit()
            print(f"[DB] Успешно удалена обработанная запись с ID {previous_record_id} для {token_symbol}.")
    except Exception as e:
        print(f"[DB Error] Не удалось удалить старую запись для {token_symbol}: {e}")
        conn.rollback()

# --- ФУНКЦИЯ ДЛЯ ОТПРАВКИ В TELEGRAM ---

async def send_telegram_alert(message):
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
        print(f"[Telegram] Успешно отправлено уведомление.")
    except Exception as e:
        print(f"[Telegram Error] Не удалось отправить уведомление: {e}")

# --- ОСНОВНОЙ ЦИКЛ АНАЛИЗАТОРА (PUSH-МОДЕЛЬ) ---
# ПАТЧ 3 для analyzer_bot.py (замените весь блок if __name__...)

# --- ОСНОВНОЙ ЦИКЛ АНАЛИЗАТОРА (PUSH-МОДЕЛЬ) ---
if __name__ == "__main__":
    print("--- ЗАПУСК СЕРВИСА-АНАЛИЗАТОРА ДИНАМИКИ OI (v2 - Умный анализ) ---")
    
    if not all([DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        print("[CRITICAL] Не все переменные окружения установлены.")
    else:
        # Проверка сортировки не требуется, т.к. Postgres по умолчанию добавляет записи в конец.
        # При выборке мы явно указываем ORDER BY scan_time.
        
        listener_conn = get_db_connection()
        if listener_conn:
            listener_conn.autocommit = True
            cursor = listener_conn.cursor()
            cursor.execute("LISTEN new_data_event;")
            print("[DB] Слушаю канал 'new_data_event'...")

            while True:
                listener_conn.poll()
                while listener_conn.notifies:
                    notification = listener_conn.notifies.pop(0)
                    print(f"\n--- {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} | Получен сигнал NOTIFY! ---")
                    
                    analysis_conn = get_db_connection()
                    if analysis_conn:
                        tokens_to_process = get_tokens_to_process(analysis_conn)
                        print(f"Найдено {len(tokens_to_process)} токенов с новой парой данных для анализа.")

                        for token in tokens_to_process:
                            current_record, previous_record = get_latest_records(analysis_conn, token)
                            
                            if current_record and previous_record:
                                current_id, current_name, current_oi, _ = current_record
                                prev_id, _, prev_oi, _ = previous_record

                                oi_delta = current_oi - prev_oi

                                print(f"  > Анализ {token}: Текущий рост OI {current_oi:.2f}%, Предыдущий {prev_oi:.2f}%. Дельта: {oi_delta:.2f}%")
                                
                                # ИЗМЕНЕНО: Проверяем ДЕЛЬТУ, а не текущее значение
                                if oi_delta >= OI_DELTA_THRESHOLD:
                                    message = (
                                        f"🚀 *Алерт по УСКОРЕНИЮ роста OI* 🚀\n\n"
                                        f"Токен: *{current_name} ({token})*\n\n"
                                        f"🔥 Изменение роста OI за 4 часа: *{oi_delta:+.2f}%*\n"
                                        f"_(Текущий рост: {current_oi:.2f}%, Предыдущий: {prev_oi:.2f}%)_"
                                    )
                                    asyncio.run(send_telegram_alert(message))
                                
                                # Удаляем предыдущую запись, чтобы токен снова ждал пару
                                cleanup_old_records(analysis_conn, prev_id, token)
                        
                        analysis_conn.close()
                
                time.sleep(1)