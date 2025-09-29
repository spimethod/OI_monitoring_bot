import os
import time
import psycopg2
from telegram import Bot

# --- ГЛАВНАЯ КОНФИГУРАЦИЯ ---
# Переменные окружения, которые нужно будет добавить в Railway
DATABASE_URL = os.environ.get('DATABASE_URL')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# Порог для отправки алерта (например, если OI вырос более чем на 5% за последние 4 часа)
OI_GROWTH_ALERT_THRESHOLD = 5.0 

# Как часто бот будет проверять базу данных (в секундах)
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
    """Находит токены, у которых есть 2 или более записей, отсортированных по времени."""
    query = """
        SELECT token_symbol FROM oi_data
        GROUP BY token_symbol
        HAVING COUNT(*) >= 2;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            # fetchall() вернет список кортежей, например [('BTC',), ('ETH',)]
            # Мы преобразуем его в простой список ['BTC', 'ETH']
            tokens = [item[0] for item in cur.fetchall()]
            return tokens
    except Exception as e:
        print(f"[DB Error] Не удалось получить список токенов: {e}")
        return []

def get_last_two_records(conn, token_symbol):
    """Получает две последние записи для указанного токена."""
    query = """
        SELECT id, oi_growth_4h FROM oi_data
        WHERE token_symbol = %s
        ORDER BY scan_time DESC
        LIMIT 2;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (token_symbol,))
            records = cur.fetchall()
            # Возвращаем (id_старой, oi_старое), (id_новой, oi_новое)
            if len(records) == 2:
                # fetchall возвращает записи от новых к старым, поэтому переворачиваем
                return records[1], records[0] 
            return None, None
    except Exception as e:
        print(f"[DB Error] Не удалось получить записи для {token_symbol}: {e}")
        return None, None

def delete_old_records(conn, record_id_to_delete):
    """Удаляет старую запись по ее ID."""
    query = "DELETE FROM oi_data WHERE id = %s;"
    try:
        with conn.cursor() as cur:
            cur.execute(query, (record_id_to_delete,))
            conn.commit()
            print(f"[DB] Успешно удалена старая запись с ID {record_id_to_delete}.")
    except Exception as e:
        print(f"[DB Error] Не удалось удалить запись с ID {record_id_to_delete}: {e}")
        conn.rollback()

# --- ФУНКЦИЯ ДЛЯ ОТПРАВКИ В TELEGRAM ---

async def send_telegram_alert(message):
    """Асинхронно отправляет сообщение в Telegram."""
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        print(f"[Telegram] Успешно отправлено уведомление.")
    except Exception as e:
        print(f"[Telegram Error] Не удалось отправить уведомление: {e}")

# --- ОСНОВНОЙ ЦИКЛ АНАЛИЗАТОРА ---
if __name__ == "__main__":
    print("--- ЗАПУСК СЕРВИСА-АНАЛИЗАТОРА OI ---")
    
    if not all([DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        print("[CRITICAL] Не все переменные окружения (DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) установлены.")
    else:
        # Для асинхронной отправки в Telegram
        import asyncio

        while True:
            print(f"\n--- {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} | Начало цикла анализа ---")
            conn = get_db_connection()
            if conn:
                # 1. Получаем список токенов, у которых есть что анализировать
                tokens_to_analyze = get_tokens_with_new_data(conn)
                print(f"Найдено {len(tokens_to_analyze)} токенов для анализа.")

                for token in tokens_to_analyze:
                    # 2. Получаем последнюю и предпоследнюю запись для каждого токена
                    previous_record, latest_record = get_last_two_records(conn, token)
                    
                    if previous_record and latest_record:
                        prev_id, prev_oi = previous_record
                        latest_id, latest_oi = latest_record

                        # 3. Сравниваем с порогом
                        print(f"  > Анализ {token}: Новое значение OI Growth = {latest_oi:.2f}%")
                        if latest_oi >= OI_GROWTH_ALERT_THRESHOLD:
                            # 4. Если порог превышен, отправляем алерт
                            message = (
                                f"🚀 **Алерт по росту OI** 🚀\n\n"
                                f"Токен: **{token}**\n"
                                f"Рост OI за последние 4 часа: **{latest_oi:.2f}%**\n"
                                f"(Предыдущее значение: {prev_oi:.2f}%)"
                            )
                            # Запускаем асинхронную отправку
                            asyncio.run(send_telegram_alert(message))
                        
                        # 5. Удаляем самую старую запись (позапрошлую)
                        # Этот шаг лучше выполнять после того, как убедимся, что все работает.
                        # Пока что мы удаляем только предпоследнюю (старую) из двух полученных.
                        delete_old_records(conn, prev_id)

                conn.close()

            # 6. "Засыпаем" до следующей проверки
            print(f"--- Анализ завершен. Следующая проверка через {CHECK_INTERVAL_SECONDS / 60:.0f} минут. ---")
            time.sleep(CHECK_INTERVAL_SECONDS)