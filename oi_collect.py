import requests
import time
from datetime import datetime, timedelta, timezone
import os
import psycopg2
from psycopg2 import sql

# --- ГЛАВНАЯ КОНФИГУРАЦИЯ ---
# Эти ключи будут браться из переменных окружения на Railway
COINGLASS_API_KEY = os.environ.get('COINGLASS_API_KEY')
COINMARKETCAP_API_KEY = os.environ.get('COINMARKETCAP_API_KEY')

NUMBER_OF_TOKENS_TO_SCAN = 300
API_DELAY_SECONDS = 2 

# --- КОНФИГУРАЦИЯ БАЗЫ ДАННЫХ (для Railway) ---
DATABASE_URL = os.environ.get('DATABASE_URL')

# --- БАЗОВЫЕ URL API ---
CMC_BASE_URL = 'https://pro-api.coinmarketcap.com/v1'
COINGLASS_BASE_URL_V4 = 'https://open-api-v4.coinglass.com/api/futures'

# --- ФУНКЦИИ-СБОРЩИКИ ДАННЫХ ---

def get_top_symbols():
    """Получает с CoinMarketCap список топ-N токенов для сканирования."""
    print(f"[CMC] Получение списка топ-{NUMBER_OF_TOKENS_TO_SCAN} токенов...")
    url = f"{CMC_BASE_URL}/cryptocurrency/listings/latest"
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY}
    params = {'limit': NUMBER_OF_TOKENS_TO_SCAN}
    try:
        response = requests.get(url, headers=headers, params=params)
        # Проверяем, что запрос вообще прошел успешно (код 200)
        response.raise_for_status() 
        data = response.json()
        
        # Проверяем, есть ли в ответе ключ 'data', или это сообщение об ошибке
        if 'data' in data:
            stablecoins = ['USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDP']
            symbols = [{'symbol': item['symbol'], 'name': item['name']} for item in data['data'] if item['symbol'] not in stablecoins and not item['symbol'].startswith('W')]
            print(f"[CMC] Успешно получено {len(symbols)} торговых символов.")
            return symbols
        else:
            # Если ключа 'data' нет, значит, пришла ошибка от API (скорее всего, лимиты)
            print(f"[CMC] КРИТИЧЕСКАЯ ОШИБКА: API не вернул данные. Ответ сервера: {data}")
            return []
            
    except requests.exceptions.HTTPError as http_err:
        # Эта ошибка сработает, если код ответа не 200 (например, 429 - Too Many Requests)
        print(f"[CMC] КРИТИЧЕСКАЯ HTTP ОШИБКА: {http_err}")
        return []
    except Exception as e:
        # Любая другая ошибка (проблемы с сетью и т.д.)
        print(f"[CMC] КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return []

def get_oi_growth_from_coinglass(symbol):
    """Получает и рассчитывает 4ч рост OI через Coinglass."""
    url = f"{COINGLASS_BASE_URL_V4}/open-interest/aggregated-history"
    headers = {'CG-API-KEY': COINGLASS_API_KEY}
    params = {'symbol': symbol, 'interval': 'h4', 'limit': 2}
    try:
        response = requests.get(url, headers=headers, params=params).json()
        if str(response.get('code')) == '0' and len(response.get('data', [])) == 2:
            data_points = response['data']
            oi_today = float(data_points[-1]['close'])
            oi_4h_ago = float(data_points[-2]['close'])
            if oi_4h_ago > 0:
                return ((oi_today - oi_4h_ago) / oi_4h_ago) * 100
        return None
    except Exception:
        return None

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ---

def get_db_connection():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"[DB Error] Не удалось подключиться к базе данных: {e}")
        return None

def setup_database(conn):
    """Создает таблицу для данных OI, если она не существует."""
    try:
        with conn.cursor() as cur:
            # Упрощаем создание таблицы, убираем сложный индекс
            cur.execute("""
                CREATE TABLE IF NOT EXISTS oi_data (
                    id SERIAL PRIMARY KEY,
                    scan_time TIMESTAMPTZ DEFAULT NOW(),
                    token_symbol VARCHAR(20) NOT NULL,
                    token_name TEXT,
                    oi_growth_4h FLOAT,
                    -- Делаем уникальной саму пару "символ-время", это проще и надежнее
                    UNIQUE(token_symbol, scan_time)
                );
            """)
            conn.commit()
            print("[DB] Таблица 'oi_data' готова к работе.")
    except Exception as e:
        print(f"[DB Error] Не удалось создать таблицу: {e}")
        conn.rollback()

def insert_oi_data(conn, data_list):
    """Вставляет список данных по OI в базу данных (упрощенная версия)."""
    if not data_list:
        return
    
    # Простой и надежный запрос INSERT
    query = """
        INSERT INTO oi_data (token_symbol, token_name, oi_growth_4h)
        VALUES (%s, %s, %s);
    """
    
    records_to_insert = [(item['symbol'], item['name'], item['oi_growth']) for item in data_list]
    
    try:
        with conn.cursor() as cur:
            # executemany - эффективный способ вставить много строк одним запросом
            cur.executemany(query, records_to_insert)
            conn.commit()
            print(f"[DB] Успешно записано {len(records_to_insert)} строк в базу данных.")
    except Exception as e:
        print(f"[DB Error] Не удалось записать данные: {e}")
        conn.rollback()

# --- ОСНОВНОЙ СКРИПТ OI СКАНЕР ---
if __name__ == "__main__":
    print("--- ЗАПУСК СКАНЕРА РОСТА OI С ЗАПИСЬЮ В БД (v2 - Порционная запись) ---")
    
    if not DATABASE_URL:
        print("[CRITICAL] Переменная окружения DATABASE_URL не установлена. Завершение работы.")
    else:
        conn = get_db_connection()
        if conn:
            setup_database(conn)

            symbols_to_scan = get_top_symbols()
            
            # ИЗМЕНЕНО: Создаем временный список для порции данных
            data_batch = []
            BATCH_SIZE = 20 # Будем записывать в БД каждые 20 токенов

            for i, token_info in enumerate(symbols_to_scan, 1):
                token = token_info['symbol']
                token_name = token_info['name']
                print(f"({i}/{len(symbols_to_scan)}) Сканирование {token} ({token_name})...", end="")
                
                oi_growth = get_oi_growth_from_coinglass(token)
                time.sleep(API_DELAY_SECONDS)
                
                if oi_growth is not None:
                    print(f" Рост OI за 4ч: {oi_growth:.2f}%")
                    # Добавляем данные в порцию
                    data_batch.append({'symbol': token, 'name': token_name, 'oi_growth': oi_growth})
                else:
                    print(" нет данных по OI.")

                # ИЗМЕНЕНО: Проверяем, не пора ли записать порцию в БД
                if i % BATCH_SIZE == 0 or i == len(symbols_to_scan):
                    if data_batch:
                        print(f"--- [DB] Запись порции из {len(data_batch)} токенов в базу данных... ---")
                        insert_oi_data(conn, data_batch)
                        data_batch = [] # Очищаем порцию
            
            # --- Итоговый ТОП-список больше не нужен, т.к. все данные уже в БД ---
            
            conn.close()
            print("[DB] Соединение с базой данных закрыто.")

    print("\nАнализ и запись завершены.")