# backend/core/config.py

import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from decimal import Decimal

# Загружаем переменные окружения из .env файла
load_dotenv()

# --- Определение путей ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = BASE_DIR / "backend" / "config"
COMMISSIONS_DIR = CONFIG_DIR / "commissions"

# --- Класс для загрузки и хранения комиссий ---
class CommissionsConfig:
    """Хранит загруженные данные по комиссиям."""
    def __init__(self):
        self._data: Dict[str, Dict[str, Dict[str, str]]] = {}
        self._load_commissions()

    def _load_commissions(self):
        """Загружает комиссии из JSON файлов в директории config/commissions/."""
        print(f"Загрузка комиссий из директории: {COMMISSIONS_DIR}")

        if not COMMISSIONS_DIR.exists():
            print(f"Внимание: Директория комиссий не найдена: {COMMISSIONS_DIR}")
            return

        for file_path in COMMISSIONS_DIR.glob("*.json"):
            exchange_name = file_path.stem.lower()
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    exchange_commissions = json.load(f)
                    self._data[exchange_name] = exchange_commissions
                    print(f"  Загружены комиссии для биржи: {exchange_name.upper()}")
            except FileNotFoundError:
                print(f"  Ошибка: Файл комиссий не найден для биржи {exchange_name.upper()}")
            except json.JSONDecodeError:
                print(f"  Ошибка: Некорректный JSON в файле {file_path}")
            except Exception as e:
                print(f"  Неизвестная ошибка при загрузке комиссий из {file_path}: {e}")

        if not self._data:
             print("Внимание: Не загружено ни одной комиссии для бирж.")

    def get_commission(self, exchange: str, symbol: str, commission_type: str) -> Optional[str]:
        """Получает строковое значение комиссии для конкретной биржи, пары и типа."""
        exchange_data = self._data.get(exchange.lower())
        if not exchange_data:
            return None

        symbol_data = exchange_data.get(symbol.upper())
        if not symbol_data:
            return None

        return symbol_data.get(commission_type)

    def get_all_exchange_symbols(self, exchange: str) -> List[str]:
        """Возвращает список всех символов (пар), для которых есть комиссии на указанной бирже."""
        exchange_data = self._data.get(exchange.lower())
        if not exchange_data:
            return []
        return list(exchange_data.keys())

# --- Класс для общих настроек приложения ---
class Settings:
    """Базовые настройки приложения, читаемые из переменных окружения."""
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "Crypto Arbitrage Scanner")
    VERSION: str = os.getenv("VERSION", "0.1.0")

    # Настройки Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))

    # Список бирж для работы
    EXCHANGES: List[str] = [
        "binance", "bybit", "mexc", "bitget", "digifinex", "exmo", "xt",
        "bitmart", "gate", "kucoin", "phemex", "coinw", "bitrue", "hitbtc", "htx"
    ]

    # API ключи бирж
    BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")
    BYBIT_API_KEY: str = os.getenv("9OZFVuuiAjCo1TsHKj")
    BYBIT_API_SECRET: str = os.getenv("WIQa8VnceRYLVuRhZKX6dm36fQoqY8asT2vT")
    MEXC_API_KEY: str = os.getenv("mx0vglBqJNkMEZJHUJ")
    MEXC_API_SECRET: str = os.getenv("d1d4747f380f42b6bde464df3a19280e")
    BITGET_API_KEY: str = os.getenv("BITGET_API_KEY", "")
    BITGET_API_SECRET: str = os.getenv("BITGET_API_SECRET", "")
    DIGIFINEX_API_KEY: str = os.getenv("DIGIFINEX_API_KEY", "")
    DIGIFINEX_API_SECRET: str = os.getenv("DIGIFINEX_API_SECRET", "")
    EXMO_API_KEY: str = os.getenv("EXMO_API_KEY", "")
    EXMO_API_SECRET: str = os.getenv("EXMO_API_SECRET", "")
    XT_API_KEY: str = os.getenv("XT_API_KEY", "")
    XT_API_SECRET: str = os.getenv("XT_API_SECRET", "")
    BITMART_API_KEY: str = os.getenv("BITMART_API_KEY", "")
    BITMART_API_SECRET: str = os.getenv("BITMART_API_SECRET", "")
    GATE_API_KEY: str = os.getenv("8adcf5babe1b1ece60b1f0a4c0a02aea")
    GATE_API_SECRET: str = os.getenv("76a8b96c99ddafd6ef5bedb39bffd6b6676b4488774ca4995f9f002b8bef8366")
    KUCOIN_API_KEY: str = os.getenv("683c9bbd965ac1000180bcfb")
    KUCOIN_API_SECRET: str = os.getenv("989876ff-1506-4288-a042-45b2cf0016cf")
    PHEMEX_API_KEY: str = os.getenv("PHEMEX_API_KEY", "")
    PHEMEX_API_SECRET: str = os.getenv("PHEMEX_API_SECRET", "")
    COINW_API_KEY: str = os.getenv("COINW_API_KEY", "")
    COINW_API_SECRET: str = os.getenv("COINW_API_SECRET", "")
    BITRUE_API_KEY: str = os.getenv("BITRUE_API_KEY", "")
    BITRUE_API_SECRET: str = os.getenv("BITRUE_API_SECRET", "")
    HITBTC_API_KEY: str = os.getenv("HITBTC_API_KEY", "")
    HITBTC_API_SECRET: str = os.getenv("HITBTC_API_SECRET", "")
    HTX_API_KEY: str = os.getenv("7360fd05-f9488dba-qz5c4v5b6n-cbd72")
    HTX_API_SECRET: str = os.getenv("959c0775-34dd05e4-5213f261-3b68e")

    # Настройки для поиска арбитража
    try:
        MIN_PROFIT_PERCENT: Decimal = Decimal(os.getenv("MIN_PROFIT_PERCENT", "0.01"))
    except InvalidOperation:
        print("Ошибка: Некорректное значение для MIN_PROFIT_PERCENT в .env. Используется значение по умолчанию 0.01.")
        MIN_PROFIT_PERCENT: Decimal = Decimal("0.01")

# --- Инициализация объектов конфигурации ---
settings = Settings()
commissions_config = CommissionsConfig()