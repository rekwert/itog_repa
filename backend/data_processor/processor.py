# backend/data_processor/processor.py
import redis.asyncio as redis
import redis.exceptions # Явно импортируем модуль исключений
import asyncio
import json
from typing import Dict, Any, Optional

from backend.core.config import settings
from backend.utils.logger import logger # Убедитесь, что здесь импортируется настроенный логгер

class DataProcessor:
    """
    Обрабатывает данные с бирж и кэширует их в Redis.
    Также предоставляет методы для получения данных из кэша.
    """
    def __init__(self):
        self._redis_client: Optional[redis.Redis] = None
        self.is_connected: bool = False # Флаг статуса подключения

    async def connect_redis(self):
        logger.info("Попытка подключения к Redis...")
        logger.debug(
            f"Конфигурация Redis: host={settings.REDIS_HOST}, port={settings.REDIS_PORT}, db={settings.REDIS_DB}")

        if self.is_connected and self._redis_client is not None:
            try:
                logger.debug(
                    f"Проверка существующего клиента ping: тип клиента={type(self._redis_client)}, тип метода ping={type(self._redis_client.ping)}")
                if self._redis_client.ping():  # Без await
                    logger.info("Существующий клиент Redis подключен и доступен.")
                    self.is_connected = True
                    return
                else:
                    logger.warning("Пинг существующего клиента Redis вернул False. Пытаемся создать новое соединение.")
                    self._redis_client = None
                    self.is_connected = False
            except Exception as e:
                logger.warning(
                    f"Пинг существующего клиента Redis завершился ошибкой: {e} (Тип: {type(e).__name__}). Пытаемся создать новое соединение.")
                self._redis_client = None
                self.is_connected = False

        try:
            logger.info(
                f"Попытка создать новое соединение с Redis на {settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}")
            self._redis_client = redis.from_url(
                f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}",
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )

            logger.debug(
                f"Начальный пинг нового клиента: тип клиента={type(self._redis_client)}, тип метода ping={type(self._redis_client.ping)}")
            if self._redis_client.ping():  # Без await
                logger.info("Успешно подключен и пингован клиент Redis.")
                self.is_connected = True
            else:
                logger.error("Пинг Redis для нового соединения не удался.")
                self._redis_client = None
                self.is_connected = False

        except redis.exceptions.ConnectionError as e:
            logger.error(f"Не удалось подключиться к Redis (ConnectionError): {e}")
            self._redis_client = None
            self.is_connected = False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при подключении к Redis: {e} (Тип: {type(e).__name__})")
            self._redis_client = None
            self.is_connected = False

        if not self.is_connected:
            logger.error("Клиент Redis НЕ подключен после попытки.")


    async def disconnect_redis(self):
        """Закрывает соединение с Redis."""
        logger.info("Закрытие соединения с Redis...")
        if self._redis_client:
            try:
                logger.debug("Closing Redis client connection...")
                # Попытка асинхронного закрытия клиента
                await self._redis_client.close()
                # Попытка отключить пул соединений
                try:
                    if hasattr(self._redis_client, 'connection_pool') and self._redis_client.connection_pool:
                       await self._redis_client.connection_pool.disconnect()
                       logger.debug("Redis connection pool disconnected.")
                except Exception as pool_e:
                     logger.warning(f"Error during Redis connection pool disconnect: {pool_e} (Type: {type(pool_e).__name__})")

                logger.info("Соединение с Redis закрыто успешно.")
            except Exception as e:
                 logger.error(f"Ошибка при закрытии соединения с Redis: {e} (Type: {type(e).__name__})")
            self._redis_client = None
            self.is_connected = False
        else:
            logger.info("Redis client was not connected, nothing to close.")


    # --- Методы для кэширования данных ---
    # В этих методах проверяем self.is_connected перед выполнением операций

    async def cache_ticker(self, exchange_id: str, symbol: str, ticker_data: Dict[str, Any]):
        """Кэширует данные тикера в Redis."""
        if not self.is_connected: # Проверяем флаг подключения
            # logger.debug("Redis client not available, cannot cache ticker.")
            return

        key = f"ticker:{exchange_id.lower()}:{symbol.upper()}"
        try:
            await self._redis_client.set(key, json.dumps(ticker_data))
            # logger.debug(f"Кэширован тикер для {exchange_id}:{symbol}")
        except Exception as e:
            logger.error(f"Ошибка кэширования тикера для {exchange_id}:{symbol} в Redis: {e} (Type: {type(e).__name__})")


    async def cache_orderbook(self, exchange_id: str, symbol: str, orderbook_data: Dict[str, Any]):
        """Кэширует данные стакана (лучшие Bid/Ask уровни) в Redis."""
        if not self.is_connected: # Проверяем флаг подключения
            # logger.debug("Redis client not available, cannot cache orderbook.")
            return

        key = f"orderbook:{exchange_id.lower()}:{symbol.upper()}"
        try:
            cached_data = {
                'bid': orderbook_data.get('bid'),
                'ask': orderbook_data.get('ask'),
                'bidVolume': orderbook_data.get('bidVolume'),
                'askVolume': orderbook_data.get('askVolume'),
                'timestamp': orderbook_data.get('timestamp')
            }
            await self._redis_client.set(key, json.dumps(cached_data))
            # logger.debug(f"Кэширован стакан для {exchange_id}:{symbol}")
        except Exception as e:
             logger.error(f"Ошибка кэширования стакана для {exchange_id}:{symbol} в Redis: {e} (Type: {type(e).__name__})")

    # --- Методы для получения данных ---

    async def get_ticker(self, exchange_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Получает данные тикера из Redis."""
        if not self.is_connected: # Проверяем флаг подключения
            # logger.debug("Redis client not available, cannot get ticker.")
            return None

        key = f"ticker:{exchange_id.lower()}:{symbol.upper()}"
        try:
            data = await self._redis_client.get(key)
            if data:
                return json.loads(data) if isinstance(data, str) else data
            return None
        except Exception as e:
            logger.error(f"Ошибка получения тикера для {exchange_id}:{symbol} из Redis: {e} (Type: {type(e).__name__})")
            return None

    async def get_orderbook(self, exchange_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Получает данные стакана (лучшие Bid/Ask) из Redis."""
        if not self.is_connected: # Проверяем флаг подключения
            # logger.debug("Redis client not available, cannot get orderbook.")
            return None

        key = f"orderbook:{exchange_id.lower()}:{symbol.upper()}"
        try:
            data = await self._redis_client.get(key)
            if data:
                 return json.loads(data) if isinstance(data, str) else data
            return None
        except Exception as e:
            logger.error(f"Ошибка получения стакана для {exchange_id}:{symbol} из Redis: {e} (Type: {type(e).__name__})")
            return None

# Инициализируем экземпляр процессора (синглтон)
data_processor = DataProcessor()