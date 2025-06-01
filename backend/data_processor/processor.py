# backend/data_processor/processor.py
import redis.asyncio as redis
import json
import ccxtpro
from typing import Dict, Any, Optional

from backend.core.config import settings
from backend.utils.logger import logger

class DataProcessor:
    def __init__(self):
        self._redis_client: Optional[redis.Redis] = None
        self._exchange_instances: Dict[str, Any] = {}

    async def connect_redis(self):
        """Подключается к Redis."""
        try:
            if self._redis_client is None:
                self._redis_client = redis.Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    db=settings.REDIS_DB,
                    decode_responses=True
                )
                await self._redis_client.ping()
                logger.info("Успешно подключено к Redis")
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {e}", exc_info=True)
            self._redis_client = None
            raise

    async def disconnect_redis(self):
        """Отключается от Redis."""
        try:
            if self._redis_client is not None:
                await self._redis_client.aclose()
                logger.info("Отключено от Redis")
        except Exception as e:
            logger.error(f"Ошибка отключения от Redis: {e}", exc_info=True)
        finally:
            self._redis_client = None

    async def cache_orderbook(self, exchange_id: str, symbol: str, orderbook_data: Dict[str, Any]):
        """Кэширует данные стакана в Redis."""
        if self._redis_client is None:
            logger.error("Redis клиент не инициализирован")
            return False
        key = f"orderbook:{exchange_id}:{symbol}"
        try:
            serialized_data = json.dumps(orderbook_data)
            await self._redis_client.set(key, serialized_data, ex=60)  # Храним 60 секунд
            logger.debug(f"Успешно кэширован стакан для {exchange_id}:{symbol}")
            return True
        except Exception as e:
            logger.error(f"Ошибка кэширования стакана для {exchange_id}:{symbol} в Redis: {e}", exc_info=True)
            return False

    async def get_orderbook(self, exchange_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Получает данные стакана из Redis."""
        if self._redis_client is None:
            logger.error("Redis клиент не инициализирован")
            return None
        key = f"orderbook:{exchange_id}:{symbol}"
        try:
            serialized_data = await self._redis_client.get(key)
            if serialized_data:
                orderbook = json.loads(serialized_data)
                logger.debug(f"Получен стакан для {exchange_id}:{symbol} из Redis")
                return orderbook
            logger.debug(f"Стакан для {exchange_id}:{symbol} не найден в Redis")
            return None
        except Exception as e:
            logger.error(f"Ошибка получения стакана для {exchange_id}:{symbol} из Redis: {e}", exc_info=True)
            return None

    async def cache_ticker(self, exchange_id: str, symbol: str, ticker_data: Dict[str, Any]):
        """Кэширует данные тикера в Redis."""
        if self._redis_client is None:
            logger.error("Redis клиент не инициализирован")
            return False
        key = f"ticker:{exchange_id}:{symbol}"
        try:
            serialized_data = json.dumps(ticker_data)
            await self._redis_client.set(key, serialized_data, ex=60)
            logger.debug(f"Успешно кэширован тикер для {exchange_id}:{symbol}")
            return True
        except Exception as e:
            logger.error(f"Ошибка кэширования тикера для {exchange_id}:{symbol} в Redis: {e}", exc_info=True)
            return False

    async def get_ticker(self, exchange_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Получает данные тикера из Redis."""
        if self._redis_client is None:
            logger.error("Redis клиент не инициализирован")
            return None
        key = f"ticker:{exchange_id}:{symbol}"
        try:
            serialized_data = await self._redis_client.get(key)
            if serialized_data:
                ticker = json.loads(serialized_data)
                logger.debug(f"Получен тикер для {exchange_id}:{symbol} из Redis")
                return ticker
            logger.debug(f"Тикер для {exchange_id}:{symbol} не найден в Redis")
            return None
        except Exception as e:
            logger.error(f"Ошибка получения тикера для {exchange_id}:{symbol} из Redis: {e}", exc_info=True)
            return None

    def get_exchange(self, exchange_id: str) -> Optional[Any]:
        """Возвращает экземпляр биржи."""
        if exchange_id not in self._exchange_instances:
            try:
                exchange_class = getattr(ccxtpro, exchange_id, None)
                if exchange_class is None:
                    logger.error(f"Биржа {exchange_id} не поддерживается ccxtpro")
                    return None
                api_key = getattr(settings, f"{exchange_id.upper()}_API_KEY", None)
                api_secret = getattr(settings, f"{exchange_id.upper()}_API_SECRET", None)
                self._exchange_instances[exchange_id] = exchange_class({
                    'apiKey': api_key,
                    'secret': api_secret,
                    'enableRateLimit': True,
                })
                logger.info(f"Создан экземпляр биржи {exchange_id}")
            except Exception as e:
                logger.error(f"Ошибка создания экземпляра биржи {exchange_id}: {e}", exc_info=True)
                return None
        return self._exchange_instances.get(exchange_id)

data_processor = DataProcessor()