# backend/data_collector/collector.py
import ccxt.pro as ccxt # Импортируем ccxtpro
import asyncio
from typing import List, Dict, Any, Optional

from backend.core.config import settings, commissions_config
from backend.data_processor.processor import data_processor # Импортируем экземпляр процессора
from backend.utils.logger import logger # Убедитесь, что здесь импортируется настроенный логгер


class DataCollector:
    """
    Собирает рыночные данные с бирж с использованием ccxtpro.
    Запускает асинхронные задачи наблюдения за тикерами и стаканами.
    """
    def __init__(self):
        self._exchanges: Dict[str, ccxt.Exchange] = {}
        self._collecting_tasks: List[asyncio.Task] = []
        self._watched_symbols: Dict[str, List[str]] = {}

    async def load_exchanges(self):
        """Инициализирует экземпляры бирж и загружает их рынки."""
        logger.info("Загрузка бирж...")
        # Очищаем список загруженных бирж перед новой загрузкой на случай перезапуска
        self._exchanges = {}
        for exchange_id in settings.EXCHANGES:
            try:
                exchange_class = getattr(ccxt, exchange_id)
                exchange = exchange_class({
                    'apiKey': getattr(settings, f"{exchange_id.upper()}_API_KEY", ""),
                    'secret': getattr(settings, f"{exchange_id.upper()}_API_SECRET", ""),
                    # 'password': ..., # Если нужно (например, для OKX)
                    'enableRateLimit': True, # ccxtpro также поддерживает управление лимитами
                    'options': {
                       # 'defaultType': 'spot'
                    },
                })

                # Проверяем поддержку необходимых методов watch
                if not (exchange.has.get('watchTicker') or exchange.has.get('watchOrderBook')):
                    logger.warning(f"Биржа {exchange_id.upper()} не поддерживает необходимые WebSocket методы watchTicker или watchOrderBook. Пропускаем.")
                    try: await exchange.close() # Пытаемся закрыть соединение, если оно было открыто при создании
                    except Exception: pass # Игнорируем ошибки закрытия
                    continue

                # Загружаем список всех доступных торговых пар на бирже
                await exchange.load_markets()

                # Проверяем, что markets успешно загрузились (опциональная проверка)
                if not exchange.symbols:
                     logger.warning(f"Биржа {exchange_id.upper()} не загрузила ни одного символа. Пропускаем.")
                     try: await exchange.close()
                     except Exception: pass
                     continue

                self._exchanges[exchange_id] = exchange
                logger.info(f"Биржа {exchange_id.upper()} успешно загружена ({len(exchange.symbols)} символов). Поддерживает watchTicker: {exchange.has.get('watchTicker')}, watchOrderBook: {exchange.has.get('watchOrderBook')}")

            except AttributeError:
                logger.error(f"Биржа '{exchange_id}' не найдена в списке ccxtpro.")
            except Exception as e:
                # Логгируем ошибку загрузки биржи, но продолжаем загружать другие
                logger.error(f"Ошибка загрузки или инициализации биржи {exchange_id.upper()}: {e} (Type: {type(e).__name__})")

        if not self._exchanges:
            logger.error("Не удалось загрузить ни одной биржи. Проверьте настройки, ключи API и доступность бирж.")


    async def close_exchanges(self):
        """Закрывает соединения с биржами."""
        logger.info("Закрытие соединений с биржами...")
        for exchange in self._exchanges.values():
            try:
                # Пытаемся закрыть соединение, если метод close существует и вызываем
                if hasattr(exchange, 'close') and callable(exchange.close):
                   await exchange.close() # ccxtpro биржи имеют асинхронный метод close
                   logger.info(f"Соединение с {exchange.id.upper()} закрыто.")
                # else: # Это нормальная ситуация для некоторых базовых объектов в ccxtpro
                   # logger.debug(f"Биржа {exchange.id.upper()} не имеет асинхронного метода close().")
            except Exception as e:
                # Логгируем ошибку закрытия, но продолжаем
                logger.error(f"Ошибка при закрытии соединения с биржей {exchange.id.upper()}: {e} (Type: {type(e).__name__})")
        logger.info("Все соединения с биржами закрыты.")


    async def start_collecting(self):
        """
        Запускает асинхронные задачи сбора данных для каждой биржи и пары в фоне.
        Требует успешного подключения к Redis.
        """
        # Убеждаемся, что биржи загружены перед началом сбора данных
        await self.load_exchanges()

        # --- КРИТИЧНАЯ ПРОВЕРКА: УСПЕШНО ЛИ ПОДКЛЮЧЕН REDIS ---
        # DataProcessor.connect_redis вызывается в startup_event FastAPI.
        # Проверяем результат этого вызова перед запуском коллекторов.
        if not data_processor.is_connected:
             logger.error("Redis клиент не подключен в DataProcessor. Невозможно запустить задачи сбора данных. Пожалуйста, убедитесь, что Redis запущен и доступен, и перезапустите приложение.")
             # Возвращаемся, не запуская collect_tasks.
             return
        # logger.info("Redis клиент доступен. Запуск задач сбора данных...") # Лог перемещен ниже к запуску задач

        # Очищаем список задач на случай перезапуска или повторного вызова start_collecting
        if self._collecting_tasks:
             logger.warning(f"Обнаружено {len(self._collecting_tasks)} существующих задач сбора данных. Останавливаем их перед запуском новых.")
             await self.stop_collecting() # Останавливаем текущие перед запуском новых

        # Определяем список пар для наблюдения
        self._watched_symbols = {}
        for exchange_id, exchange in self._exchanges.items():
             # Берем список пар, для которых у нас есть комиссии
             configured_symbols = commissions_config.get_all_exchange_symbols(exchange_id)
             if not configured_symbols:
                  logger.warning(f"Для биржи {exchange_id.upper()} не найдено настроенных пар с комиссиями. Пропускаем сбор данных для этой биржи.")
                  continue

             # Фильтруем, оставляя только те пары, которые реально есть на бирже
             available_symbols = [
                  symbol for symbol in configured_symbols
                  if symbol in exchange.symbols # Проверяем наличие символа в загруженных markets
             ]

             if not available_symbols:
                  logger.warning(f"Из настроенных пар с комиссиями для биржи {exchange_id.upper()}, ни одна не найдена на бирже. Пропускаем сбор данных для этой биржи.")
                  continue

             self._watched_symbols[exchange_id] = available_symbols
             # Логгируем начало сбора для конкретной биржи, если есть доступные пары
             logger.info(f"Биржа {exchange_id.upper()}: Запуск задач сбора данных по парам: {available_symbols}")


        # Запускаем асинхронные задачи наблюдения
        self._collecting_tasks = []
        if not self._watched_symbols:
             logger.warning("Нет бирж/пар с комиссиями, доступных на биржах, для наблюдения. Не запущено ни одной задачи сбора данных.")
             return # Выходим, если нечего наблюдать

        # Запускаем задачи для каждой пары на каждой бирже
        for exchange_id, symbols in self._watched_symbols.items():
             exchange = self._exchanges[exchange_id] # Берем экземпляр из словаря успешно загруженных бирж

             for symbol in symbols:
                 # Запускаем задачи только если биржа поддерживает соответствующий метод watch_*
                 if exchange.has.get('watchTicker'):
                    # Создаем задачу и добавляем в список запущенных. Даем задаче имя для отладки.
                    task = asyncio.create_task(self._watch_ticker_loop(exchange, symbol), name=f"watch_ticker_{exchange_id}_{symbol}")
                    self._collecting_tasks.append(task)
                 # else: # Эти логи лучше на DEBUG, т.к. их может быть много, и они не критичны
                   # logger.debug(f"[{exchange.id.upper()}] Не поддерживает watchTicker для {symbol}.")


                 if exchange.has.get('watchOrderBook'):
                    # Создаем задачу для наблюдения за стаканом (ограничение глубины 1)
                    task = asyncio.create_task(self._watch_orderbook_loop(exchange, symbol, limit=1), name=f"watch_orderbook_{exchange_id}_{symbol}")
                    self._collecting_tasks.append(task)
                 # else:
                    # logger.debug(f"[{exchange.id.upper()}] Не поддерживает watchOrderBook для {symbol}.")


        if not self._collecting_tasks:
             logger.warning("Не запущено ни одной задачи сбора данных (после проверки поддержки watch методов и наличия пар).")
        else:
             logger.info(f"Запущено {len(self._collecting_tasks)} задач сбора данных в фоновом режиме.")


    async def stop_collecting(self):
        """Останавливает задачи сбора данных и закрывает соединения."""
        if not self._collecting_tasks:
            logger.info("Нет активных задач сбора данных для остановки.")
            await self.close_exchanges() # Все равно пытаемся закрыть соединения
            return

        logger.info(f"Остановка {len(self._collecting_tasks)} задач сбора данных...")
        # Отменяем все запущенные задачи
        for task in self._collecting_tasks:
            try:
                # Отменяем задачу, она вызовет asyncio.CancelledError
                task.cancel()
            except Exception as e:
                # Логгируем ошибку отмены, но продолжаем
                logger.error(f"Ошибка при отмене задачи {task.get_name()}: {e} (Type: {type(e).__name__})")

        # Ждем завершения всех задач после их отмены
        # return_exceptions=True предотвращает остановку gather, если одна из задач
        # выкинет необработанное исключение при отмене.
        # Таймаут на завершение задач (опционально, но полезно, чтобы shutdown не вис)
        try:
            await asyncio.wait_for(asyncio.gather(*self._collecting_tasks, return_exceptions=True), timeout=15.0) # Ждем максимум 15 секунд
            logger.info("Все задачи сбора данных завершены после отмены.")
        except asyncio.TimeoutError:
             # Если таймаут, некоторые задачи могли не завершиться
             logger.warning("Таймаут ожидания завершения задач сбора данных. Некоторые задачи могли не завершиться корректно.")
        except Exception as e:
             # Логгируем другие неожиданные ошибки при ожидании завершения
             logger.error(f"Неожиданная ошибка при ожидании завершения задач сбора данных: {e} (Type: {type(e).__name__})")
        finally:
             self._collecting_tasks = [] # Очищаем список задач после попытки завершения

        await self.close_exchanges() # Закрываем соединения с биржами

    async def _watch_ticker_loop(self, exchange: ccxt.Exchange, symbol: str):
        logger.info(f"[{exchange.id.upper()}] Запуск наблюдения за тикером: {symbol}")
        while True:
            try:
                # Проверяем, что возвращает watch_ticker
                ticker_iter = await exchange.watch_ticker(symbol)  # Добавляем await
                async for ticker in ticker_iter:
                    if ticker and ticker.get('symbol') == symbol and ticker.get('bid') is not None and ticker.get(
                            'ask') is not None:
                        await data_processor.cache_ticker(exchange.id, symbol, ticker)
                    # else:
                    #     logger.debug(f"[{exchange.id.upper()}] Получен невалидный тикер для {symbol}: {ticker}")
            except asyncio.CancelledError:
                logger.info(f"[{exchange.id.upper()}] Задача наблюдения за тикером {symbol} отменена.")
                raise
            except Exception as e:
                logger.error(
                    f"[{exchange.id.upper()}] Ошибка в _watch_ticker_loop для {symbol}: {e} (Type: {type(e).__name__}). Попытка переподключения через 5 секунд...")
                await asyncio.sleep(5)

    async def _watch_orderbook_loop(self, exchange: ccxt.Exchange, symbol: str, limit: Optional[int] = None):
        logger.info(f"[{exchange.id.upper()}] Запуск наблюдения за стаканом: {symbol} (limit={limit})")
        while True:
            try:
                # Проверяем, что возвращает watch_order_book
                orderbook_iter = await exchange.watch_order_book(symbol, limit=limit)  # Добавляем await
                async for orderbook in orderbook_iter:
                    if orderbook and orderbook.get('symbol') == symbol and orderbook.get(
                            'bid') is not None and orderbook.get('ask') is not None:
                        await data_processor.cache_orderbook(exchange.id, symbol, orderbook)
                    # else:
                    #     logger.debug(f"[{exchange.id.upper()}] Получен невалидный стакан для {symbol}: {orderbook}")
            except asyncio.CancelledError:
                logger.info(f"[{exchange.id.upper()}] Задача наблюдения за стаканом {symbol} отменена.")
                raise
            except Exception as e:
                logger.error(
                    f"[{exchange.id.upper()}] Ошибка в _watch_orderbook_loop для {symbol}: {e} (Type: {type(e).__name__}). Попытка переподключения через 5 секунд...")
                await asyncio.sleep(5)

             # Эти строки теперь внутри except блока
             # logger.warning(f"[{exchange.id.upper()}] Поток watch_order_book для {symbol} завершился. Переподключение через 5 секунд...")
             # await asyncio.sleep(5)


# Инициализируем экземпляр коллектора (синглтон)
data_collector = DataCollector()