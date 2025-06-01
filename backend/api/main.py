

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware # Если фронтенд будет на другом домене
import asyncio

from backend.core.config import settings
from backend.utils.logger import logger # Настроим логгер при старте
from backend.data_processor.processor import data_processor # Импорт экземпляра процессора
from backend.data_collector.collector import data_collector # Импорт экземпляра коллектора
from backend.arbitrage_finder.finder import arbitrage_finder # Импорт экземпляра finder'а
from backend.api.v1.endpoints import router as api_v1_router # Импорт роутов v1


logger.info(f"Запуск приложения: {settings.PROJECT_NAME} (v{settings.VERSION})")

# Создаем экземпляр FastAPI приложения
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="API для сканера крипто арбитражных возможностей",
)

# Настройка CORS (Cross-Origin Resource Sharing)
# Это нужно, если ваш фронтенд будет работать на другом адресе (порту/домене),
# отличном от бэкенда. В продакшене нужно ограничить origins конкретным доменом фронтенда.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешить запросы с любых доменов (для разработки)
    allow_credentials=True,
    allow_methods=["*"],  # Разрешить все HTTP методы (GET, POST, etc.)
    allow_headers=["*"],  # Разрешить все заголовки
)

# --- События запуска и остановки приложения ---

@app.on_event("startup")
async def startup_event():
    """Выполняется при запуске FastAPI приложения."""
    logger.info("Выполнение startup_event...")
    # Подключение к Redis
    await data_processor.connect_redis()

    # Запуск сбора данных с бирж (в фоне)
    # start_collecting запускает asyncio задачи, которые работают параллельно
    # Мы не await'им здесь, т.к. они должны работать в фоне
    asyncio.create_task(data_collector.start_collecting()) # Запускаем как фоновую задачу


    # Arbitrage finder также может работать в цикле, периодически проверяя Redis
    # asyncio.create_task(arbitrage_finder.start_finding_loop()) # Пример


@app.on_event("shutdown")
async def shutdown_event():
    """Выполняется при остановке FastAPI приложения."""
    logger.info("Выполнение shutdown_event...")
    # Остановка задач сбора данных
    await data_collector.stop_collecting()



    # Отключение от Redis
    await data_processor.disconnect_redis()
    logger.info("Startup/shutdown события выполнены.")

# --- Подключение API роутов ---
# Все эндпоинты из api_v1_router будут доступны по префиксу /api/v1
app.include_router(api_v1_router, prefix="/api/v1")

# --- Базовый рутовый эндпоинт (опционально) ---
@app.get("/", tags=["Root"])
async def read_root():
    """Базовый эндпоинт."""
    return {"message": "Crypto Arbitrage Scanner API is running", "version": settings.VERSION}