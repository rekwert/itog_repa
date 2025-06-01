# backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from contextlib import asynccontextmanager

from backend.core.config import settings
from backend.utils.logger import logger
from backend.data_processor.processor import data_processor
from backend.data_collector.collector import data_collector
from backend.arbitrage_finder.finder import arbitrage_finder
from backend.api.v1.endpoints import router as api_v1_router
from backend.monitoring import start_prometheus_server

logger.info(f"Запуск приложения: {settings.PROJECT_NAME} (v{settings.VERSION})")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Выполнение startup...")
    start_prometheus_server()
    await data_processor.connect_redis()
    asyncio.create_task(data_collector.start_collecting())
    asyncio.create_task(arbitrage_finder.start_finding_loop())
    yield
    logger.info("Выполнение shutdown...")
    await data_collector.stop_collecting()
    await arbitrage_finder.stop_finding_loop()
    await data_processor.disconnect_redis()
    logger.info("Startup/shutdown события выполнены.")

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="API для сканера крипто арбитражных возможностей",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_v1_router, prefix="/api/v1")

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Crypto Arbitrage Scanner API is running", "version": settings.VERSION}