from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from backend.monitoring import start_prometheus_server

from backend.core.config import settings
from backend.utils.logger import logger
from backend.data_processor.processor import data_processor
from backend.data_collector.collector import data_collector
from backend.arbitrage_finder.finder import arbitrage_finder
from backend.api.v1.endpoints import router as api_v1_router

logger.info(f"Запуск приложения: {settings.PROJECT_NAME} (v{settings.VERSION})")

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="API для сканера крипто арбитражных возможностей",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    logger.info("Выполнение startup_event...")
    start_prometheus_server()  # Запуск Prometheus
    await data_processor.connect_redis()
    asyncio.create_task(data_collector.start_collecting())
    asyncio.create_task(arbitrage_finder.start_finding_loop())

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Выполнение shutdown_event...")
    await data_collector.stop_collecting()
    await arbitrage_finder.stop_finding_loop()
    await data_processor.disconnect_redis()
    logger.info("Startup/shutdown события выполнены.")

app.include_router(api_v1_router, prefix="/api/v1")

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Crypto Arbitrage Scanner API is running", "version": settings.VERSION}