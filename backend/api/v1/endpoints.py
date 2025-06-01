# backend/api/v1/endpoints.py
from typing import List, Tuple, Optional
from fastapi import APIRouter, HTTPException, WebSocket
from pydantic import BaseModel, ConfigDict, field_serializer
from decimal import Decimal
import asyncio
import redis.asyncio as redis
import json

from backend.core.config import settings
from backend.arbitrage_finder.finder import arbitrage_finder, OpportunityCexCex
from backend.utils.logger import logger

router = APIRouter()

class OpportunityCexCexResponse(BaseModel):
    pair: str
    buy_exchange: str
    sell_exchange: str
    buy_price: Decimal
    sell_price: Decimal
    profit_percent: Decimal
    volume_usd: Optional[Decimal] = None

    @field_serializer('buy_price', 'sell_price', 'profit_percent', 'volume_usd', when_used='json')
    def serialize_decimal(self, value: Decimal) -> str:
        return str(value) if value is not None else None

    model_config = ConfigDict(from_attributes=False)

class OpportunityCexCexCexResponse(BaseModel):
    cycle: List[Tuple[str, str, str]]
    profit_percent: Decimal
    volume_usd: Optional[Decimal] = None

    @field_serializer('profit_percent', 'volume_usd', when_used='json')
    def serialize_decimal(self, value: Decimal) -> str:
        return str(value) if value is not None else None

    model_config = ConfigDict(from_attributes=False)

@router.get("/arbitrage/cex_cex", response_model=List[OpportunityCexCexResponse], tags=["Arbitrage"])
async def get_cex_cex_opportunities():
    logger.info("Получен запрос на /api/v1/arbitrage/cex_cex")
    try:
        opportunities = await arbitrage_finder.find_cex_cex_opportunities()
        response_opportunities = [
            OpportunityCexCexResponse(
                pair=opp.pair,
                buy_exchange=opp.buy_exchange,
                sell_exchange=opp.sell_exchange,
                buy_price=opp.buy_price,
                sell_price=opp.sell_price,
                profit_percent=opp.profit_percent,
                volume_usd=opp.volume_usd
            ) for opp in opportunities
        ]
        logger.info(f"Ответ на /api/v1/arbitrage/cex_cex: {len(response_opportunities)} возможностей найдено.")
        return response_opportunities
    except Exception as e:
        logger.error(f"Ошибка при поиске CEX-CEX арбитража: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера при поиске арбитража")

@router.get("/arbitrage/cex_cex_cex", response_model=List[OpportunityCexCexCexResponse], tags=["Arbitrage"])
async def get_cex_cex_cex_opportunities():
    logger.info("Получен запрос на /api/v1/arbitrage/cex_cex_cex")
    try:
        opportunities = await arbitrage_finder.find_cex_cex_cex_opportunities()
        response_opportunities = [
            OpportunityCexCexCexResponse(
                cycle=opp.cycle,
                profit_percent=opp.profit_percent,
                volume_usd=opp.volume_usd
            ) for opp in opportunities
        ]
        logger.info(f"Ответ на /api/v1/arbitrage/cex_cex_cex: {len(response_opportunities)} возможностей найдено.")
        return response_opportunities
    except Exception as e:
        logger.error(f"Ошибка при поиске CEX-CEX-CEX арбитража: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера при поиске арбитража")

@router.websocket("/ws/arbitrage/cex_cex")
async def websocket_cex_cex_opportunities(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket подключен для /ws/arbitrage/cex_cex")
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        decode_responses=True
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("arbitrage:cex_cex")
    try:
        async for message in pubsub.listen():
            if message['type'] == 'message':
                await websocket.send_text(message['data'])
    except Exception as e:
        logger.error(f"Ошибка в WebSocket /ws/arbitrage/cex_cex: {e}", exc_info=True)
    finally:
        await pubsub.unsubscribe("arbitrage:cex_cex")
        await redis_client.aclose()
        await websocket.close()

@router.websocket("/ws/arbitrage/cex_cex_cex")
async def websocket_cex_cex_cex_opportunities(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket подключен для /ws/arbitrage/cex_cex_cex")
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        decode_responses=True
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("arbitrage:cex_cex_cex")
    try:
        async for message in pubsub.listen():
            if message['type'] == 'message':
                await websocket.send_text(message['data'])
    except Exception as e:
        logger.error(f"Ошибка в WebSocket /ws/arbitrage/cex_cex_cex: {e}", exc_info=True)
    finally:
        await pubsub.unsubscribe("arbitrage:cex_cex_cex")
        await redis_client.aclose()
        await websocket.close()