# tests/test_arbitrage.py
import pytest
import asyncio
from decimal import Decimal
from fastapi.testclient import TestClient
from backend.api.v1.endpoints import router as api_v1_router
from backend.arbitrage_finder.finder import ArbitrageFinder, OpportunityCexCex, OpportunityCexCexCex
from backend.core.config import settings, commissions_config
from backend.data_processor.processor import data_processor
from backend.main import app

# Настраиваем тестовый клиент FastAPI
client = TestClient(app)


@pytest.mark.asyncio
async def test_cex_cex_arbitrage():
    """Тестирует логику поиска CEX-CEX арбитража."""
    await data_processor.connect_redis()
    await data_processor.cache_orderbook(
        exchange_id="binance",
        symbol="BTC/USDT",
        orderbook_data={
            "ask": 50000,
            "askVolume": 1,
            "bid": 49000,
            "bidVolume": 1,
            "timestamp": 1234567890
        }
    )
    await data_processor.cache_orderbook(
        exchange_id="bybit",
        symbol="BTC/USDT",
        orderbook_data={
            "ask": 48000,
            "askVolume": 1,
            "bid": 51000,
            "bidVolume": 1,
            "timestamp": 1234567890
        }
    )

    finder = ArbitrageFinder()
    opportunities = await finder.find_cex_cex_opportunities()

    assert len(opportunities) > 0
    opp = opportunities[0]
    assert isinstance(opp, OpportunityCexCex)
    assert opp.pair == "BTC/USDT"
    assert opp.buy_exchange == "BYBIT"
    assert opp.sell_exchange == "BINANCE"
    assert opp.profit_percent > 0
    await data_processor.disconnect_redis()


@pytest.mark.asyncio
async def test_cex_cex_cex_arbitrage():
    """Тестирует логику поиска CEX-CEX-CEX арбитража."""
    await data_processor.connect_redis()
    await data_processor.cache_orderbook(
        exchange_id="binance",
        symbol="BTC/USDT",
        orderbook_data={"ask": 50000, "askVolume": 1, "bid": 49000, "bidVolume": 1}
    )
    await data_processor.cache_orderbook(
        exchange_id="bybit",
        symbol="ETH/BTC",
        orderbook_data={"ask": 0.05, "askVolume": 1, "bid": 0.04, "bidVolume": 1}
    )
    await data_processor.cache_orderbook(
        exchange_id="mexc",
        symbol="ETH/USDT",
        orderbook_data={"ask": 2500, "askVolume": 1, "bid": 2600, "bidVolume": 1}
    )

    finder = ArbitrageFinder()
    opportunities = await finder.find_cex_cex_cex_opportunities()

    assert len(opportunities) > 0
    opp = opportunities[0]
    assert isinstance(opp, OpportunityCexCexCex)
    assert len(opp.cycle) == 3
    assert opp.profit_percent > 0
    await data_processor.disconnect_redis()


@pytest.mark.asyncio
async def test_api_cex_cex_endpoint():
    """Тестирует эндпоинт /api/v1/arbitrage/cex_cex."""
    await data_processor.connect_redis()
    await data_processor.cache_orderbook(
        exchange_id="binance",
        symbol="BTC/USDT",
        orderbook_data={"ask": 50000, "askVolume": 1, "bid": 49000, "bidVolume": 1}
    )
    await data_processor.cache_orderbook(
        exchange_id="bybit",
        symbol="BTC/USDT",
        orderbook_data={"ask": 48000, "askVolume": 1, "bid": 51000, "bidVolume": 1}
    )

    response = client.get("/api/v1/arbitrage/cex_cex")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert data[0]["pair"] == "BTC/USDT"
    assert data[0]["buy_exchange"] == "BYBIT"
    assert data[0]["sell_exchange"] == "BINANCE"
    await data_processor.disconnect_redis()