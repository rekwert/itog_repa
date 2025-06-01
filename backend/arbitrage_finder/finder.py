# backend/arbitrage_finder/finder.py
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal, InvalidOperation
import math
import redis.asyncio as redis
import json
import time
from backend.monitoring import arbitrage_cex_cex_count, arbitrage_cex_cex_cex_count, arbitrage_search_time

from backend.core.config import settings, commissions_config
from backend.data_processor.processor import data_processor
from backend.utils.logger import logger

class OpportunityCexCex:
    def __init__(self, pair: str, buy_exchange: str, sell_exchange: str, buy_price: Decimal, sell_price: Decimal, profit_percent: Decimal, volume_usd: Optional[Decimal] = None):
        self.pair = pair
        self.buy_exchange = buy_exchange
        self.sell_exchange = sell_exchange
        self.buy_price = buy_price
        self.sell_price = sell_price
        self.profit_percent = profit_percent
        self.volume_usd = volume_usd

    def __repr__(self):
        return (f"CEX-CEX Opportunity: {self.pair} | Buy on {self.buy_exchange} @ {self.buy_price} "
                f"| Sell on {self.sell_exchange} @ {self.sell_price} | Profit: {self.profit_percent:.4f}% "
                f"| Volume(USD): {self.volume_usd}")

class OpportunityCexCexCex:
    def __init__(self, cycle: List[Tuple[str, str, str]], profit_percent: Decimal, volume_usd: Optional[Decimal] = None):
        self.cycle = cycle
        self.profit_percent = profit_percent
        self.volume_usd = volume_usd

    def __repr__(self):
        cycle_str = " -> ".join([f"{action} {pair} on {exchange}" for exchange, pair, action in self.cycle])
        return (f"CEX-CEX-CEX Opportunity: {cycle_str} | Profit: {self.profit_percent:.4f}% "
                f"| Volume(USD): {self.volume_usd}")

def parse_commission_rate(commission_str: Optional[str]) -> Decimal:
    if commission_str is None:
        return Decimal(0)
    try:
        if commission_str.endswith('%'):
            return Decimal(commission_str[:-1].strip()) / Decimal(100)
        return Decimal(0)
    except (InvalidOperation, ValueError):
        logger.warning(f"Не удалось распарсить строку комиссии: '{commission_str}'. Считаем 0%.")
        return Decimal(0)

class ArbitrageFinder:
    def __init__(self):
        self._min_profit_percent = Decimal(settings.MIN_PROFIT_PERCENT)
        self._redis_client = redis.from_url(
            f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}",
            encoding="utf-8",
            decode_responses=True
        )
        self._pubsub = self._redis_client.pubsub()
        self._running = False

    async def find_cex_cex_opportunities(self) -> List[OpportunityCexCex]:
        start_time = time.time()
        with arbitrage_search_time.labels(type="cex_cex").time():
            opportunities: List[OpportunityCexCex] = []
            exchanges = settings.EXCHANGES
            configured_pairs_by_exchange = {
                exc: commissions_config.get_all_exchange_symbols(exc)
                for exc in exchanges
            }

            logger.info("Начало поиска CEX-CEX арбитража...")
            all_configured_pairs = set()
            for symbols in configured_pairs_by_exchange.values():
                all_configured_pairs.update(symbols)

            for pair in all_configured_pairs:
                for buy_exchange_id in exchanges:
                    for sell_exchange_id in exchanges:
                        if buy_exchange_id == sell_exchange_id:
                            continue
                        if pair not in configured_pairs_by_exchange.get(buy_exchange_id, []):
                            continue
                        if pair not in configured_pairs_by_exchange.get(sell_exchange_id, []):
                            continue

                        buy_orderbook_data = await data_processor.get_orderbook(buy_exchange_id, pair)
                        sell_orderbook_data = await data_processor.get_orderbook(sell_exchange_id, pair)

                        if not buy_orderbook_data:
                            buy_ticker_data = await data_processor.get_ticker(buy_exchange_id, pair)
                            if buy_ticker_data:
                                buy_ask = Decimal(str(buy_ticker_data.get('ask', 0)))
                                buy_ask_volume = Decimal(0)
                            else:
                                buy_ask = Decimal(0)
                                buy_ask_volume = Decimal(0)
                        else:
                            buy_ask = Decimal(str(buy_orderbook_data.get('ask', 0)))
                            buy_ask_volume = Decimal(str(buy_orderbook_data.get('askVolume', 0)))

                        if not sell_orderbook_data:
                            sell_ticker_data = await data_processor.get_ticker(sell_exchange_id, pair)
                            if sell_ticker_data:
                                sell_bid = Decimal(str(sell_ticker_data.get('bid', 0)))
                                sell_bid_volume = Decimal(0)
                            else:
                                sell_bid = Decimal(0)
                                sell_bid_volume = Decimal(0)
                        else:
                            sell_bid = Decimal(str(sell_orderbook_data.get('bid', 0)))
                            sell_bid_volume = Decimal(str(sell_orderbook_data.get('bidVolume', 0)))

                        if buy_ask <= Decimal(0) or sell_bid <= Decimal(0):
                            continue

                        buy_commission_str = commissions_config.get_commission(buy_exchange_id, pair, 'taker_buy_rate')
                        sell_commission_str = commissions_config.get_commission(sell_exchange_id, pair, 'taker_sell_rate') or \
                                             commissions_config.get_commission(sell_exchange_id, pair, 'taker_order_rate')
                        buy_fee_rate = parse_commission_rate(buy_commission_str)
                        sell_fee_rate = parse_commission_rate(sell_commission_str)

                        cost = buy_ask * (Decimal(1) + buy_fee_rate)
                        revenue = sell_bid * (Decimal(1) - sell_fee_rate)

                        if revenue > cost:
                            absolute_profit = revenue - cost
                            profit_percent = (absolute_profit / cost) * Decimal(100)
                            available_volume = min(buy_ask_volume, sell_bid_volume)
                            potential_volume_usd = available_volume * (buy_ask + sell_bid) / Decimal(2) if available_volume > 0 else None

                            if profit_percent >= self._min_profit_percent:
                                opportunities.append(OpportunityCexCex(
                                    pair=pair,
                                    buy_exchange=buy_exchange_id.upper(),
                                    sell_exchange=sell_exchange_id.upper(),
                                    buy_price=buy_ask,
                                    sell_price=sell_bid,
                                    profit_percent=profit_percent,
                                    volume_usd=potential_volume_usd
                                ))

            logger.info(f"Поиск CEX-CEX арбитража завершен. Найдено {len(opportunities)} возможностей.")
            arbitrage_cex_cex_count.inc(len(opportunities))
            opportunities.sort(key=lambda opp: opp.profit_percent, reverse=True)
            return opportunities

    async def find_cex_cex_cex_opportunities(self) -> List[OpportunityCexCexCex]:
        start_time = time.time()
        with arbitrage_search_time.labels(type="cex_cex_cex").time():
            opportunities: List[OpportunityCexCexCex] = []
            exchanges = settings.EXCHANGES
            configured_pairs_by_exchange = {
                exc: commissions_config.get_all_exchange_symbols(exc)
                for exc in exchanges
            }

            logger.info("Начало поиска CEX-CEX-CEX арбитража...")
            graph = []
            currencies = set()
            for exchange_id in exchanges:
                for pair in configured_pairs_by_exchange.get(exchange_id, []):
                    base, quote = pair.split('/')
                    currencies.add(base)
                    currencies.add(quote)
                    orderbook_data = await data_processor.get_orderbook(exchange_id, pair)
                    if not orderbook_data:
                        continue
                    ask = Decimal(str(orderbook_data.get('ask', 0)))
                    bid = Decimal(str(orderbook_data.get('bid', 0)))
                    ask_volume = Decimal(str(orderbook_data.get('askVolume', 0)))
                    bid_volume = Decimal(str(orderbook_data.get('bidVolume', 0)))
                    if ask <= 0 or bid <= 0:
                        continue

                    buy_fee = parse_commission_rate(commissions_config.get_commission(exchange_id, pair, 'taker_buy_rate'))
                    sell_fee = parse_commission_rate(commissions_config.get_commission(exchange_id, pair, 'taker_sell_rate') or
                                                    commissions_config.get_commission(exchange_id, pair, 'taker_order_rate'))

                    if ask > 0:
                        weight_buy = -math.log(float((Decimal(1) - buy_fee) / ask))
                        graph.append((exchange_id, base, quote, weight_buy, ask, ask_volume, 'buy'))
                    if bid > 0:
                        weight_sell = -math.log(float(bid * (Decimal(1) - sell_fee)))
                        graph.append((exchange_id, quote, base, weight_sell, bid, bid_volume, 'sell'))

            for start_currency in currencies:
                distances = {currency: float('inf') for currency in currencies}
                distances[start_currency] = 0
                predecessors = {currency: None for currency in currencies}
                actions = {currency: None for currency in currencies}
                exchanges_used = {currency: None for currency in currencies}
                volumes = {currency: None for currency in currencies}
                prices = {currency: None for currency in currencies}

                for _ in range(len(currencies) - 1):
                    for exchange_id, source, target, weight, price, volume, action in graph:
                        if distances[source] + weight < distances[target]:
                            distances[target] = distances[source] + weight
                            predecessors[target] = source
                            actions[target] = action
                            exchanges_used[target] = exchange_id
                            volumes[target] = volume
                            prices[target] = price

                for exchange_id, source, target, weight, price, volume, action in graph:
                    if distances[source] + weight < distances[target]:
                        cycle = []
                        current = target
                        seen = set()
                        while current not in seen:
                            seen.add(current)
                            cycle.append((exchanges_used[current], f"{predecessors[current]}/{current}", actions[current]))
                            current = predecessors[current]
                        cycle.append((exchanges_used[current], f"{predecessors[current]}/{current}", actions[current]))
                        cycle.reverse()

                        profit = -distances[target]
                        profit_percent = (math.exp(-profit) - 1) * 100
                        if profit_percent >= float(self._min_profit_percent):
                            volume_usd = None
                            min_volume = min(volumes[c] for c in volumes if volumes[c] is not None and c in seen)
                            if min_volume > 0:
                                avg_price = sum(prices[c] for c in prices if prices[c] is not None and c in seen) / len(seen)
                                volume_usd = min_volume * avg_price

                            opportunities.append(OpportunityCexCexCex(
                                cycle=cycle,
                                profit_percent=Decimal(str(profit_percent)),
                                volume_usd=volume_usd
                            ))

            logger.info(f"Поиск CEX-CEX-CEX арбитража завершен. Найдено {len(opportunities)} возможностей.")
            arbitrage_cex_cex_cex_count.inc(len(opportunities))
            opportunities.sort(key=lambda opp: opp.profit_percent, reverse=True)
            return opportunities

    async def start_finding_loop(self):
        self._running = True
        while self._running:
            try:
                cex_cex_opps = await self.find_cex_cex_opportunities()
                cex_cex_data = [
                    {
                        "pair": opp.pair,
                        "buy_exchange": opp.buy_exchange,
                        "sell_exchange": opp.sell_exchange,
                        "buy_price": str(opp.buy_price),
                        "sell_price": str(opp.sell_price),
                        "profit_percent": str(opp.profit_percent),
                        "volume_usd": str(opp.volume_usd) if opp.volume_usd else None
                    } for opp in cex_cex_opps
                ]
                await self._redis_client.publish("arbitrage:cex_cex", json.dumps(cex_cex_data))

                cex_cex_cex_opps = await self.find_cex_cex_cex_opportunities()
                cex_cex_cex_data = [
                    {
                        "cycle": opp.cycle,
                        "profit_percent": str(opp.profit_percent),
                        "volume_usd": str(opp.volume_usd) if opp.volume_usd else None
                    } for opp in cex_cex_cex_opps
                ]
                await self._redis_client.publish("arbitrage:cex_cex_cex", json.dumps(cex_cex_cex_data))

                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Ошибка в фоновом поиске арбитража: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def stop_finding_loop(self):
        self._running = False
        await self._redis_client.close()

arbitrage_finder = ArbitrageFinder()