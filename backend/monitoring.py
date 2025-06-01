# backend/monitoring.py
from prometheus_client import Counter, Histogram, start_http_server

# Метрики
arbitrage_cex_cex_count = Counter(
    "arbitrage_cex_cex_opportunities_total",
    "Total number of CEX-CEX arbitrage opportunities found"
)
arbitrage_cex_cex_cex_count = Counter(
    "arbitrage_cex_cex_cex_opportunities_total",
    "Total number of CEX-CEX-CEX arbitrage opportunities found"
)
arbitrage_search_time = Histogram(
    "arbitrage_search_duration_seconds",
    "Time spent searching for arbitrage opportunities",
    labelnames=["type"]
)

def start_prometheus_server():
    start_http_server(8001)  # Порт для Prometheus