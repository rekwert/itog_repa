import logging
import sys

def setup_logger():
    """Настраивает базовое логирование."""
    logging.basicConfig(
        level=logging.INFO, # Можно поменять на DEBUG для более подробного вывода
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logger()