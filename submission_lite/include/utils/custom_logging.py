import logging

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | "
    "%(name)s | %(filename)s:%(lineno)d | %(message)s"
)


def setup_logging(level=logging.INFO, log_file: str = "app_logging.log"):
    # Création du logger root
    logger = logging.getLogger()
    logger.setLevel(level)

    # Format commun
    formatter = logging.Formatter(LOG_FORMAT)

    # Handler fichier
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Évite les doublons si setup_logging est appelé plusieurs fois
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)


def get_logger(name: str):
    return logging.getLogger(name)
