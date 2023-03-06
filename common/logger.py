import logging
import sys


def get_logger(name: str, verbose: bool = False) -> logging.Logger:
    """Create and return a new Logger instance with basic hardcode config."""
    # TODO: make loggers more flexible
    root_logger = logging.getLogger('')
    logger = root_logger.getChild(name)
    logger.setLevel(logging.DEBUG) if verbose else logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.DEBUG) if verbose else stream_handler.setLevel(logging.INFO)

    fmt = '[%(levelname)s %(asctime)s %(name)s] %(message)s'
    date_fmt = '%d.%m.%Y %H:%M:%S'
    stream_formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)

    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

    return logger
