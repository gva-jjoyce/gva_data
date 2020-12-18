import logging
from .add_level import add_logging_level
from functools import lru_cache

LOGNAME:str = "GVA"

class LEVELS():
    """
    Proxy the Logging levels so we can just reference these without
    having to import Logging everywhere.
    """
    DEBUG = int(logging.DEBUG)          # 10
    INFO = int(logging.INFO)            # 20
    WARNING = int(logging.WARNING)      # 30
    ERROR = int(logging.ERROR)          # 40
    CRITICAL = int(logging.CRITICAL)    # 50
    TRACE = 100                         # trace is higher priority

@lru_cache(1)
def get_logger() -> logging.Logger:
    """
    Use Python's native logging - we created a named logger so we can make sure
    only the logs related to our jobs are included (other modules also use the
    Python's logging module).
    """
    logger = logging.getLogger(LOGNAME)

    # add the TRACE level to the logger
    #if not hasattr(logging, "trace"):
    add_logging_level("TRACE", LEVELS.TRACE)

    # configure the logger
    fh = logging.StreamHandler()    
    formatter = logging.Formatter('[%(name)s] [%(levelname)-8s] [%(asctime)s] [%(filename)s:%(lineno)s] [%(funcName)s()] - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
