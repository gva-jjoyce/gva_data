import sys
import logging
from typing import Callable
from .logger import get_logger

def error_trap(logger:logging.Logger=None, propagate:bool=True) -> Callable:
    """
    If function raises an un-caught exception, this decorator sends into logger 
    a failure traceback using the exception's information. In case if 
    KeyboardInterrupt exception is received, only a warning about interruption 
    is printed.

    Used like this:
        @error_trap()
        def plus_one(n):
            return n + 1

    adapted from: https://medium.com/swlh/python-errors-done-right-faa1bfa85d02
    """
    if not logger: 
        logger = get_logger()

    def wrapper(f):
        def wrapped(logger=logger, *args, **kwargs):
            try:
                return f(*args, **kwargs)
            except KeyboardInterrupt:
                logger.warning('Keyboard interruption was sent. Terminating...')
            except Exception as e:
                logger.critical(f'[{e.__class__.__name__}] {e.args[0]} - {f.__name__} \n {e}')
                if propagate:
                    raise e
        return wrapped
    return wrapper
