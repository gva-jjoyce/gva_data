"""
Test the gva logger, this extends the Python logging logger.
We test that the trace method and decorators raise no errors.
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.logging import LEVELS, get_logger, error_trap, verbose_logger
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

def alert():
  pass
  
def test_smoke_test():
  """
  This is just a smoke test, it exercises most of the logging functionality
  it should just work.

  There's no asserts in testing the logger
  """
  logger = get_logger()
  logger.setLevel(LEVELS.DEBUG)
  logger.trace("trace")
  logger.debug("debug")
  logger.info("info")
  logger.warning("warn")
  logger.error("error")
  logger.critical("critical")

  @error_trap(propagate=False)
  def i_fail():
      return 0 / 0

  @verbose_logger()
  def i_pass(key="value"):
    return key * 2

  i_fail()
  i_pass(key="invaluable")


def test_logger_errors():

  from gva.logging import add_level

  add_level.add_logging_level('alert', 25)

  failed = False
  try:
    add_level.add_logging_level('alert', 25)
  except:
    failed = True
  assert failed

  failed = False
  try:
    add_level.add_logging_level('none', 25, alert)
  except:
    failed = True
  assert failed


if __name__ == "__main__":
  test_smoke_test()
  test_logger_errors()
