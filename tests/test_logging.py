import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.logging import LEVELS, get_logger, error_trap, verbose_logger

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


if __name__ == "__main__":
  test_smoke_test()
