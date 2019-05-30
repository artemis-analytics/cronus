import logging
import sys
from cronus.logger import Logger

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter(Logger.FMT))
sh.setLevel(logging.DEBUG)
logging.getLogger().addHandler(sh)

