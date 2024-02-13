"""Testing library"""

from re import match

from pydantic import __version__

PYD_VERSION = match(r"(\d+.\d+).\d+", __version__).group(1)
