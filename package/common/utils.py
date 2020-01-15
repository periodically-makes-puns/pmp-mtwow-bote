import logging
import json
import sys
from typing import Tuple, Union

time_units = [24 * 60 * 60 * 1000, 60 * 60 * 1000, 60 * 1000, 1000, 1]


class InvalidTimeStringError(Exception):
    pass

def parse_time(time: str) -> int:
    """Parses times in the format:
    format = XXDXXhXXmXXsXXX
    alt format = XX:XX:XX:XX.XXX
    """
    res = 0
    token = time.split(":")
    if len(token) > 1:
        # Use alt format
        ind = 4 - len(token)
        for i in range(len(token) - 1):  # don't process last term
            res += time_units[ind] * int(token[i])
            ind += 1
        # parse last term
        a = token[-1].partition(".")
        res += int(a[0]) * time_units[ind]
        if a[1] == ".":  # decimal point
            res += int(a[2])
    else:
        a = time.partition("d")
        if a[1] == "d":
            res += 24 * 60 * 60 * 1000 * int(a[0])
            a = a[2].partition("h")
        else:
            a = a[0].partition("h")
        if a[1] == "h":
            res += 60 * 60 * 1000 * int(a[0])
            a = a[2].partition("m")
        else:
            a = a[0].partition("m")
        if a[1] == "m":
            res += 60 * 1000 * int(a[0])
            a = a[2].partition("s")
        else:
            a = a[0].partition("s")
        if a[1] == "s":
            res += 24 * 60 * 60 * 1000 * int(a[0])
            a = a[2]
        else:
            a = a[0]
        if len(a) > 0:
            res += int(a)
    return res


class ColoredTerminalLogger(logging.Logger):
    """Logger that extends the basic logger's features to include ANSI color escape codes."""
    FAIL = "\033[38;5;9m"
    INFO = "\033[38;5;244m"
    WARN = "\033[38;5;214m"
    RESET = "\033[0m"
    DEBUG = "\033[38;5;27m"
    BOLD = "\033[1m"
    UNBOLD = "\033[21m"
    ULINE = "\033[4m"
    UNLINE = "\033[24m"

    @staticmethod
    def test():
        print(ColoredTerminalLogger.FAIL + "FAILURE!!!" + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.WARN + "WARNING!" + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.INFO + "Things you might need to know." + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.RESET + "Everything is fine." + ColoredTerminalLogger.RESET)
        print(ColoredTerminalLogger.DEBUG + "Anything and everything, here." + ColoredTerminalLogger.RESET)
        print(
            ColoredTerminalLogger.BOLD + "REALLY IMPORTANT." + ColoredTerminalLogger.UNBOLD + " Or not." + ColoredTerminalLogger.RESET)
        print(
            ColoredTerminalLogger.ULINE + "Read this." + ColoredTerminalLogger.UNLINE + " Or don't." + ColoredTerminalLogger.RESET)

    def info(self, msg: str, *args, **kwargs):
        if (self.getEffectiveLevel() <= logging.INFO):
            logging.Logger.info(self, ColoredTerminalLogger.INFO + msg + ColoredTerminalLogger.RESET, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        if (self.getEffectiveLevel() <= logging.CRITICAL):
            logging.Logger.critical(self, ColoredTerminalLogger.FAIL + msg + ColoredTerminalLogger.RESET, *args,
                                    **kwargs)

    def debug(self, msg: str, *args, **kwargs):
        if (self.getEffectiveLevel() <= logging.DEBUG):
            logging.Logger.debug(self, ColoredTerminalLogger.DEBUG + msg + ColoredTerminalLogger.RESET, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> str:
        if (self.getEffectiveLevel() <= logging.WARNING):
            logging.Logger.warning(self, ColoredTerminalLogger.WARN + msg + ColoredTerminalLogger.RESET, *args,
                                   **kwargs)

    def error(self, msg: str, *args, **kwargs) -> str:
        if (self.getEffectiveLevel() <= logging.ERROR):
            logging.Logger.error(self, ColoredTerminalLogger.FAIL + msg + ColoredTerminalLogger.RESET, *args, **kwargs)


logging.setLoggerClass(ColoredTerminalLogger)
discord_logger = logging.getLogger("discord")


def load_data(filename) -> dict:
    """Loads configuration data from filename."""
    with open(filename, "r") as f:
        data = json.load(f)
    if data is None:
        discord_logger.critical("Could not load data parameters! Aborting")
        sys.exit(1)
    if data.get("token") is None:
        discord_logger.critical("Could not load token! Aborting")
        sys.exit(1)
    if data.get("owner") is None:
        discord_logger.warning("No owner specified. Will assume owner from Discord application info.")
    if data.get("primaryServer") is None:
        discord_logger.error("No primary server specified. This bot will run without a primary server.")
    if not isinstance(data.get("portNum"), int):
        data["portNum"] = 8080
        discord_logger.warning("Invalid port number. This bot will use the default port, port 8080.")
    if data.get("prefix") is None:
        discord_logger.warning("No prefix. This bot will use the default prefix, 'p?'.")
        data["prefix"] = "p?"
    return data


data = load_data("secrets.json")
"""Contains the configuration data."""

if __name__ == "__main__":
    ColoredTerminalLogger.test()