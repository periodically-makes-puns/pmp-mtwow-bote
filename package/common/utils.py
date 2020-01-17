import json
import logging
import sys


from package.common.sqlhandle import SQLThread

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


def load_data(filename) -> dict:
    """Loads configuration data from file."""
    global discord_logger

    with open(filename, "r") as f:
        data = json.load(f)
    if data is None:
        discord_logger.critical("Could not load data parameters! Aborting")
        sys.exit(1)
    if data.get("token") is None:
        discord_logger.critical("Could not load token! Aborting")
        sys.exit(1)
    if data.get("db") is None:
        discord_logger.warning("No database file specified. Will assume db filename of 'mtwow.sqlite'.")
        data["db"] = "mtwow.sqlite"
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


discord_logger = logging.getLogger("discord")

data = load_data("secrets.json")
"""Contains the configuration data."""

sqlthread = SQLThread(data["db"])
sqlthread.start()

if __name__ == "__main__":
    ColoredTerminalLogger.test()