import json
import logging
import sys
from time import strftime, gmtime

from package.common.sqlhandle import SQLThread

time_units = [24 * 60 * 60 * 1000, 60 * 60 * 1000, 60 * 1000, 1000, 1]


class InvalidTimeStringError(Exception):
    pass

def parse_time(time: str) -> int:
    """Parses times in the format:
    format = XXDXXhXXmXXsXXX
    alt format = XX:XX:XX:XX dhms
               = XX:XX:XX hms
               = XX:XX hm
    """
    res = 0
    token = time.split(":")
    if len(token) > 1:
        if len(token) > 4:
            raise InvalidTimeStringError("Too many colons")
        if len(token) == 4:
            try:
                res += 24 * 60 * 60 * 1000 * int(token[0])
            except ValueError:
                raise InvalidTimeStringError("Could not parse integer")
            del token[0]
        if len(token) == 3:
            # parse seconds
            a = token[2].partition(".")
            try:
                res += int(a[0]) * 1000
            except ValueError:
                raise InvalidTimeStringError("Could not parse integer")
            if a[1] == ".":
                try:
                    res += int(a[2])
                except ValueError:
                    raise InvalidTimeStringError("Could not parse integer")
            del token[2]
        try:
            res += 60 * 60 * 1000 * int(token[0])
            res += 60 * 1000 * int(token[1])
        except ValueError:
            raise InvalidTimeStringError("Could not parse integer")
    else:
        a = time.partition("d")
        if a[1] == "d":
            try:
                res += 24 * 60 * 60 * 1000 * int(a[0])
            except ValueError:
                raise InvalidTimeStringError("Could not parse integer")
            a = a[2].partition("h")
        else:
            a = a[0].partition("h")
        if a[1] == "h":
            try:
                res += 60 * 60 * 1000 * int(a[0])
            except ValueError:
                raise InvalidTimeStringError("Could not parse integer")
            a = a[2].partition("m")
        else:
            a = a[0].partition("m")
        if a[1] == "m":
            try:
                res += 60 * 1000 * int(a[0])
            except ValueError:
                raise InvalidTimeStringError("Could not parse integer")
            a = a[2].partition("s")
        else:
            a = a[0].partition("s")
        if a[1] == "s":
            try:
                res += 1000 * int(a[0])
            except ValueError:
                raise InvalidTimeStringError("Could not parse integer")
            a = a[2]
        else:
            a = a[0]
        if len(a) > 0:
            try:
                res += int(a)
            except ValueError:
                raise InvalidTimeStringError("Could not parse integer")
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


def name_string(user):
    return user.name + "#" + user.discriminator


def format_time(time: int):
    return strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime(time / 1000))

discord_logger = logging.getLogger("discord")

data = load_data("secrets.json")
"""Contains the configuration data."""

sqlthread = SQLThread(data["db"])
sqlthread.start()