import logging
import sqlite3
from threading import Condition, get_ident
from typing import Callable, Tuple
from collections import namedtuple

from package.common.rwlock import lock_read
from .sqlhandle import SQLThread

sql_thread_logger = logging.getLogger("sqlitethread")
Status = namedtuple("Status", ["id", "round_num", "prompt", "phase", "deadline", "start_time"])


def sql_utility(ret_tuple=None):
    def deco(func: Callable):
        def nfunc(thread: SQLThread, *args, **kwargs):
            cond = Condition()
            cond.acquire()
            oid = func(thread, cond, *args, **kwargs)
            cond.wait()
            res = thread.get_result(oid)
            if ret_tuple is not None:
                return map(ret_tuple._make, res)
            return res
        return nfunc
    return deco


@sql_utility()
def construct_schema(thread: SQLThread, condition: Condition):
    """Constructs the SQLite schema."""
    sql_thread_logger.debug("Thread {} is constructing schema".format(get_ident()))
    return thread.request([
        ("""CREATE TABLE IF NOT EXISTS Members (
            uid INTEGER PRIMARY KEY NOT NULL,
            aggregateVoteCount INTEGER DEFAULT 0,
            roundVoteCount INTEGER DEFAULT 0,
            timezone INTEGER DEFAULT 0,
            remindStart UNSIGNED BIG INT,
            remindInterval UNSIGNED BIG INT
        );""", ()),
        ("""CREATE TABLE IF NOT EXISTS Contestants (
            uid INTEGER PRIMARY KEY NOT NULL,
            alive BOOLEAN DEFAULT 0,
            allowedResponseCount INTEGER DEFAULT 1,
            responseCount INTEGER DEFAULT 0,
            prized INTEGER DEFAULT 0
        );""", ()),
        ("""CREATE TABLE IF NOT EXISTS Responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            uid INTEGER NOT NULL,
            rid INTEGER NOT NULL,
            response TEXT,
            confirmedVoteCount INTEGER DEFAULT 0,
            pendingVoteCount INTEGER DEFAULT 0
        );""", ()),
        ("""CREATE TABLE IF NOT EXISTS Votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            uid INTEGER NOT NULL,
            vid INTEGER NOT NULL,
            gseed TEXT UNIQUE NOT NULL,
            vote TEXT
        );""", ()),
        ("""CREATE TABLE IF NOT EXISTS Status (
            id INTEGER PRIMARY KEY,
            roundNum INTEGER,
            prompt TEXT,
            phase TEXT,
            deadline UNSIGNED BIG INT,
            startTime UNSIGNED BIG INT
        );""", ()),
        ("""INSERT OR IGNORE INTO Status VALUES (0, 1, NULL, "none", -1, -1);""", ()),
        ("""CREATE TABLE IF NOT EXISTS ResponseArchive (
            roundNum INTEGER NOT NULL,
            id INTEGER NOT NULL,
            uid INTEGER NOT NULL,
            rid INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            response TEXT NOT NULL,
            score DOUBLE NOT NULL,
            skew DOUBLE NOT NULL
        );""", ())
    ], condition)


@sql_utility()
def destroy_schema(thread: SQLThread, condition: Condition):
    sql_thread_logger.debug("Thread {} is destroying schema".format(get_ident()))
    return thread.request([
        ("DROP TABLE Members;", ()),
        ("DROP TABLE Contestants;", ()),
        ("DROP TABLE Responses;", ()),
        ("DROP TABLE Votes;", ()),
        ("DROP TABLE Status;", ()),
        ("DROP TABLE ResponseArchive;", ())
    ], condition)


@sql_utility()
def make_request(thread: SQLThread, condition: Condition, request: str, params: Tuple = ()):
    sql_thread_logger.debug("Thread {} is making request: '{}' with params {}".format(get_ident(), request, str(params)))
    return thread.request((request, params), condition)


def snapshot_schema(thread: SQLThread, filename: str):
    sql_thread_logger.debug("Thread {} is snapshotting schema to file {}".format(get_ident(), filename))
    orig = sqlite3.Connection(thread.db)
    backup = sqlite3.Connection(filename)
    with backup:
        with lock_read(thread.rwlock):
            orig.backup(backup)
    orig.close()
    backup.close()

@sql_utility(Status)
def get_status(thread: SQLThread, condition: Condition):
    sql_thread_logger.debug("Thread {} requesting mTWOW status".format(get_ident()))
    return thread.request(("SELECT * FROM Status", ()), condition)