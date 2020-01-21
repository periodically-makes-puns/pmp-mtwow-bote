import logging
import sqlite3
from threading import Condition, get_ident
from typing import Callable, Tuple
from collections import namedtuple
from time import time_ns

from package.common.rwlock import lock_read
from .sqlhandle import SQLThread

sql_thread_logger = logging.getLogger("sqlitethread")
Status = namedtuple("Status", ["id", "round_num", "prompt", "phase", "deadline", "start_time"])
Contestant = namedtuple("Contestant", ["uid", "alive", "response_count", "allowed_response_count", "prized"])
Response = namedtuple("Response", ["id", "uid", "rid", "response", "word_count", "confirmed_vote_count", "pending_vote_count"])
Member = namedtuple("Member", ["uid", "vid", "total_votes", "round_votes", "timezone", "remind_in", "remind_every"])
Vote = namedtuple("Vote", ["id", "vid", "vote_num", "seed", "vote"])
Result = namedtuple("Result", ["round_num", "id", "uid", "rid", "rank", "response", "score", "skew"])


def sql_get(ret_tuple=None):
    """Decorator method that casts SQL rows to a named tuple, or leaves them as is if no arg given"""
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

def sql_run(handler: Callable = None, *params, **kwparams):
    """Decorator method that allows for error handling"""
    def deco(func: Callable):
        def nfunc(thread: SQLThread, *args, **kwargs):
            cond = Condition()
            cond.acquire()
            oid = func(thread, cond, *args, **kwargs)
            cond.wait()
            res = thread.get_result(oid)
            if isinstance(res, sqlite3.Error):
                if handler is not None:
                    handler(res, *params, **kwparams)
                    return None
                else:
                    raise res
            else:
                return res
        return nfunc
    return deco


@sql_get()
def construct_schema(thread: SQLThread, condition: Condition):
    """Constructs the SQLite schema."""
    sql_thread_logger.debug("Thread {} is constructing schema".format(get_ident()))
    return thread.request([
        ("""CREATE TABLE IF NOT EXISTS Members (
            uid INTEGER PRIMARY KEY NOT NULL,
            vid INTEGER UNIQUE,
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
            wordCount INTEGER,
            confirmedVoteCount INTEGER DEFAULT 0,
            pendingVoteCount INTEGER DEFAULT 0
        );""", ()),
        ("""CREATE TABLE IF NOT EXISTS Votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            vid INTEGER NOT NULL,
            vnum INTEGER NOT NULL,
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


@sql_get()
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


@sql_get()
def get(thread: SQLThread, condition: Condition, request: str, params: Tuple[str] = ()):
    sql_thread_logger.debug("Thread {} is making request: '{}' with params {}".format(get_ident(), request, " ".join(params)))
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


@sql_get(Status)
def get_status(thread: SQLThread, condition: Condition):
    sql_thread_logger.debug("Thread {} requesting mTWOW status".format(get_ident()))
    return thread.request(("SELECT * FROM Status", ()), condition)


@sql_get(Contestant)
def get_contestant(thread: SQLThread, condition: Condition, uid: int):
    sql_thread_logger.debug("Thread {} requesting Contestant {}'s data".format(get_ident(), uid))
    return thread.request(("SELECT * FROM Contestants WHERE uid = ?;", (uid,)), condition)


@sql_get(Member)
def get_voter(thread: SQLThread, condition: Condition, *, uid: int, vid: int):
    if uid is not None:
        sql_thread_logger.debug("Thread {} requesting Member with UID {}'s data".format(get_ident(), uid))
        return thread.request(("SELECT * FROM Members WHERE uid = ?;", (uid, )), condition)
    elif vid is not None:
        sql_thread_logger.debug("Thread {} requesting Member with VID {}'s data".format(get_ident(), uid))
        return thread.request(("SELECT * FROM Members WHERE vid = ?;", (vid, )), condition)
    raise sqlite3.Error("No arguments provided to voter get function")


@sql_get(Vote)
def get_vote(thread: SQLThread, condition: Condition, vid: int, votenum: int):
    sql_thread_logger.debug("Thread {} requesting vote {} of the user with VID {}".format(get_ident(), votenum, vid))
    return thread.request(("SELECT * FROM Votes WHERE vid = ? AND vnum = ?;", (vid, votenum)), condition)


@sql_get(Response)
def get_response(thread: SQLThread, condition: Condition, uid: int, rid: int):
    sql_thread_logger.debug("Thread {} requesting response {} of the user with VID {}".format(get_ident(), rid, uid))
    return thread.request(("SELECT * FROM Votes WHERE uid = ? AND rid = ?;", (uid, rid)), condition)


@sql_get()
def get_vids(thread: SQLThread, condition: Condition):
    sql_thread_logger.debug("Thread {} requesting list of VIDs".format(get_ident()))
    return thread.request(("SELECT vid FROM Members WHERE vid NOT NULL;", ()), condition)


@sql_get(Response)
def get_responses(thread: SQLThread, condition: Condition, uid: int):
    sql_thread_logger.debug("Thread {} requesting list of responses from UID {}".format(get_ident(), uid))
    return thread.request(("SELECT * FROM Responses WHERE uid = ?;", (uid,)), condition)


@sql_get(Vote)
def get_votes(thread: SQLThread, condition: Condition, vid: int):
    sql_thread_logger.debug("Thread {} requesting list of votes from VID {}".format(get_ident(), vid))
    return thread.request(("SELECT * FROM Votes WHERE vid = ?;", (vid,)), condition)


def uid2vid(thread: SQLThread, uid: int):
    cond = Condition()
    cond.acquire()
    oid = thread.request(("SELECT vid FROM Members WHERE uid = ?;", (uid,)), cond)
    cond.wait()
    return thread.get_result(oid)[0][0]


def vid2uid(thread: SQLThread, vid: int):
    cond = Condition()
    cond.acquire()
    oid = thread.request(("SELECT uid FROM Members WHERE vid = ?;", (vid,)), cond)
    cond.wait()
    return thread.get_result(oid)[0][0]


@sql_run()
def run(thread: SQLThread, condition: Condition, request: str, params: Tuple[str] = ()):
    sql_thread_logger.debug("Thread {} is making request: '{}' with params {}".format(get_ident(), request, " ".join(params)))
    return thread.request((request, params), condition)


@sql_run()
def set_time(thread: SQLThread, condition: Condition, start_time: int, time_left: int):
    sql_thread_logger.debug("Thread {} is setting startTime to {} and deadline to {}"
                            .format(get_ident(), start_time, time_left))
    return thread.request(("UPDATE Status SET startTime = ?, deadline = ?;", (start_time, time_left)), condition)


def get_deadline(thread: SQLThread):
    sql_thread_logger.debug("Thread {} requesting the deadline.".format(get_ident()))
    cond = Condition()
    cond.acquire()
    oid = thread.request(("SELECT startTime, deadline FROM Status;", ()), cond)
    cond.wait()
    return thread.get_result(oid)[0][0] + thread.get_result(oid)[0][1]


def update_timers(thread: SQLThread):
    thread.atomic.acquire()
    cond = Condition()
    ctime = time_ns() // 1000000
    deadline = get_deadline(thread)
    set_time(thread, ctime, deadline - ctime)
    thread.atomic.release()