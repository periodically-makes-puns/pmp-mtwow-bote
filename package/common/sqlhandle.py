import logging
import sqlite3
from collections import namedtuple
from queue import Queue
from threading import Thread, Event, Condition, get_ident, Lock, RLock
from time import time_ns
from typing import Union, List, Tuple, Dict

from .rwlock import RWLock, lock_read, lock_write

sql_thread_logger = logging.getLogger("sqlitethread")


class SQLThreadShuttingDownError(Exception):
    pass


class CleanupTimer(Thread):
    """A Timer that handles cleaning up the results dictionary regularly."""
    def __init__(self, thread, time):
        Thread.__init__(self)
        self.stopped = Event()
        self.sqlthread = thread
        self.cleantime = time

    def run(self):
        sql_thread_logger.debug("Garbage collector thread {}".format(get_ident()))
        while not self.stopped.wait(self.cleantime):
            sql_thread_logger.debug("Collecting garbage")
            self.sqlthread.cleanup()
            sql_thread_logger.debug("Finished collecting garbage")


Result = namedtuple("Result", ["res", "time"])

class SQLThread(Thread):
    """A thread that handles all SQL operations, including reads and writes."""
    _results: Dict[int, Union[Condition, namedtuple]]
    _ops: Queue
    sql_resource: Lock
    shutdown: Event
    _opcount: int
    rwlock: RWLock
    conn: sqlite3.Connection
    cleanup: CleanupTimer
    db: str
    atomic: RLock

    def __init__(self, db: str = ":memory:", cleantime: int = 1200):
        Thread.__init__(self)
        self._results = dict()
        self._ops = Queue()
        self.shutdown = Event()
        self._opcount = 0
        self.rwlock = RWLock()
        self._cthread = CleanupTimer(self, cleantime)
        self.db = db
        self.sql_resource = Lock()
        self.atomic = RLock()

    def run(self):
        self.shutdown.clear()
        self._cthread.start()
        self.conn = sqlite3.Connection(self.db)
        while not self.shutdown.is_set():
            oid, ops = self._ops.get()
            sql_thread_logger.debug("Handling oid {:d}".format(oid))
            self.sql_resource.acquire()
            cursor = self.conn.cursor()
            try:
                if isinstance(ops, list):
                    for statement, params in ops:
                        cursor.execute(statement, params)
                else:
                    statement, params = ops
                    cursor.execute(statement, params)
                res = cursor.fetchall()
            except sqlite3.Error as e:
                res = e
            self.conn.commit()
            self.sql_resource.release()
            with lock_read(self.rwlock):
                cond = self._results[oid]
            with lock_write(self.rwlock):
                self._results[oid] = Result(res=res, time=time_ns())
            sql_thread_logger.debug("Finished processing, now notifying")
            cond.acquire()
            sql_thread_logger.debug("Acquired, notifying")
            cond.notify_all()
            cond.release()
        with lock_write(self.rwlock):
            while not self._ops.empty():
                oid = self._ops.get()[0]
                cond = self._results[oid]
                self._results[oid] = Result(res=SQLThreadShuttingDownError("Thread is shutting down"), time=time_ns())
                cond.acquire()
                cond.notify_all()
        self._cthread.stopped.set()

    def close(self):
        self._cthread.stopped.set()
        self.shutdown.set()

    def request(self, query: Union[Tuple[str, Tuple], List[Tuple[str, Tuple]]], cond: Condition) -> int:
        """Makes a request, with a condition flag that should be used to
        check when the request has finished.

        Precondition: cond must be acquired by the calling thread.
        cond must be waited on or released after this function call."""
        if self.shutdown.is_set(): raise SQLThreadShuttingDownError("Thread is shutting down")
        sql_thread_logger.debug("Request from thread {}".format(get_ident()))
        self.atomic.acquire()
        with lock_write(self.rwlock):
            oid = self._opcount
            self._ops.put((oid, query))
            self._results[oid] = cond
            self._opcount += 1
        self.atomic.release()
        return oid

    def get_result(self, oid: int):
        """Obtain result for an OID if it is available.
        If your condition is not notified, then there is no guarantee
        this method will not return None."""
        with lock_read(self.rwlock):
            if isinstance(self._results[oid], Result):
                return self._results[oid].res
        return

    def cleanup(self):
        """A cleanup function to save memory in the results dictionary."""
        curtime = time_ns()
        sql_thread_logger.debug("Thread {} is cleaning up".format(get_ident()))
        with lock_write(self.rwlock):
            for k, v in self._results.items():
                if isinstance(v, Result) and v.time - curtime > self._cthread.cleantime * 1000000000:
                    # if 100 seconds elapsed and did not read result, delete
                    del self._results[k]