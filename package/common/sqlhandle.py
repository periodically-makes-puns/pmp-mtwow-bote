import sqlite3
from collections import namedtuple
from queue import Queue
from threading import Thread, Event, Condition
from time import time_ns
from typing import Union, List, Tuple, Dict

from .rwlock import RWLock, lock_read, lock_write


class SQLThreadShuttingDownError(Exception):
    pass


class CleanupTimer(Thread):
    def __init__(self, thread):
        Thread.__init__(self)
        self.stopped = Event()
        self.sqlthread = thread

    def run(self):
        while not self.stopped.wait(100):
            self.sqlthread.cleanup()


Result = namedtuple("Result", ["res", "time"])

class SQLThread(Thread):
    """A thread that handles all SQL operations, including reads and writes."""
    results: Dict[int, Union[Condition, namedtuple]]
    ops: Queue
    shutdown: Event
    opcount: int
    rwlock: RWLock
    conn: sqlite3.Connection
    cleanup: CleanupTimer

    def __init__(self, db: str):
        Thread.__init__(self)
        self.results = dict()
        self.ops = Queue()
        self.shutdown = Event()
        self.opcount = 0
        self.rwlock = RWLock()
        self.conn = sqlite3.Connection(db)
        self.cleanup = CleanupTimer(self)

    def run(self):
        while not self.shutdown.is_set():
            oid, ops = self.ops.get()
            cursor = self.conn.cursor()
            if isinstance(ops, list):
                for statement, params in ops:
                    cursor.execute(statement, params)
                pass
            else:
                statement, params = ops
                cursor.execute(statement, params)
            res = cursor.fetchall()
            with lock_read(self.rwlock):
                cond = self.results[oid]
            with lock_write(self.rwlock):
                self.results[oid] = Result(res=res, time=time_ns())
            cond.notify_all()
        with lock_write(self.rwlock):
            while not self.ops.empty():
                oid = self.ops.get()[0]
                cond = self.results[oid]
                self.results[oid] = Result(res=SQLThreadShuttingDownError("Thread is shutting down"), time=time_ns())
                cond.notify_all()
        self.cleanup.cancel()

    def close(self):
        self.shutdown.set()

    def request(self, query: Union[Tuple[str, Tuple], List[Tuple[str, Tuple]]]) -> Tuple[int, Condition]:
        if self.shutdown.set(): raise SQLThreadShuttingDownError("Thread is shutting down")
        with lock_write(self.rwlock):
            oid = self.opcount
            self.ops.put((oid, query))
            self.results[oid] = Condition()
            self.opcount += 1
            cond = self.results[oid]
        return oid, cond

    def get_result(self, oid: int):
        with lock_read(self.rwlock):
            if isinstance(self.results[oid], Result):
                return self.results[oid].res

    def cleanup(self):
        curtime = time_ns()
        with lock_write(self.rwlock):
            for k, v in self.results.items():
                if isinstance(v, Result) and v.time - curtime > 100000000000:
                    # if 100 seconds elapsed and did not read result, delete
                    del self.results[k]