import logging
from threading import Lock, get_ident

sql_thread_logger = logging.getLogger("sqlite_thread")


class RWLock:
    """A class implementing a read/write lock with writer priority.

    """
    _readtry: Lock
    _rlock: Lock
    _wlock: Lock
    _resource: Lock
    _readers: int
    _writers_waiting: int

    def __init__(self):
        """Constructs a default RWlock."""

        self._readtry = Lock()
        """Lock that locks out readers until a writer is finished."""

        self._rlock = Lock()
        """Lock for readers to prevent race conditions."""

        self._wlock = Lock()
        """Lock for writers to prevent race conditions."""

        self._resource = Lock()
        """Lock for the resource overall."""

        self._readers = 0
        """Number of readers currently active."""

        self._writers_waiting = 0
        """Number of writers waiting to acquire the resource."""

    def acquire_read(self):
        """Called by any reader to acquire a read lock. Will block until all writers release the resource."""
        sql_thread_logger.debug("Thread {} acquiring read".format(get_ident()))
        self._readtry.acquire()
        sql_thread_logger.debug("Thread {}: All writers finished".format(get_ident()))
        self._rlock.acquire()
        self._readers += 1
        if self._readers == 1:
            self._resource.acquire()
        self._rlock.release()
        self._readtry.release()
        sql_thread_logger.debug("Thread {} acquired read".format(get_ident()))

    def release_read(self):
        """Called by any reader to release a read lock. Will block until all writers release the resource."""
        sql_thread_logger.debug("Thread {} releasing read".format(get_ident()))
        self._rlock.acquire()
        self._readers -= 1
        if self._readers == 0:
            self._resource.release()
        self._rlock.release()
        sql_thread_logger.debug("Thread {} released read".format(get_ident()))

    def acquire_write(self):
        """Called by any writer to acquire a write lock. Will block until resource is available."""
        sql_thread_logger.debug("Thread {} acquiring write".format(get_ident()))
        self._wlock.acquire()
        self._writers_waiting += 1
        if self._writers_waiting == 1:
            self._readtry.acquire()
        self._wlock.release()
        self._resource.acquire()
        sql_thread_logger.debug("Thread {} acquired write".format(get_ident()))

    def release_write(self):
        """Called by any writer to release a write Lock. Will block until resource is available."""
        sql_thread_logger.debug("Thread {} releasing write".format(get_ident()))
        self._wlock.acquire()
        self._writers_waiting -= 1
        if self._writers_waiting == 0:
            self._readtry.release()
            self._resource.release()
        self._wlock.release()
        sql_thread_logger.debug("Thread {} released write".format(get_ident()))


class lock_read:
    """"Utility context manager for read locks."""
    def __init__(self, rwlock):
        self.rwlock = rwlock

    def __enter__(self):
        self.rwlock.acquire_read()

    def __exit__(self, type, value, traceback):
        self.rwlock.release_read()


class lock_write:
    """Utility context manager for write locks."""
    def __init__(self, rwlock):
        self.rwlock = rwlock

    def __enter__(self):
        self.rwlock.acquire_write()

    def __exit__(self, type, value, traceback):
        self.rwlock.release_write()