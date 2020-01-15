from threading import Lock, Condition


class RWLock:
    def __init__(self):
        self._readtry = Lock()
        self._rlock = Lock()
        self._wlock = Lock()
        self._resource = Lock()
        self._readers = 0
        self._writers_waiting = 0

    def acquire_read(self):
        self._readtry.acquire()
        self._rlock.acquire()
        self._readers += 1
        if self._readers == 1:
            self._resource.acquire()
        self._rlock.release()
        self._readtry.release()

    def release_read(self):
        self._rlock.acquire()
        self._readers -= 1
        if self._readers == 0:
            self._resource.release()
        self._rlock.release()

    def acquire_write(self):
        self._wlock.acquire()
        self._writers_waiting += 1
        if self._writers_waiting == 1:
            self._readtry.acquire()
        self._wlock.release()
        self._resource.acquire()

    def release_write(self):
        self._wlock.acquire()
        self._writers_waiting -= 1
        if self._writers_waiting == 0:
            self._readtry.release()
        self._wlock.release()


class lock_read:
    def __init__(self, rwlock):
        self.rwlock = rwlock

    def __enter__(self):
        self.rwlock.acquire_read()

    def __exit__(self):
        self.rwlock.release_read()


class lock_write:
    def __init__(self, rwlock):
        self.rwlock = rwlock

    def __enter__(self):
        self.rwlock.acquire_write()

    def __exit__(self):
        self.rwlock.release_write()