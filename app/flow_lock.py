"""One owner of flow mutations: live worker or explicit offline maintenance."""
from contextlib import contextmanager
import os


@contextmanager
def flow_writer_lock(database):
    import fcntl
    path = str(database) + '.offline-drain.lock'
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Flow writer lock is held') from None
        yield
    finally:
        os.close(fd)
