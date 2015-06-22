from cdisutils.log import get_logger
from contextlib import contextmanager

log = get_logger(__name__)


class NoMoreWorkException(Exception):
    """An exception to indicate that the process could not find work to
    do and that the supervising process (supervisord) should shut it
    down

    """
    pass


class DoNotRestartException(Exception):
    """An exception that indicates a error so fatal that the supervising
    process should not even try to restart this thing, generally a
    situation that requires human intervention. Examples of this might
    be being out of disk space, or the tcga dcc being down.

    """
    pass


@contextmanager
def zug_wrap():
    """The idea behind this is that we'll tell supervisord that 3 and 4
    are the "expected" exit codes of this process, and then have it
    only restart on "unexpected" exit codes, (such as 0, 1, and 2), so
    that it will restart the process if the finishes successfully or
    throws a normal type of exception, but will not be restarted if
    one of these two things happens.
    """
    try:
        yield
    except NoMoreWorkException:
        log.exception("No more work")
        exit(3)
    except DoNotRestartException:
        # Ideally we will someday alert a human here, since this kind of
        # problem is usually very drastic!
        log.exception("Fatal exception")
        exit(4)
