import re
from consulate import Consul
from threading import Thread, current_thread
import thread as thread_module
from cdisutils.log import get_logger
import time
from contextlib import contextmanager


class StoppableThread(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        super(StoppableThread, self).__init__(*args, **kwargs)
        self._stop = False

    def stop(self):
        self._stop = True

    def stopped(self):
        return self._stop


class ConsulManager(object):
    '''
    Consul Manager class for utilizing consul key value store
    @param prefix: consul key prefix, default to class name
    @param consul_key: class attribute that will be used to acquire lock, default to self.key
    '''
    def __init__(self, prefix='', debug=True):
        self.consul = Consul()
        self.key_acquired = False
        self.heartbeat_thread = None
        self.consul_session = None
        self.logger = get_logger('consul_manager')
        self.should_have_lock = False
        self.debug = debug
        if not debug:
            self.logger.level = 30
        if prefix:
            self.consul_prefix = prefix
        else:
            self.consul_prefix = self.__class__.__name__.lower()

    def consul_heartbeat(self, debug=True):
        """Heartbeat with consul to keep `self.session` alive every
        `self.interval` seconds. This must be called as the `target`
        of a `StoppableThread`.
        """
        logger = get_logger("consul_heartbeat_thread")
        if not debug:
            logger.level = 30
        thread = current_thread()
        logger.info("current thread is %s", thread)
        while not thread.stopped():
            try:
                logger.debug("renewing consul session %s", self.consul_session)
                ret = self.consul.session.renew(self.consul_session)
                if isinstance(ret, basestring) and "not found" in ret:
                    self.key_acquired = False  # key is lost with session
                    logger.info("consul session %s appears to have been invalidated, creating new session",
                                self.consul_session)
                    # session has been invalidated, get a new session and
                    # reaquire lock if necessary
                    self.consul_session = self.consul.session.create(
                        behavior=self.behavior,
                        ttl=self.ttl,
                        delay=self.delay,
                    )
                    logger.info("got new consul session: %s", self.consul_session)
                if self.should_have_lock and not self.key_acquired:
                    logger.info("We previously had a lock on %s, attempting reaquisition after waiting %s seconds",
                                self.consul_key, 2*self.delay_seconds)
                    time.sleep(2*self.delay_seconds)
                    self.key_acquired = self.consul.kv.acquire_lock(
                        self.consul_key, self.consul_session)
                    if self.key_acquired:
                        logger.info("Lock successfully reaquired")
                    else:
                        logger.warning("Could not reaquire lock!")
            except Exception as e:
                logger.info("Caught %s: %s while trying to consul heartbeat, retrying",
                            e.__class__, e)
            finally:
                time.sleep(self.interval)

    @property
    def consul_key(self):
        return "{}/current/{}".format(
            self.consul_prefix, self._key)

    def consul_get(self, path, default=''):
        if not hasattr(path, '__iter__'):
            path = [path]
        return self.consul.kv.get("/".join([self.consul_prefix] + path),default)

    def consul_set(self, path, value):
        if not hasattr(path, '__iter__'):
            path = [path]
        self.consul.kv["/".join([self.consul_prefix] + path)] = value

    def consul_key_set(self, value):
        if self.key_acquired:
            self.consul.kv.set(self.consul_key, value)
            return True
        else:
            self.logger.warn("the key is not acquired yet")
            return False

    def set_consul_state(self, state):
        if self.key_acquired:
            current = self.consul.kv.get(self.consul_key)
            current["state"] = state
            self.logger.info("Setting %s to %s", self.consul_key, current)
            self.consul.kv.set(self.consul_key, current)
            return True
        else:
            self.logger.warn("Lock is not acquired yet")
            return False

    def get_consul_lock(self, key):
        self._key = key
        if self.consul_session:
            self.logger.info(
                "Attempting to lock %s in consul", self.consul_key)
            self.key_acquired = self.consul.kv.acquire_lock(
                self.consul_key, self.consul_session)
            self.should_have_lock = True
            return self.key_acquired
        else:
            self.logger.error("Consul session not started")
            return False

    def list_locked_keys(self):
        current = [key.split("/")[-1] for key in
                   self.consul.kv.find(
                       "/".join([self.consul_prefix, "current"]))]
        self.logger.info(
            "there are %s keys currently being synced", len(current))
        return current

    def start_consul_session(self, behavior='delete', ttl='60s',
                             delay='15s', interval=10):
        self.logger.info("Starting new consul session")
        self.behavior = behavior
        self.ttl = ttl
        assert re.match("\d*s$", delay)
        self.delay = delay
        self.delay_seconds = int(delay.strip("s"))
        self.interval = interval
        self.consul_session = self.consul.session.create(
            behavior=self.behavior,
            ttl=self.ttl,
            delay=self.delay,
        )
        self.logger.info(
            "Consul session %s started, forking thread to heartbeat",
            self.consul_session)
        self.heartbeat_thread = StoppableThread(target=self.consul_heartbeat,
                                                args=(self.debug,))
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    @contextmanager
    def consul_session_scope(self, behavior='delete', ttl='60s', delay='15s'):
        try:
            yield self.start_consul_session(behavior=behavior, ttl=ttl, delay=delay)
        finally:
            self.cleanup()

    def cleanup(self):
        self.logger.info("Stopping consul heartbeat thread")
        if self.heartbeat_thread:
            self.heartbeat_thread.stop()
            self.logger.info("Waiting to join heartbeat thread . . .")
            self.heartbeat_thread.join(20)
            if self.heartbeat_thread.is_alive():
                self.logger.warning(
                    "Joining heartbeat thread failed after 20 seconds!")
            self.logger.info("Invalidating consul session")
            self.consul.session.destroy(self.consul_session)
