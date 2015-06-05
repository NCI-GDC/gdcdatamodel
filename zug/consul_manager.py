from consulate import Consul
from threading import Thread, current_thread
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


def consul_heartbeat(session, interval, debug=True):
    """
    Heartbeat with consul to keep `session` alive every `interval`
    seconds. This must be called as the `target` of a `StoppableThread`.
    """
    consul = Consul()
    logger = get_logger("consul_heartbeat_thread")
    if not debug:
        logger.level = 30
    thread = current_thread()
    logger.info("current thread is %s", thread)
    while not thread.stopped():
        logger.debug("renewing consul session %s", session)
        consul.session.renew(session)
        time.sleep(interval)


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
        self.debug = debug
        if not debug:
            self.logger.level = 30
        if prefix:
            self.consul_prefix = prefix
        else:
            self.consul_prefix = self.__class__.__name__.lower()

    @property
    def consul_key(self):
        return "{}/current/{}".format(
            self.consul_prefix, self._key)

    def consul_get(self, path):
        if not hasattr(path, '__iter__'):
            path = [path]
        return self.consul.kv["/".join([self.consul_prefix] + path)]

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
            return self.key_acquired
        else:
            self.logger.error("Consul session not started")
            return False

    def list_locked_keys(self):
        current = [key.split("/")[-1] for key in
                   self.consul.kv.find(
                       "/".join([self.consul_prefix, "current"]))]
        self.logger.info(
            "there are %s keys currently being synced: %s",
            len(current), current)
        return current

    def start_consul_session(self, behavior='delete', ttl='60s', delay='15s'):
        self.logger.info("Starting new consul session")
        self.consul_session = self.consul.session.create(
            behavior=behavior,
            ttl=ttl,
            delay=delay
        )
        self.logger.info(
            "Consul session %s started, forking thread to heartbeat",
            self.consul_session)
        self.heartbeat_thread = StoppableThread(target=consul_heartbeat,
                                                args=(self.consul_session, 10, self.debug))
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
