from consulate import Consul
from threading import Thread, current_thread
from cdisutils.log import get_logger
import time


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


def consul_heartbeat(session, interval):
    """
    Heartbeat with consul to keep `session` alive every `interval`
    seconds. This must be called as the `target` of a `StoppableThread`.
    """
    consul = Consul()
    logger = get_logger("consul_heartbeat_thread")
    thread = current_thread()
    logger.info("current thread is %s", thread)
    while not thread.stopped():
        logger.debug("renewing consul session %s", session)
        consul.session.renew(session)
        time.sleep(interval)


class ConsulMixin(object):
    def __init__(self, logger, prefix='', consul_key='key'):
        self.logger = logger
        self.key = consul_key
        self.consul = Consul()
        if prefix:
            self.prefix = prefix
        else:
            self.prefix = self.__class__.__name__.lower()

    @property
    def consul_key(self):
        return "{}/current/{}".format(
            self.prefix, object.__getattribute__(self, self.key))

    def consul_get(self, path):
        return self.consul.kv["/".join([self.prefix] + path)]

    def consul_key_set(self, value):
        self.consul.kv.set(self.consul_key, value)

    def set_consul_state(self, state):
        current = self.consul.kv.get(self.consul_key)
        print self.consul_key
        print current
        print type(current)
        current["state"] = state
        self.logger.info("Setting %s to %s", self.consul_key, current)
        self.consul.kv.set(self.consul_key, current)

    def get_consul_lock(self):
        self.logger.info("Attempting to lock %s in consul", self.consul_key)
        return self.consul.kv.acquire_lock(
            self.consul_key, self.consul_session)

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
                                                args=(self.consul_session, 10))
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()


class Test(ConsulMixin):
    def __init__(self):
        super(Test, self).__init__('t', 'test', 'id')
        self.id = 1
