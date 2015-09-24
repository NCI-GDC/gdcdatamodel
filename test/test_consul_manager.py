import time
from consulate import Consul
from zug.consul_manager import ConsulManager
from unittest import TestCase


class ConsulManagerTest(TestCase):
    def setUp(self):
        self.worker = ConsulManager()
        self.consul = Consul()
        self.session = self.consul.session.create(delay='0s')

    def tearDown(self):
        self.worker.cleanup()
        self.consul.session.destroy(self.session)

    def test_acquire_lock(self):
        self.worker.start_consul_session(delay='0s')
        self.assertTrue(self.worker.get_consul_lock('id1'))

    def test_key_set(self):
        self.worker.start_consul_session(delay='0s')
        self.worker.get_consul_lock('id1')
        self.worker.consul_key_set({'test': 'value'})
        self.worker.set_consul_state('downloading')
        self.assertEqual(self.worker.consul.kv[self.worker.consul_key],
                         {'test': 'value', 'state': 'downloading'})

    def test_key_set_without_locking(self):
        self.worker.key = 'id1'
        self.assertFalse(self.worker.consul_key_set('value'))
        self.assertFalse(self.worker.set_consul_state('downloading'))

    def test_acquire_locked_key(self):
        worker2 = ConsulManager()
        worker2.key = 'id1'
        worker2.start_consul_session(delay='0s')
        self.assertTrue(worker2.get_consul_lock('id1'))
        self.assertFalse(self.worker.get_consul_lock('id1'))
        worker2.cleanup()

    def test_session_loss_recovery(self):
        self.worker.start_consul_session(delay='0s', interval=1)
        self.assertTrue(self.worker.get_consul_lock('test_key'))
        # first, let's kill the worker's session
        self.consul.session.destroy(self.worker.consul_session)
        # now if we wait, it should reacquire the lock . . .
        time.sleep(3)
        self.assertFalse(self.consul.kv.acquire_lock(self.worker.consul_key, self.session))
