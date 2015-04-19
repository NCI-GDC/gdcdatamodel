from gdcdatamodel import models
import os
import functools
import datamodel
import qa


class ContextDecorator(object):
    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwds):
            with self:
                return f(*args, **kwds)
        return decorated


class no_proxy(ContextDecorator):
    def __enter__(self):
        PROXY_ENV_VARS = ["http_proxy", "https_proxy"]
        self.temp_env = {}
        for key in PROXY_ENV_VARS:
            if os.environ.get(key):
                self.temp_env[key] = os.environ.pop(key)

    def __exit__(self, exc_type, exc_value, traceback):
        for key, val in self.temp_env.iteritems():
            os.environ[key] = val


def process(f):
    f.zug_process = True
    return f


def initialize(f):
    f.zug_initialize = True
    return f
