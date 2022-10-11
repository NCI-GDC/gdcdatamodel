import sys
from functools import wraps


def validate(*types, **kwargs):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        return f

    return decorator


def py3_to_bytes(bytes_or_str):
    if sys.version_info[0] > 2 and isinstance(bytes_or_str, str):
        return bytes_or_str.encode("utf-8")
    return bytes_or_str
