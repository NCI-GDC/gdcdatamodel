from functools import wraps


def validate(*types, **kwargs):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return f
    return decorator
