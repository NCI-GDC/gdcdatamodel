def validate(*types, **options):
    def check(f):
        def new_f(instance, value, **kwargs):
            _types = types+(type(None),)
            # If type is str, accept unicode as well, it will be sanitized
            if str in _types:
                _types = types+(unicode,)
            assert isinstance(value, _types), (
                "arg '{}' ({}), does not match {} for property {}".format(
                    value, type(value), _types, f.__name__))
            enum = options.get('enum')
            if enum:
                assert value in enum, (
                    "arg '{}' not in {} for property {}".format(
                        value, enum, f.__name__))
            return f(instance, value, **kwargs)
        return new_f
    return check
