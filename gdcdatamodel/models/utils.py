def validate(*types, **options):
    def check(f):
        def new_f(instance, value, **kwargs):
            assert isinstance(value, types),\
                'arg %r does not match %s' % (value, types)
            enum = options.get('enum')
            if enum:
                assert value in enum, 'arg %r not in %s' % (value, enum)
            return f(instance, value, **kwargs)
        return new_f
    return check
