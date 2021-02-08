class Handlers(object):
    @classmethod
    def repeat(cls, *args, **kwargs):
        return args


class InheritedHandlers(Handlers):
    pass


def handler(*args, **kwargs):
    return args
