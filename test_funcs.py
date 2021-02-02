from functools import reduce


def product(*args):
    return reduce(lambda result, item: result * item, args, 1)


class Test(object):
    @classmethod
    def const(cls, *args):
        return args[0] if len(args) == 1 else args

    @staticmethod
    def sum(*args):
        return sum(args)
