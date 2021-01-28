from functools import reduce


def product(*args):
    return reduce(lambda product, item: product * item, args, 1)


class Test(object):
    @classmethod
    def const(cls, param):
        return param

    @staticmethod
    def sum(*args):
        return sum(args)
