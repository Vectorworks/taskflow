from functools import reduce


def product(parameters):
    return reduce(lambda result, item: result * item, parameters, 1)


class Test(object):
    @classmethod
    def const(cls, value):
        return value

    @staticmethod
    def sum(parameters):
        return sum(parameters)
