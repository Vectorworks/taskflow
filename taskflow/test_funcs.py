from functools import reduce


def product(*args):
    return reduce(lambda product, item: product * item, args, 1)


class Test(object):
    @classmethod
    def mul_5_2(cls):
        return 5 * 2

    @staticmethod
    def sum(*args):
        return sum(args)
