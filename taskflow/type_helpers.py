from importlib import import_module


def type_to_string(type_object):
    return f'{type_object.__module__}.{type_object.__name__}'


def type_from_string(type_name):
    container = import_string(type_name)
    return container


def function_to_string(func):
    func = getattr(func, '__func__', func)
    return f'{func.__module__}.{func.__qualname__}'


def function_from_string(func_path):
    container_name, func_name = func_path.rsplit('.', maxsplit=1)

    try:
        container = import_module(container_name)
    except ImportError:
        # probably a class - try again
        container = import_string(container_name)

    return getattr(container, func_name)


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError as err:
        raise ImportError('Module "%s" does not define a "%s" attribute/class' % (
            module_path, class_name)
        ) from err
