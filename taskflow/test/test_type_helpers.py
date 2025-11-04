import pytest

from taskflow import type_helpers

from .fixtures import Handlers, InheritedHandlers, handler


def test_handler():
    # dummy test for coverage of the test fixture
    handler()


def test_type_to_string():
    assert type_helpers.type_to_string(Handlers) == "taskflow.test.fixtures.Handlers"


def test_type_from_string():
    assert type_helpers.type_from_string("taskflow.test.fixtures.Handlers") == Handlers


def test_function_to_string():
    assert type_helpers.function_to_string(handler) == "taskflow.test.fixtures.handler"


def test_function_to_string_class():
    assert type_helpers.function_to_string(Handlers.repeat) == "taskflow.test.fixtures.Handlers.repeat"


def test_function_to_string_inherited():
    assert (
        type_helpers.function_to_string(InheritedHandlers.repeat) == "taskflow.test.fixtures.InheritedHandlers.repeat"
    )


def test_function_from_string():
    assert type_helpers.function_from_string("taskflow.test.fixtures.handler") == handler


def test_function_from_string_class_method():
    assert type_helpers.function_from_string("taskflow.test.fixtures.Handlers.repeat") == Handlers.repeat


def test_import_string_no_dots():
    with pytest.raises(ImportError):
        type_helpers.import_string("taskflow")


def test_import_string_not_found():
    with pytest.raises(ImportError):
        type_helpers.import_string("taskflow.missing")
