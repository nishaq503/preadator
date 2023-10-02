"""Tests that exceptions are correctly caught and handled by Preadator."""

import math
import random
import time
import typing

import preadator
import pytest


def division(a: float, b: float) -> typing.Union[float, ZeroDivisionError]:
    """Returns the result of dividing a by b.

    Raises:
        ZeroDivisionError: If b is 0
    """
    try:
        return a / b
    except ZeroDivisionError:
        msg = "Division by zero is not allowed"
        raise ZeroDivisionError(msg)


class NegativeSqrtError(ValueError):
    """Raised when a negative number is encountered."""

    pass


def slow_square_root(a: int) -> typing.Union[float, NegativeSqrtError]:
    """Returns the square root of a after a delay."""
    random_big_number = random.randint(1, 100) * 100  # noqa: S311
    i = 0
    for _ in range(0, random_big_number):
        i += 1
    return square_root(a) + square_root(i)  # type: ignore[operator]


def slow_square_root_that_releases_gil(
    a: int,
) -> typing.Union[float, NegativeSqrtError]:
    """Returns the square root of a after a random delay."""
    random_delay = random.randint(1, 10)  # noqa: S311
    time.sleep(random_delay)
    return square_root(a)


def square_root(a: int) -> typing.Union[float, NegativeSqrtError]:
    """Returns the square root of a.

    Raises:
        NegativeNumberError: If a is negative
    """
    try:
        return math.sqrt(a)
    except ValueError:
        msg = "Square root of negative numbers is not allowed"
        raise NegativeSqrtError(msg)


def prev_fib(a: int, b: int) -> int:
    """Returns the previous Fibonacci number.

    Args:
        a: the current Fibonacci number
        b: the previous Fibonacci number
    """
    return a - b


def test_division() -> None:
    """Tests that division errors are correctly caught and handled by Preadator."""
    assert division(1, 1) == 1
    assert division(1, 2) == 0.5

    with pytest.raises(ZeroDivisionError):
        division(1, 0)

    with pytest.raises(  # noqa: PT012
        ZeroDivisionError,
    ), preadator.ProcessManager() as pm:
        futures = []

        a, b = 8, 5
        futures.append(pm.submit_process(division, a, b))

        a, b = b, prev_fib(a, b)  # 5, 3
        futures.append(pm.submit_process(division, a, b))

        a, b = b, prev_fib(a, b)  # 3, 2
        futures.append(pm.submit_process(division, a, b))

        a, b = b, prev_fib(a, b)  # 2, 1
        futures.append(pm.submit_process(division, a, b))

        a, b = b, prev_fib(a, b)  # 1, 1
        futures.append(pm.submit_process(division, a, b))

        a, b = b, prev_fib(a, b)  # 1, 0
        futures.append(pm.submit_process(division, a, b))

        pm.join_processes()


def test_square_root() -> None:
    """Tests that square root errors are correctly caught and handled by Preadator."""
    assert square_root(1) == 1
    assert square_root(4) == 2

    with pytest.raises(NegativeSqrtError):
        square_root(-1)

    with pytest.raises(  # noqa: PT012
        NegativeSqrtError,
    ), preadator.ProcessManager() as pm:
        futures = []

        a, b = 8, 6
        futures.append(pm.submit_process(square_root, a))
        futures.append(pm.submit_process(square_root, b))

        a, b = b, prev_fib(a, b)  # 6, 2
        futures.append(pm.submit_process(square_root, b))

        a, b = b, prev_fib(a, b)  # 2, 4
        futures.append(pm.submit_process(square_root, b))

        a, b = b, prev_fib(a, b)  # 4, -2
        futures.append(pm.submit_process(square_root, b))

        pm.join_processes()


def test_slow_square_root() -> None:
    """Tests that square root errors are correctly caught and handled by Preadator."""
    assert square_root(1) == 1
    assert square_root(4) == 2

    with pytest.raises(NegativeSqrtError):
        square_root(-1)

    with pytest.raises(  # noqa: PT012
        NegativeSqrtError,
    ), preadator.ProcessManager() as pm:
        futures = []

        a, b = 8, 6
        futures.append(pm.submit_process(slow_square_root, a))
        futures.append(pm.submit_process(slow_square_root, b))

        a, b = b, prev_fib(a, b)  # 6, 2
        futures.append(pm.submit_process(slow_square_root, b))

        a, b = b, prev_fib(a, b)  # 2, 4
        futures.append(pm.submit_process(slow_square_root, b))

        a, b = b, prev_fib(a, b)  # 4, -2
        futures.append(pm.submit_process(slow_square_root, b))

        pm.join_processes()


def test_slow_square_root_that_releases_gil() -> None:
    """Tests that square root errors are correctly caught and handled by Preadator."""
    assert square_root(1) == 1
    assert square_root(4) == 2

    with pytest.raises(NegativeSqrtError):
        square_root(-1)

    with pytest.raises(  # noqa: PT012
        NegativeSqrtError,
    ), preadator.ProcessManager() as pm:
        futures = []

        a, b = 8, 6
        futures.append(pm.submit_process(slow_square_root_that_releases_gil, a))
        futures.append(pm.submit_process(slow_square_root_that_releases_gil, b))

        a, b = b, prev_fib(a, b)  # 6, 2
        futures.append(pm.submit_process(slow_square_root_that_releases_gil, b))

        a, b = b, prev_fib(a, b)  # 2, 4
        futures.append(pm.submit_process(slow_square_root_that_releases_gil, b))

        a, b = b, prev_fib(a, b)  # 4, -2
        futures.append(pm.submit_process(slow_square_root_that_releases_gil, b))

        pm.join_processes()
