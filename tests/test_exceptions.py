"""Tests that exceptions are correctly caught and handled by Preadator."""

import math
import pathlib
import re
import typing


class InvalidEmailError(Exception):
    """Raised when an email address is invalid."""

    pass


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


def square_root(a: int) -> typing.Union[float, ValueError]:
    """Returns the square root of a.

    Raises:
        ValueError: If a is negative
    """
    try:
        return math.sqrt(a)
    except ValueError:
        msg = "Square root of negative numbers is not allowed"
        raise ValueError(msg)


def delete_file(filename: str) -> typing.Union[None, FileNotFoundError]:
    """Deletes a file.

    Raises:
        FileNotFoundError: If the file does not exist
    """
    try:
        pathlib.Path(filename).unlink()
        return None
    except FileNotFoundError:
        msg = f"File {filename} not found"
        raise FileNotFoundError(msg)


def validate_email(email: str) -> typing.Union[bool, InvalidEmailError]:
    """Validates an email address.

    Raises:
        InvalidEmailError: If the email address is invalid
    """
    if re.match(r"[^@]+@[^@]+\.[^@]+", email):
        return True

    msg = "Invalid email address"
    raise InvalidEmailError(msg)
