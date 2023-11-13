"""pytest configuration file."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add options to pytest."""
    parser.addoption(
        "--hangs",
        action="store_true",
        dest="hangs",
        default=False,
        help="run hanging tests",
    )
    parser.addoption(
        "--slow",
        action="store_true",
        dest="slow",
        default=False,
        help="run slow tests",
    )
    parser.addoption(
        "--downloads",
        action="store_true",
        dest="downloads",
        default=False,
        help="run tests that download large data files",
    )
    parser.addoption(
        "--all",
        action="store_true",
        dest="all",
        default=False,
        help="run all tests",
    )
