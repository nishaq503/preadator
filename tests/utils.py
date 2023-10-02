"""Utility functions for tests."""

import pathlib

import bfio
import numpy

TILE_SIZE = 1024


def make_random_image(
    path: pathlib.Path,
    rng: numpy.random.Generator,
    size: int,
) -> None:
    """Make a random image.

    Args:
        path: Path to save the image.
        rng: Random number generator.
        size: Size of the image.
    """
    with bfio.BioWriter(path) as writer:
        writer.X = size
        writer.Y = size
        writer.dtype = numpy.uint32

        writer[:] = rng.integers(2**8, 2**16, size=(size, size), dtype=writer.dtype)


def add_one(tile: numpy.ndarray) -> numpy.ndarray:
    """Add one to every pixel in the tile."""
    return tile + 1
