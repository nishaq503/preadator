"""Tests for the image calculator plugin."""

import pathlib
import shutil
import tempfile

import bfio
import numpy
import preadator

from . import utils


def gen_images(name: str, size: int) -> tuple[pathlib.Path, pathlib.Path]:
    """Generate a set of random images for testing."""
    # make a temporary directory
    data_dir = pathlib.Path(tempfile.mkdtemp(suffix="_data_dir"))

    input_dir = data_dir.joinpath("inputs")
    input_dir.mkdir(exist_ok=True)
    input_path = input_dir.joinpath(name)

    # Generate a random image
    utils.make_random_image(
        path=input_path,
        rng=numpy.random.default_rng(42),
        size=size,
    )

    output_dir = data_dir.joinpath("outputs")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir.joinpath(name)

    return input_path, output_path


def test_serial_process() -> None:
    """Test that multiple PMs can be run concurrently."""
    input_path, output_path = gen_images("test_serial.ome.tif", utils.TILE_SIZE * 2)

    with preadator.ProcessManager(
        name="test_serial",
        log_level="INFO",
        num_processes=1,
        threads_per_process=1,
        threads_per_request=1,
    ) as pm, bfio.BioReader(input_path, max_workers=1) as reader:
        input_shape = (reader.Y, reader.X)

        with bfio.BioWriter(
            output_path,
            max_workers=1,
            metadata=reader.metadata,
        ) as writer:
            for y_min in (0, reader.Y, utils.TILE_SIZE):
                y_max = min(y_min + utils.TILE_SIZE, reader.Y)

                for x_min in (0, reader.X, utils.TILE_SIZE):
                    x_max = min(x_min + utils.TILE_SIZE, reader.X)

                    tile = reader[y_min:y_max, x_min:x_max]
                    pm.submit_process(utils.add_one, tile)
                    writer[y_min:y_max, x_min:x_max] = tile

    assert output_path.exists(), "Output file does not exist."

    with bfio.BioReader(output_path, max_workers=1) as reader:
        output_shape = (reader.Y, reader.X)

        assert input_shape == output_shape, "Input and output shapes do not match."

    shutil.rmtree(input_path.parent.parent)


def test_serial_thread() -> None:
    """Test that multiple PMs can be run concurrently."""
    input_path, output_path = gen_images("test_serial.ome.tif", utils.TILE_SIZE * 2)

    with preadator.ProcessManager(
        name="test_serial",
        log_level="INFO",
        num_processes=1,
        threads_per_process=1,
        threads_per_request=1,
    ) as pm, bfio.BioReader(input_path, max_workers=1) as reader:
        input_shape = (reader.Y, reader.X)

        with bfio.BioWriter(
            output_path,
            max_workers=1,
            metadata=reader.metadata,
        ) as writer:
            for y_min in (0, reader.Y, utils.TILE_SIZE):
                y_max = min(y_min + utils.TILE_SIZE, reader.Y)

                for x_min in (0, reader.X, utils.TILE_SIZE):
                    x_max = min(x_min + utils.TILE_SIZE, reader.X)

                    tile = reader[y_min:y_max, x_min:x_max]
                    pm.submit_thread(utils.add_one, tile)
                    writer[y_min:y_max, x_min:x_max] = tile

    assert output_path.exists(), "Output file does not exist."

    with bfio.BioReader(output_path, max_workers=1) as reader:
        output_shape = (reader.Y, reader.X)

        assert input_shape == output_shape, "Input and output shapes do not match."

    shutil.rmtree(input_path.parent.parent)
