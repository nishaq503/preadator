"""Test that multiple preadator's ProcessManagers can be run concurrently."""

import pathlib
import shutil
import tempfile

import bfio
import numpy
import preadator

from . import utils


def gen_images(name: str, size: int) -> tuple[pathlib.Path, pathlib.Path]:
    """ Generate a temporary directory for inputs and outputs and an empty image.
        Each call to this function will create its own temporary directory,
        making each image unique.
        :return: temporary inputs and outputs directories.
    """
    # make a temporary directory
    data_dir = pathlib.Path(tempfile.mkdtemp(suffix="_data_dir"))

    input_dir = data_dir.joinpath("inputs")
    input_dir.mkdir(exist_ok=True)
    input_path = input_dir.joinpath(name)

    # Generate an image of zeros
    with bfio.BioWriter(input_path) as writer:
        writer.X = size
        writer.Y = size
        writer.dtype = numpy.uint32

        writer[:] = numpy.zeros((size, size), dtype=writer.dtype)

    output_dir = data_dir.joinpath("outputs")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir.joinpath(name)

    return input_path, output_path


def serial_execution(submit_method : str) -> None:
    """Serial execution with the given `submit_method`.
    :param submit_method: can be one of `submit_process` or `submit_thread`
    """
    input_path, output_path = gen_images("test_serial.ome.tif", utils.TILE_SIZE * 2)

    with preadator.ProcessManager(
        name="test_serial_process",
        log_level="INFO",
        num_processes=1,
        threads_per_process=1,
        threads_per_request=1,
    ) as pm:
        futures = []

        with bfio.BioReader(input_path, max_workers=1) as reader:
            input_shape = (reader.Y, reader.X)
            for y_min in (0, reader.Y, utils.TILE_SIZE):
                y_max = min(y_min + utils.TILE_SIZE, reader.Y)

                for x_min in (0, reader.X, utils.TILE_SIZE):
                    x_max = min(x_min + utils.TILE_SIZE, reader.X)

                    tile = reader[y_min:y_max, x_min:x_max]
                    futures.append(getattr(pm, submit_method)(utils.add_one, tile))

            pm.join_processes()

            with bfio.BioWriter(
                output_path,
                max_workers=1,
                metadata=reader.metadata,
            ) as writer:
                for y_min in (0, reader.Y, utils.TILE_SIZE):
                    y_max = min(y_min + utils.TILE_SIZE, reader.Y)

                    for x_min in (0, reader.X, utils.TILE_SIZE):
                        x_max = min(x_min + utils.TILE_SIZE, reader.X)

                        tile = futures[0].result()
                        futures = futures[1:]
                        writer[y_min:y_max, x_min:x_max] = tile

    assert output_path.exists(), "Output file does not exist."

    with bfio.BioReader(output_path, max_workers=1) as reader:
        output_shape = (reader.Y, reader.X)

        assert input_shape == output_shape, "Input and output shapes do not match."

        image = reader[:]
        assert numpy.all(image == 1), "Not all pixels were incremented by 1."

    shutil.rmtree(input_path.parent.parent)

def test_serial_process() -> None:
    """Test serial execution of a simple image algorithm using a single process."""
    serial_execution("submit_process")



def test_serial_thread() -> None:
    """Test serial execution of a simple image algorithm using a single thread."""
    serial_execution("submit_thread")
