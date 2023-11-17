"""Test that multiple preadator's ProcessManagers can be nested."""

import pathlib
import shutil
import tempfile
import typing

import bfio
import numpy
import preadator
import pytest

from . import utils


def gen_image(
    input_dir: pathlib.Path,
    name: str,
    size: int,
    value: int,
) -> pathlib.Path:
    """Generate an image return the path to the image.

    The image will be filled with the given value.

    Args:
        input_dir: directory in which to save the image.
        name: with which to save the image.
        size: of the image.
        value: with which to fill the image.

    Returns:
        path to the image.
    """
    input_path = input_dir.joinpath(name)

    # Generate an image of zeros
    with bfio.BioWriter(input_path) as writer:
        writer.X = size
        writer.Y = size
        writer.dtype = numpy.uint32

        writer[:] = numpy.full((size, size), fill_value=value, dtype=writer.dtype)

    return input_path


def tile_add(x: numpy.ndarray, y: numpy.ndarray) -> numpy.ndarray:
    """Add the two tiles together."""
    return x + y


def tile_sub(x: numpy.ndarray, y: numpy.ndarray) -> numpy.ndarray:
    """Subtract the two tiles."""
    return x - y


def tile_mul(x: numpy.ndarray, y: numpy.ndarray) -> numpy.ndarray:
    """Multiply the two tiles."""
    return x * y


def img_op(
    x: pathlib.Path,
    y: pathlib.Path,
    o: pathlib.Path,
    op: typing.Callable[[numpy.ndarray, numpy.ndarray], numpy.ndarray],
) -> pathlib.Path:
    """Apply the given op to two images and save the result.

    Args:
        x: path to the first image.
        y: path to the second image.
        o: path to the output image.
        op: operation to apply to the images.

    Returns:
        path to the output image.
    """
    with preadator.ProcessManager(
        name="img_op",
        log_level="INFO",
        num_processes=2,
        threads_per_process=1,
    ) as pm:
        futures = []

        with bfio.BioReader(x, max_workers=1) as reader_x, bfio.BioReader(
            y,
            max_workers=1,
        ) as reader_y:
            metadata = reader_x.metadata
            for y_min in (0, reader_x.Y, utils.TILE_SIZE):
                y_max = min(y_min + utils.TILE_SIZE, reader_x.Y)

                for x_min in (0, reader_x.X, utils.TILE_SIZE):
                    x_max = min(x_min + utils.TILE_SIZE, reader_x.X)

                    tile_x = reader_x[y_min:y_max, x_min:x_max]
                    tile_y = reader_y[y_min:y_max, x_min:x_max]
                    futures.append(pm.submit_process(op, tile_x, tile_y))

        pm.join_processes()

        with bfio.BioWriter(
            o,
            max_workers=1,
            metadata=metadata,
        ) as writer:
            for y_min in (0, writer.Y, utils.TILE_SIZE):
                y_max = min(y_min + utils.TILE_SIZE, writer.Y)

                for x_min in (0, writer.X, utils.TILE_SIZE):
                    x_max = min(x_min + utils.TILE_SIZE, writer.X)

                    tile = futures[0].result()
                    futures = futures[1:]
                    writer[y_min:y_max, x_min:x_max] = tile

    return o


@pytest.mark.skipif("not config.getoption('hangs')")
def test_nested_pm() -> None:
    """Test that preadator's ProcessManager can be nested."""
    data_dir = pathlib.Path(tempfile.mkdtemp(suffix="_data_dir"))

    input_dir = data_dir.joinpath("inputs")
    input_dir.mkdir(exist_ok=True)

    output_dir = data_dir.joinpath("outputs")
    output_dir.mkdir(exist_ok=True)

    image_paths = [
        gen_image(input_dir, f"{name}.ome.tif", utils.TILE_SIZE * 2, i)
        for i, name in enumerate(["a", "b", "c", "d"], start=1)
    ]

    """
    out_path = img_op(
        img_op(
            image_paths[0],
            image_paths[1],
            output_dir.joinpath("ab.ome.tif"),
            tile_add,
        ),
        img_op(
            image_paths[3],
            image_paths[2],
            output_dir.joinpath("cd.ome.tif"),
            tile_sub,
        ),
        output_dir.joinpath("abcd.ome.tif"),
        tile_mul,
    )
    """

    with preadator.ProcessManager(
        name="test_nested",
        log_level="INFO",
        num_processes=4,
        threads_per_process=2,
    ) as pm:
        futures = []

        futures.append(
            pm.submit_process(
                img_op,
                image_paths[0],
                image_paths[1],
                output_dir.joinpath("ab.ome.tif"),
                tile_add,
            ),
        )
        futures.append(
            pm.submit_process(
                img_op,
                image_paths[3],
                image_paths[2],
                output_dir.joinpath("cd.ome.tif"),
                tile_sub,
            ),
        )
        futures.append(
            pm.submit_process(
                img_op,
                futures[0].result(),
                futures[1].result(),
                output_dir.joinpath("abcd.ome.tif"),
                tile_mul,
            ),
        )

        pm.join_processes()

        out_path = futures[-1].result()

    with bfio.BioReader(out_path, max_workers=1) as reader:
        assert numpy.all(reader[:] == 3), "The output image is incorrect."

    shutil.rmtree(data_dir)


def fibonacci(n: int, method: typing.Literal["process", "thread"]) -> int:
    """Get Fibonacci number, the recursive way.

    For now, his test should fail with a large enough n. The reason is that the
    ProcessManager will have too many threads/processes to manage and will
    hang.

    With small n, the test will pass if there are enough threads/processes
    available.

    With large n, the test will fail because there are not enough
    threads/processes available for the shortest possible execution path to a
    base case.

    With moderate n, the test will have a chance of passing or failing depending
    on the number of threads/processes available and on the inherent randomness
    of the execution order of the threads/processes.

    Eventually, we will rework how the ProcessManager handles nested
    thread/process submissions. At that point, this test should pass for all
    values of n if there is enough time to run the test.

    Args:
        n: the nth Fibonacci number to get.
        method: use threads or processes.

    Returns:
        the nth Fibonacci number.
    """
    if n < 0:
        msg = "fibonacci() not defined for negative values"
        raise ValueError(msg)
    if n < 2:
        return n
    with preadator.ProcessManager(
        name=f"PM_Fibonacci({n})",
        num_processes=4,
        threads_per_process=2,
    ) as pm:
        if method == "thread":
            f_1 = pm.submit_thread(fibonacci, n - 1, method)
            f_2 = pm.submit_thread(fibonacci, n - 2, method)
            pm.join_threads()
        else:
            assert method == "process"
            f_1 = pm.submit_process(fibonacci, n - 1, method)
            f_2 = pm.submit_process(fibonacci, n - 2, method)
            pm.join_processes()
        return f_1.result() + f_2.result()


@pytest.mark.skipif("not config.getoption('hangs')")
@pytest.mark.parametrize("n", [3, 4, 5, 6])
@pytest.mark.parametrize("method", ["process", "thread"])
def test_fibonacci(n: int, method: typing.Literal["process", "thread"]) -> None:
    """Run a series of recursive tests to calculate Fibonacci numbers."""
    fibonacci(n, method)
