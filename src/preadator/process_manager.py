"""Provides the ProcessManager."""

import abc
import concurrent.futures
import logging
import multiprocessing
import os
import queue
import typing

DEBUG = os.getenv("PREADATOR_DEBUG", "0").lower() == "1"
LOG_LEVEL = getattr(logging, os.environ.get("PREADATOR_LOG_LEVEL", "WARNING"))


def unit(x: typing.Any) -> typing.Any:  # noqa: ANN401
    """Returns the given argument."""
    return x


class ProcessManager(concurrent.futures.Executor):
    """Manages processes and threads.

    The ProcessManager class is a singleton class that manages processes and
    threads. It is a subclass of concurrent.futures.Executor and can be used
    as such. It is also a context manager and can be used as such.

    Preadator can also log the progress of the processes and threads. This is
    controlled by the PREADATOR_LOG_LEVEL environment variable. The default
    logging level is WARNING.

    Examples:
        The `ProcessManager` is meant to be a drop-in replacement for
        `concurrent.futures.ProcessPoolExecutor` and
        `concurrent.futures.ThreadPoolExecutor`. The intent is to minimize the
        configuration (ant thought behind said configuration) required to run
        code in parallel.

        The following example shows how to use the `ProcessManager` as a context
        manager.

        >>> from math import factorial
        >>> from preadator import ProcessManager

        - This example shows how we can submit jobs as processes to the
        internal ProcessPoolExecutor.

        >>> with ProcessManager(
        ...     name="PM_with_processes",
        ...     num_processes=4,
        ...     threads_per_process=2,
        ... ) as pm:
        ...     futures = []
        ...     # Submit the jobs as processes.
        ...     for i in range(7):
        ...         futures.append(pm.submit_process(factorial, i))
        ...     # Wait for all processes to finish.
        ...     pm.join_processes()
        ...     results = [f.result() for f in futures]
        ...     # Print the results.
        ...     print(results)
        [1, 1, 2, 6, 24, 120, 720]

        - This example shows how we can submit jobs as threads to the
        internal ThreadPoolExecutor.

        >>> with ProcessManager(
        ...     name="PM_with_threads",
        ...     num_processes=4,
        ...     threads_per_process=2,
        ... ) as pm:
        ...     futures = []
        ...     # Submit the jobs as threads.
        ...     for i in range(7):
        ...         futures.append(pm.submit_thread(factorial, i))
        ...     # Wait for all threads to finish.
        ...     pm.join_threads()
        ...     results = [f.result() for f in futures]
        ...     # Print the results.
        ...     print(results)
        [1, 1, 2, 6, 24, 120, 720]

        - This example shows how we can initialize several `ProcessManager`
        nested within each other.

        >>> def fibonacci(n: int) -> int:
        ...    if n < 0:
        ...        raise ValueError("fibonacci() not defined for negative values")
        ...    if n < 2:
        ...        return n
        ...    with ProcessManager(
        ...        name=f"PM_Fibonacci({n})",
        ...        num_processes=4,
        ...        threads_per_process=2,
        ...    ) as pm:
        ...        f_1 = pm.submit_process(fibonacci, n - 1)
        ...        f_2 = pm.submit_process(fibonacci, n - 2)
        ...        pm.join_processes()
        ...        return f_1.result() + f_2.result()
        >>> print(fibonacci(9))
        55

        - This example shows that the `ProcessManager` will catch and raise
        exceptions.

        >>> with ProcessManager(
        ...     name="PM_with_errors",
        ...     num_processes=4,
        ...     threads_per_process=2,
        ... ) as pm:
        ...     futures = []
        ...     # Submit the jobs as processes.
        ...     for i in range(4, -4, -2):
        ...         futures.append(pm.submit_process(factorial, i))
        ...     # Wait for all processes to finish.
        ...     pm.join_processes()
        Traceback (most recent call last):
            ...
        ValueError: factorial() not defined for negative values
    """

    _unique_manager: typing.Optional["ProcessManager"] = None
    """The unique ProcessManager instance.

    This is a class attribute that is used to ensure that only one instance of
    the ProcessManager class is ever created. This instance will be reused if
    the ProcessManager class is instantiated more than once.
    """

    _running: bool
    """Whether the ProcessManager is running.

    This is a class attribute that is used to ensure that the ProcessManager
    is not started more than once and so that we can nest the submission of
    threads.
    """

    def __new__(
        cls,
        *args,  # noqa: ANN002, ARG003
        **kwargs,  # noqa: ANN003, ARG003
    ) -> "ProcessManager":
        """Creates a new ProcessManager instance if none exists.

        Otherwise, returns the existing instance.

        Returns:
            A unique ProcessManager instance.
        """
        if cls._unique_manager is None:
            cls._unique_manager = super().__new__(cls)
            cls._running: bool = False
        return cls._unique_manager

    def __init__(  # noqa: PLR0913
        self,
        name: typing.Optional[str] = None,
        log_level: typing.Optional[str] = None,
        debug: typing.Optional[bool] = None,
        num_processes: typing.Optional[int] = None,
        process_names: typing.Optional[list[str]] = None,
        threads_per_process: typing.Optional[int] = None,
    ) -> None:
        """Initializes the process manager.

        Args:
            name: The name of the ProcessManager.
            log_level: The logging level of the ProcessManager.
            debug: Whether to run in debug mode.
            num_processes: The maximum number of processes.
            process_names: The names of the processes.
            threads_per_process: The number of threads per process that can be run.

        Raises:
            ValueError: If the number of process names is not equal to the number
            of processes.
        """
        # Make sure that the ProcessManager is not already running.
        if self._running:
            msg = "ProcessManager is already running. Try shutting it down first."
            self._main_logger.warning(msg)  # type: ignore[has-type]
            return

        # Change the name of the logger.
        self.name = "PM_main" if name is None else name
        """The name of the ProcessManager's main logger."""

        self.log_level = LOG_LEVEL if log_level is None else log_level

        self._main_logger = logging.getLogger(name)
        """The main logger of the ProcessManager."""

        self._main_logger.setLevel(self.log_level)

        self.debug = DEBUG if debug is None else debug
        """Whether to run in debug mode.

        This is controlled by the PREADATOR_DEBUG environment variable.

        In debug mode, the ProcessManager will run all submitted jobs in the main
        process and thread. It still returns the results using futures, as if they
        were run in parallel, but it does not actually run them in parallel. This is
        useful for debugging when, for example, you want to attach a debugger, or if
        you want to find out whether a bug is caused by running code in parallel, or
        because of dependencies between the processes/threads, or because of
        something else.
        """

        self.num_processes = (
            max(1, multiprocessing.cpu_count() // 2)
            if num_processes is None
            else num_processes
        )
        """The maximum number of processes.

        This is set to half the number of available CPUs by default. This is because
        the number of processes is usually limited by the number of available CPUs
        and each process can run, for example, IO-bound tasks in different threads.
        """

        self.threads_per_process = (
            2 if threads_per_process is None else threads_per_process
        )
        """The number of threads per process that can be run.

        This default is set so that the product of the number of processes and the
        number of threads per process is equal to the number of available CPUs.
        """

        self._threads_per_request = self.threads_per_process

        self.num_available_cpus = min(
            self.num_processes * self.threads_per_process,
            multiprocessing.cpu_count(),
        )
        """The number of available CPUs."""

        self.num_active_threads = 0
        """The number of active threads."""

        # Start the Process Manager.
        self._running = True
        self._main_logger.debug(
            f"Starting ProcessManager with {self.num_processes} processes ...",
        )

        # Create the process name queue.
        if process_names is None:
            process_names = [
                f"{self.name}: Process {i + 1}" for i in range(self.num_processes)
            ]
        elif len(process_names) != self.num_processes:
            msg = f"Number of process names must be equal to {self.num_processes}."
            raise ValueError(msg)

        self._process_names = multiprocessing.Queue(self.num_processes)  # type: ignore
        for p_name in process_names:
            self._process_names.put(p_name)

        # Create the process queue and populate it.
        self._process_queue = multiprocessing.Queue(self.num_processes)  # type: ignore
        for _ in range(self.num_processes):
            self._process_queue.put(self.threads_per_process)

        # Create the process IDs queue.
        self._process_ids = multiprocessing.Queue()  # type: ignore

        # Create the process executor.
        self._process_executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.num_processes,
        )
        self.processes = []  # type: ignore

        # Create the thread executor.
        self._thread_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_available_cpus,
        )
        self.threads = []  # type: ignore

        if self._running:
            num_threads = self.threads_per_process
        else:
            num_threads = self.num_available_cpus // self._threads_per_request

        self._main_logger.debug(
            f"Starting the thread pool with {num_threads} threads ...",
        )
        self._thread_queue = multiprocessing.Queue(num_threads)  # type: ignore
        for _ in range(0, num_threads, self._threads_per_request):
            self._thread_queue.put(self._threads_per_request)
        self.num_active_threads = num_threads

    # def _initializer(
    #     self,
    #     **init_args,
    # ) -> None:
    #     # Initialize the process.
    #     for k, v in init_args.items():

    #     self._main_logger.debug(

    def acquire_process(self) -> "ProcessLock":
        """Acquires a lock for the current process."""
        return ProcessLock(self._process_queue, self)

    def acquire_thread(self) -> "ThreadLock":
        """Acquires a lock for the current thread."""
        return ThreadLock(self._thread_queue, self)

    def submit_process(
        self,
        fn,  # noqa: ANN001
        /,
        *args,  # noqa: ANN002
        **kwargs,  # noqa: ANN003
    ) -> concurrent.futures.Future:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        if self.debug:
            result = fn(*args, **kwargs)
            f = self._process_executor.submit(unit, result)
        else:
            f = self._process_executor.submit(fn, *args, **kwargs)

        self.processes.append(f)
        return f

    def submit_thread(
        self,
        fn,  # noqa: ANN001
        /,
        *args,  # noqa: ANN002
        **kwargs,  # noqa: ANN003
    ) -> concurrent.futures.Future:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        if self.debug:
            result = fn(*args, **kwargs)
            f = self._thread_executor.submit(unit, result)
        else:
            f = self._thread_executor.submit(fn, *args, **kwargs)
        self.threads.append(f)
        return f

    def submit(
        self,
        fn,  # noqa: ANN001
        /,
        *args,  # noqa: ANN002
        **kwargs,  # noqa: ANN003
    ) -> concurrent.futures.Future:
        """This defaults to `submit_process`."""
        return self.submit_process(fn, *args, **kwargs)

    def join_processes(self, update_period: float = 30) -> None:
        """Waits for all processes to finish.

        Args:
            update_period: The time in seconds between updates on the progress
            of the processes.
        """
        if len(self.processes) == 0:
            if not self.debug:
                self._main_logger.warning("No processes to join.")
            self._running = False
            return

        errors = []

        while True:
            done, not_done = concurrent.futures.wait(
                self.processes,
                timeout=update_period,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )

            for p in done:
                if (e := p.exception(timeout=0)) is not None:
                    self._process_executor.shutdown(cancel_futures=True)
                    self._main_logger.error(e)
                    errors.append(e)

            progress = 100 * len(done) / (len(done) + len(not_done))
            self._main_logger.info(f"Processes progress: {progress:6.2f}% ...")

            if len(not_done) == 0:
                self._main_logger.info("All processes finished.")
                break

        if len(errors) > 0:
            msg = (
                f"{len(errors)} errors occurred during process join. "
                f"The first is: {errors[0]}"
            )
            self._main_logger.error(msg)
            raise errors[0]

    def join_threads(  # noqa: PLR0912, C901
        self,
        update_period: float = 10,
    ) -> None:
        """Waits for all threads to finish.

        Args:
            update_period: The time in seconds between updates on the progress
            of the threads.
        """
        if len(self.threads) == 0:
            if not self.debug:
                self._main_logger.warning("No threads to join.")
            self._running = False
            return

        errors = []

        while True:
            done, not_done = concurrent.futures.wait(
                self.threads,
                timeout=update_period,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )

            for t in done:
                if (e := t.exception(timeout=0)) is not None:
                    self._thread_executor.shutdown(cancel_futures=True)
                    self._main_logger.error(e)
                    errors.append(e)

            progress = 100 * len(done) / (len(done) + len(not_done))
            self._main_logger.info(f"Threads progress: {progress:6.2f}% ...")

            if len(not_done) == 0:
                self._main_logger.info("All threads finished.")
                break

            # Steal threads from available processes.
            if (
                self.num_active_threads < self.num_available_cpus
                and self._process_queue is not None
            ):
                try:
                    num_new_threads = 0
                    while self._process_queue.get(timeout=0):
                        for _ in range(
                            0,
                            self.threads_per_process,
                            self._threads_per_request,
                        ):
                            self._thread_queue.put(self._threads_per_request, timeout=0)
                            num_new_threads += self._threads_per_request

                except queue.Empty:
                    if num_new_threads > 0:
                        self.num_active_threads += num_new_threads
                        self._main_logger.info(
                            f"Stole {num_new_threads} threads from processes.",
                        )

        if len(errors) > 0:
            msg = (
                f"{len(errors)} errors occurred during thread join. "
                f"The first is: {errors[0]}"
            )
            self._main_logger.error(msg)
            raise errors[0]

    def shutdown(  # noqa: PLR0912 (too-many-branches (13 > 12))
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Args:
            wait: Whether to wait for all processes and threads to finish.
            cancel_futures: Whether to cancel pending futures.
        """
        errors = []

        if self._thread_executor is not None:
            self._main_logger.debug("Shutting down the thread pool ...")
            self._thread_executor.shutdown(wait=wait, cancel_futures=cancel_futures)

            if self.threads is not None:
                for t in self.threads:
                    try:
                        if (e := t.exception(timeout=0)) is not None:
                            errors.append(e)
                    except concurrent.futures.TimeoutError:
                        pass

        if self._process_executor is not None:
            self._main_logger.debug("Shutting down the process pool ...")
            self._process_executor.shutdown(wait=wait, cancel_futures=cancel_futures)

            if self.processes is not None:
                for p in self.processes:
                    try:
                        if (e := p.exception(timeout=0)) is not None:
                            errors.append(e)
                    except concurrent.futures.TimeoutError:
                        pass

        self.join_threads()
        self.join_processes()
        self._thread_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_available_cpus,
        )
        self.threads = []
        self._process_executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.num_processes,
        )
        self.processes = []

        if len(errors) > 0:
            msg = (
                f"{len(errors)} errors occurred during shutdown. "
                f"The first is: {errors[0]}"
            )
            self._main_logger.error(msg)
            raise errors[0]


class QueueLock(abc.ABC):
    """An abstract class for dealing with locks in processes and threads."""

    def __init__(
        self,
        queue: multiprocessing.Queue,
        pm: ProcessManager,
    ) -> None:
        """Initializes the lock with the queue.

        Args:
            queue: The queue to use for locking.
            pm: The ProcessManager instance.
        """
        self.queue = queue
        """The queue to use for locking."""

        self.count = 0
        """The number of locks to acquire."""

        self.name = "QueueLock"
        """The name of the lock's logger."""

        self.logger = logging.getLogger(self.name)
        """The logger of the lock."""

        self.logger.setLevel(pm.log_level)

        self._thread_queue = pm._thread_queue
        self.threads_per_process = pm.threads_per_process
        self._threads_per_request = pm._threads_per_request

    def lock(self) -> None:
        """Acquires the lock in the queue."""
        self.logger.debug("Acquiring lock ...")
        self.count = self.queue.get()
        self.logger.debug("Lock acquired.")

    def __del__(self) -> None:
        """Releases the lock in the queue."""
        if self.count != 0:
            self.logger.debug("Releasing lock ...")
            self.release()
            self.logger.debug("Lock released.")

    @abc.abstractmethod
    def release(self) -> None:
        """Releases the lock in the queue."""
        pass

    def __enter__(self) -> "QueueLock":
        """Acquires the lock in the queue."""
        self.lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # noqa: ANN001
        """Releases the lock in the queue."""
        self.release()
        self.count = 0


class ThreadLock(QueueLock):
    """A lock for threads."""

    def release(self) -> None:
        """Releases the lock in the queue."""
        self.logger.debug(f"Releasing {self.count} locks ...")
        self.queue.put(self.count)


class ProcessLock(QueueLock):
    """A lock for processes."""

    def release(self) -> None:
        """Releases the lock in the queue."""
        # We may have stolen some threads from other processes, so we need to
        # return them.
        try:
            num_returned = 0
            while True:
                num_returned += self._thread_queue.get(timeout=0)

        except queue.Empty:
            self.logger.debug(f"Returning {num_returned} threads to the queue ...")
            for _ in range(0, num_returned, self.threads_per_process):
                self.queue.put(self.threads_per_process, timeout=0)

            self._thread_queue.put(self._threads_per_request)
            self.logger.debug(f"Returned {num_returned} threads to the queue.")
