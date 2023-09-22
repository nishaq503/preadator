"""Provides the ProcessManager class."""

import concurrent.futures
import logging
import multiprocessing
import os
import queue
import typing

from . import locks

DEBUG = os.getenv("PREADATOR_DEBUG", "False").lower() in ("true", "1", "t")

class ProcessManager(concurrent.futures.Executor):

    unique_manager = None

    log_level = getattr(logging, os.environ.get('PREADATOR_LOG_LEVEL', 'WARNING'))
    debug = os.getenv("PREADATOR_DEBUG", "false").lower() in ("true", "1", "t")

    name = "ProcessManager"
    main_logger = logging.getLogger(name)
    main_logger.setLevel(log_level)

    job_name = None
    job_logger = None

    process_executor = None
    processes = None

    thread_executor = None
    threads = None
    num_active_threads = 0

    num_threads: int = len(os.sched_getaffinity(0))
    num_processes: int = max(1, num_threads // 2)
    threads_per_process: int = num_threads // num_processes
    threads_per_request: int = threads_per_process

    running: bool = False

    process_queue = None
    process_names = None
    process_ids = None
    processes = None

    thread_queue = None
    thread_names = None
    thread_ids = None
    threads = None

    def __new__(cls, *args, **kwargs):
        if cls.unique_manager is None:
            cls.unique_manager = super().__new__(cls)
        return cls.unique_manager

    def _initializer(self, initargs) -> None:
        """Initializes the process."""
        for k, v in initargs.items():
            setattr(self, k, v)

        self.job_name = self.process_names.get(timeout=0)
        self.job_logger = logging.getLogger(self.job_name)
        self.job_logger.setLevel(self.log_level)

        self.process_ids.put(os.getpid())
        self.main_logger.debug(f"Process {self.job_name} initialized with PID {os.getpid()}.")

    def __init__(
        self,
        name: str = None,
        process_names: list[str] = None,
        **initargs,
    ):
        """Initializes the process manager."""

        # Make sure that the ProcessManager is not already running.
        if self.running:
            raise RuntimeError("ProcessManager is already running. Try shutting it down first.")

        # Change the name of the logger.
        if name is not None:
            self.name = name
            self.main_logger = logging.getLogger(name)
            self.main_logger.setLevel(self.log_level)

        # Start the Process Manager.
        self.running = True
        self.main_logger.debug(
            f"Starting ProcessManager with {self.num_processes} processes ..."
        )

        # Create the process name queue.
        if process_names is None:
            process_names = [
                f"{self.name}: Process {i + 1}"
                for i in range(self.num_processes)
            ]
        else:
            if len(process_names) != self.num_processes:
                raise ValueError(
                    f"Number of process names must be equal to {self.num_processes}."
                )
        self.process_names = multiprocessing.Queue(self.num_processes)
        for p_name in process_names:
            self.process_names.put(p_name)
        
        # Create the process queue and populate it.
        self.process_queue = multiprocessing.Queue(self.num_processes)
        for _ in range(self.num_processes):
            self.process_queue.put(self.threads_per_process)
        
        # Create the process IDs queue.
        self.process_ids = multiprocessing.Queue()

        # Create the process executor.
        self.process_executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.num_processes,
            initializer=self._initializer,
            initargs=initargs,
        )
        self.processes = []

    def init_threads(self):
        self.thread_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_threads,
        )
        self.threads = []

        if self.running:
            num_threads = self.threads_per_process
        else:
            num_threads = self.num_threads // self.threads_per_request
        
        self.main_logger.debug(f"Starting the thread pool with {num_threads} threads ...")
        self.thread_queue = queue.Queue(num_threads)
        for _ in range(0, num_threads, self.threads_per_request):
            self.thread_queue.put(self.threads_per_request)
        self.num_active_threads = num_threads

    def process(self, name: str = None):
        """Locks a process for the current thread."""
        if name is not None:
            self.job_name = f"{name}: "
        
        return locks.ProcessLock(self.process_queue)

    def thread(self):
        return locks.ThreadLock(self.thread_queue)
    
    def submit_process(self, fn, /, *args, **kwargs) -> concurrent.futures.Future:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        return self.process_executor.submit(fn, *args, **kwargs)
    
    def submit_thread(self, fn, /, *args, **kwargs) -> concurrent.futures.Future:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        return self.thread_executor.submit(fn, *args, **kwargs)
    
    def submit(self, process: bool, fn, /, *args, **kwargs) -> concurrent.futures.Future:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        if process:
            return self.submit_process(fn, *args, **kwargs)
        else:
            return self.submit_thread(fn, *args, **kwargs)
    
    def join_processes(self, update_period: float = 30) -> None:
        """Waits for all processes to finish."""

        if len(self.processes) == 0:
            if not self.debug:
                self.main_logger.warning("No processes to join.")
            return

        while True:
            done, not_done = concurrent.futures.wait(
                self.processes,
                timeout=update_period,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )

            for p in done:
                if (e := p.exception(timeout=0)) is not None:
                    self.shutdown(cancel_futures=True)
                    raise e
            
            progress = 100 * len(done) / (len(done) + len(not_done))
            self.main_logger.info(f"Processes progress: {progress:6.2f}% ...")

            if len(not_done) == 0:
                self.main_logger.info("All processes finished.")
                break
    
    def join_threads(self, update_period: float = 10) -> None:
        """Waits for all threads to finish."""

        if len(self.threads) == 0:
            if not self.debug:
                self.main_logger.warning("No threads to join.")
            return

        while True:
            done, not_done = concurrent.futures.wait(
                self.threads,
                timeout=update_period,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )

            for t in done:
                if (e := t.exception(timeout=0)) is not None:
                    self.shutdown(cancel_futures=True)
                    raise e
            
            progress = 100 * len(done) / (len(done) + len(not_done))
            self.main_logger.info(f"Threads progress: {progress:6.2f}% ...")

            if len(not_done) == 0:
                self.main_logger.info("All threads finished.")
                break
            
            # Steal threads from available processes.
            if self.num_active_threads < self.num_threads and self.process_queue is not None:
                try:
                    num_new_threads = 0
                    while self.process_queue.get(timeout=0):
                        for _ in range(0, self.threads_per_process, self.threads_per_request):
                            self.thread_queue.put(self.threads_per_request, timeout=0)
                            num_new_threads += self.threads_per_request

                except queue.Empty:
                    if num_new_threads > 0:
                        self.num_active_threads += num_new_threads
                        self.main_logger.info(f"Stole {num_new_threads} threads from processes.")
    
    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Args:
            wait: If True then shutdown will not return until all running
                futures have finished executing and the resources used by the
                executor have been reclaimed.
            cancel_futures: If True then shutdown will cancel all pending
                futures. Futures that are completed or running will not be
                cancelled.
        """
        
        errors = []

        if self.thread_executor is not None:
            self.main_logger.debug("Shutting down the thread pool ...")
            self.thread_executor.shutdown(wait=wait, cancel_futures=cancel_futures)
            self.thread_executor = None

            if self.threads is not None:
                for t in self.threads:
                    try:
                        if (e := t.exception(timeout=0)) is not None:
                            errors.append(e)
                    except concurrent.futures.TimeoutError:
                        pass
        
        if self.process_executor is not None:
            self.main_logger.debug("Shutting down the process pool ...")
            self.process_executor.shutdown(wait=wait, cancel_futures=cancel_futures)
            self.process_executor = None

            if self.processes is not None:
                for p in self.processes:
                    try:
                        if (e := p.exception(timeout=0)) is not None:
                            errors.append(e)
                    except concurrent.futures.TimeoutError:
                        pass
        
        self.join_threads()
        self.join_processes()
        self.running = False

        if len(errors) > 0:
            self.main_logger.error(f"{len(errors)} errors occurred during shutdown. The first is: {errors[0]}")
            raise errors[0]
