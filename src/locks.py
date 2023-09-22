"""Provides locks on threads and processes for the process manager."""

import abc
import logging
import multiprocessing
import queue
import typing

from . import process_manager

QueueAlias = typing.Union[multiprocessing.Queue, queue.Queue]

class QueueLock(abc.ABC):
    name = "QueueLock"
    logger = logging.getLogger(name)
    logger.setLevel(process_manager.ProcessManager.log_level)

    def __init__(self, queue: QueueAlias):
        self.queue = queue
        self.count = 0
    
    def lock(self) -> None:
        self.logger.debug(f"Acquiring lock ...")
        self.count = self.queue.get()
        self.logger.debug("Lock acquired.")

    def __del__(self):
        if self.count != 0:
            self.logger.debug("Releasing lock ...")
            self.release()
            self.logger.debug("Lock released.")

    @abc.abstractmethod
    def release(self) -> None:
        """Releases the lock in the queue."""
        pass
    
    def __enter__(self):
        self.lock()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
        self.count = 0


class ThreadLock(QueueLock):

    name = "ThreadLock"
    logger = logging.getLogger(name)
    logger.setLevel(process_manager.ProcessManager.log_level)

    def release(self) -> None:
        """Releases the lock in the queue."""
        self.logger.debug(f"Releasing {self.count} locks ...")
        self.queue.put(self.count)


class ProcessLock(QueueLock):

    name = "ProcessLock"
    logger = logging.getLogger(name)
    logger.setLevel(process_manager.ProcessManager.log_level)

    def release(self) -> None:
        """Releases the lock in the queue."""
        # We may have stolen some threads from other processes, so we need to
        # return them.
        try:
            num_returned = 0
            while True:
                num_returned += process_manager.ProcessManager.thread_queue.get(timeout=0)

        except queue.Empty:
            self.logger.debug(f"Returning {num_returned} threads to the queue ...")
            for _ in range(0, num_returned, process_manager.ProcessManager.threads_per_process):
                self.queue.put(process_manager.ProcessManager.threads_per_process, timeout=0)
            
            process_manager.ProcessManager.num_active_threads = process_manager.ProcessManager.threads_per_process
            process_manager.ProcessManager.thread_queue.put(process_manager.ProcessManager.threads_per_request)
            self.logger.debug(f"Returned {num_returned} threads to the queue.")
