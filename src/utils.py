"""Utilities for Preadator."""

import logging
import os


LOG_LEVEL = getattr(logging, os.environ.get('PREADATOR_LOG_LEVEL', 'WARNING'))
DEBUG = LOG_LEVEL == logging.DEBUG

NUM_THREADS = len(os.sched_getaffinity(0))
NUM_PROCESSES = max(1, NUM_THREADS // 2)
THREADS_PER_REQUEST = THREADS_PER_PROCESS = NUM_THREADS // NUM_PROCESSES
