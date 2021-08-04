""" Helpers to run the same task on multiple items and collect errors.

"""

import abc
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import RLock
from typing import Any, Generic, List, Tuple, TypeVar


from tsrc.errors import Error
from tsrc.utils import erase_last_line

T = TypeVar("T")


class ExecutorFailed(Error):
    pass


class Task(Generic[T], metaclass=abc.ABCMeta):
    """Represent an action to be performed."""

    @abc.abstractmethod
    def process(self, index: int, count: int, item: T) -> None:
        """
        Daughter classes should override this method to provide the code
        that processes the item.

        It should *not* display anything to stdout/stderr in order to be used
        equally by the ParallelExecutor and SequentialExecutor
        """
        pass


class SequentialExecutor(Generic[T]):
    """Run the task on all items one at a time, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T]) -> None:
        self.task = task

    # Collected errors as a list tuples: (item, caught_exception)
    def process(self, items: List[T]) -> List[Tuple[T, Error]]:
        errors = []
        count = len(items)
        for index, item in enumerate(items):
            try:
                self.task.process(index, count, item)
            except Error as error:
                errors.append((item, error))
        return errors


class ParallelExecutor(Generic[T]):
    """Run the tasks using `n` threads, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T], max_workers: int) -> None:
        self.task = task
        self.max_workers = max_workers
        self.stdout_lock = RLock()

    # Collected errors as a list tuples: (item, caught_exception)
    def process(self, items: List[T]) -> List[Tuple[T, Error]]:
        errors = []
        count = len(items)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures_to_item = {
                executor.submit(self.process_item, index, count, item): item
                for (index, item) in enumerate(items)
            }
            for future in as_completed(futures_to_item):
                item = futures_to_item[future]
                try:
                    future.result()
                except Error as error:
                    errors.append((item, error))
        return errors

    def process_item(self, index: int, count: int, item: T) -> None:
        with self.stdout_lock:
            erase_last_line()

        self.task.process(index, count, item)

        with self.stdout_lock:
            erase_last_line()


def run_sequence(items: List[T], task: Task[Any]) -> List[Tuple[T, Error]]:
    executor = SequentialExecutor(task)
    return executor.process(items)


def run_parallel(
    items: List[T], task: Task[Any], *, max_workers: int
) -> List[Tuple[T, Error]]:
    executor = ParallelExecutor(task, max_workers=max_workers)
    return executor.process(items)
