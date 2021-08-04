""" Helpers to run the same task on multiple items and collect errors.

"""

import abc
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import RLock
from typing import Any, Generic, List, Tuple, TypeVar

import cli_ui as ui

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

        It's advised (but not required) to call `ui.info_count(index, count)` at
        the beginning of the overwritten method.
        """
        pass

    def on_failure(self, *, num_errors: int) -> None:
        """Called when the executor ends and `num_errors` is not 0."""
        pass

    def on_success(self) -> None:
        """Called when the task succeeds on one item."""
        pass

    @abc.abstractmethod
    def display_item(self, item: T) -> str:
        """Called to describe the item that caused an error."""
        pass


class SequentialExecutor(Generic[T]):
    """Run the task on all items one at a time, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T]) -> None:
        self.task = task
        # Collected errors as a list tuples: (item, caught_exception)
        self.errors: List[Tuple[T, Error]] = []

    def process(self, items: List[T]) -> None:
        self.task.on_start(num_items=len(items))

        self.errors = []
        num_items = len(items)
        for i, item in enumerate(items):
            self.process_one(i, num_items, item)

        if self.errors:
            self.handle_errors()
        else:
            self.task.on_success()

    def handle_errors(self) -> None:
        self.task.on_failure(num_errors=len(self.errors))
        for item, error in self.errors:
            item_desc = self.task.display_item(item)
            message = [ui.green, "*", " ", ui.reset, ui.bold, item_desc]
            if error.message:
                message.extend([ui.reset, ": ", error.message])
            ui.info(*message, sep="", fileobj=sys.stderr)
        raise ExecutorFailed()

    def process_one(self, index: int, count: int, item: T) -> None:
        try:
            self.task.process(index, count, item)
        except Error as error:
            self.errors.append((item, error))


class ParallelExecutor(Generic[T]):
    """Run the tasks using `n` threads, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T], max_workers: int) -> None:
        self.task = task
        # Collected errors as a list tuples: (item, caught_exception)
        self.errors: List[Tuple[T, Error]] = []
        self.max_workers = max_workers
        self.stdout_lock = RLock()

    def process(self, items: List[T]) -> None:
        count = len(items)
        ui.info_1("Frobnicating", count, "items")
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
                    self.errors.append((item, error))
        ui.info()
        if self.errors:
            self.handle_errors()
        else:
            ui.info_1("All done")

    def process_item(self, index: int, count: int, item: T) -> None:
        with self.stdout_lock:
            erase_last_line()
            ui.info_count(index, count, "frobnicating", item, end="\r")

        self.task.process(index, count, item)

        with self.stdout_lock:
            erase_last_line()
            ui.info_count(index, count, item, "done", end="\r")

    def handle_errors(self) -> None:
        self.task.on_failure(num_errors=len(self.errors))
        for item, error in self.errors:
            item_desc = self.task.display_item(item)
            message = [ui.green, "*", " ", ui.reset, ui.bold, item_desc]
            if error.message:
                message.extend([ui.reset, ": ", error.message])
            ui.info(*message, sep="", fileobj=sys.stderr)
        raise ExecutorFailed()


def run_sequence(items: List[T], task: Task[Any]) -> None:
    executor = SequentialExecutor(task)
    return executor.process(items)


def run_parallel(items: List[T], task: Task[Any], *, max_workers: int) -> None:
    executor = ParallelExecutor(task, max_workers=max_workers)
    return executor.process(items)
