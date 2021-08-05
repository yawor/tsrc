""" Helpers to run the same task on multiple items and collect errors.

"""

import abc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Generic, List, Optional, TypeVar

import attr
import cli_ui as ui

from tsrc.errors import Error
from tsrc.git import run_git
from tsrc.utils import erase_last_line

T = TypeVar("T")


class ExecutorFailed(Error):
    pass


class Task(Generic[T], metaclass=abc.ABCMeta):
    """Represent an action to be performed."""

    def __init__(self, *, parallel: bool):
        self.parallel = parallel

    def info(self, *args: Any, **kwargs: Any) -> None:
        if not self.parallel:
            ui.info(*args, **kwargs)

    def info_2(self, *args: Any, **kwargs: Any) -> None:
        if not self.parallel:
            ui.info_2(*args, **kwargs)

    def info_3(self, *args: Any, **kwargs: Any) -> None:
        if not self.parallel:
            ui.info_3(*args, **kwargs)

    def info_count(self, index: int, count: int, *args: Any, **kwargs: Any) -> None:
        if not self.parallel:
            ui.info_count(index, count, *args, **kwargs)

    def run_git(self, working_path: Path, *args: str) -> None:
        if self.parallel:
            run_git(working_path, *args, verbose=False)
        else:
            run_git(working_path, *args)

    @abc.abstractmethod
    def describe_item(self, item: T) -> str:
        pass

    @abc.abstractmethod
    def describe_process_start(self, item: T) -> List[ui.Token]:
        """Describe start of the process - when the task is run in parallel"""
        pass

    @abc.abstractmethod
    def describe_process_end(self, item: T) -> List[ui.Token]:
        """Describe end of the process - when the task is run in parallel"""
        pass

    @abc.abstractmethod
    def process(self, index: int, count: int, item: T) -> Optional[List[ui.Token]]:
        """
        Daughter classes should override this method to provide the code
        that processes the item.

        Instances can use self.parallel to know whether they are run
        in parallel with other instances.

        Instances may return a short description of what happened as a string.

        Note: you should use self.info_* and self.run_git so that
        no output is produced when running tasks in parallel.
        """
        pass


@attr.s
class Outcome(Generic[T]):
    errors: Dict[str, str] = attr.ib()
    summary: Dict[str, List[ui.Token]] = attr.ib()


class SequentialExecutor(Generic[T]):
    """Run the task on all items one at a time, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T]) -> None:
        self.task = task

    # Collected errors as a list tuples: (item, caught_exception)
    def process(self, items: List[T]) -> Outcome[T]:
        errors = {}
        count = len(items)
        for index, item in enumerate(items):
            try:
                self.task.process(index, count, item)
            except Error as error:
                ui.error(error)
                item_desc = self.task.describe_item(item)
                errors[item_desc] = str(error)
        return Outcome(errors=errors, summary={})


class ParallelExecutor(Generic[T]):
    """Run the tasks using `n` threads, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T], num_jobs: int) -> None:
        self.task = task
        self.num_jobs = num_jobs
        self.done_count = 0

    def process(self, items: List[T]) -> Outcome:
        if not items:
            return Outcome(errors={}, summary={})
        errors = {}
        summary = {}
        count = len(items)
        with ThreadPoolExecutor(max_workers=self.num_jobs) as executor:
            futures_to_item = {
                executor.submit(self.process_item, index, count, item): item
                for (index, item) in enumerate(items)
            }
            for future in as_completed(futures_to_item):
                item = futures_to_item[future]
                item_desc = self.task.describe_item(item)
                try:
                    result = future.result()
                    if result:
                        summary[item_desc] = result
                except Error as error:
                    errors[item_desc] = str(error)
        erase_last_line()
        return Outcome(errors=errors, summary=summary)

    def process_item(self, index: int, count: int, item: T) -> Optional[List[ui.Token]]:
        tokens = self.task.describe_process_start(item)
        if tokens:
            erase_last_line()
            ui.info_count(index, count, *tokens, end="\r")

        result = self.task.process(index, count, item)

        self.done_count += 1

        tokens = self.task.describe_process_end(item)
        if tokens:
            erase_last_line()
            ui.info_count(self.done_count - 1, count, *tokens, end="\r")
            if self.done_count == count:
                ui.info()
        return result


def process_items(
    items: List[T], task: Task[T], *, num_jobs: Optional[int] = None
) -> Outcome[T]:
    if num_jobs:
        return process_items_parallel(items, task, num_jobs=num_jobs)
    else:
        return process_items_sequence(items, task)


def process_items_parallel(
    items: List[T], task: Task[T], *, num_jobs: int
) -> Outcome[T]:
    task.parallel = True
    executor = ParallelExecutor(task, num_jobs=num_jobs)
    return executor.process(items)


def process_items_sequence(items: List[T], task: Task[T]) -> Outcome[T]:
    task.parallel = False
    executor = SequentialExecutor(task)
    return executor.process(items)
