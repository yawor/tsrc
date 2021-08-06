""" Helpers to run the same task on multiple items and collect errors.

"""

import abc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Generic, List, Optional, TypeVar

import attr
import cli_ui as ui

from tsrc.errors import Error
from tsrc.git import run_git
from tsrc.utils import erase_last_line

T = TypeVar("T")


class ExecutorFailed(Error):
    pass


@attr.s
class Outcome:
    error: Optional[Error] = attr.ib()
    summary: Optional[str] = attr.ib()

    @classmethod
    def empty(cls) -> "Outcome":
        return cls(error=None, summary=None)

    @classmethod
    def from_error(cls, error: Error) -> "Outcome":
        return cls(error=error, summary=None)

    @classmethod
    def from_summary(cls, message: str) -> "Outcome":
        return cls(error=None, summary=message)

    @classmethod
    def from_lines(cls, lines: List[str]) -> "Outcome":
        if lines:
            message = "\n".join(lines)
            return cls(error=None, summary=message)
        else:
            return cls.empty()

    def success(self) -> bool:
        return self.error is None


class OutcomeCollection:
    def __init__(self, outcomes: Dict[str, Outcome]) -> None:
        self.summary = []
        self.errors = {}
        for item, outcome in outcomes.items():
            if outcome.summary:
                self.summary.append(outcome.summary)
            if outcome.error:
                self.errors[item] = outcome.error

    def handle_result(
        self, *, error_message: str, summary_title: Optional[str] = None
    ) -> None:
        if self.summary:
            if summary_title:
                ui.info(summary_title)
            self.print_summary()
        if self.errors:
            ui.error(error_message)
            self.print_errors()
            raise ExecutorFailed

    def print_summary(self) -> None:
        for summary in self.summary:
            ui.info(summary)

    def print_errors(self) -> None:
        for (item, error) in self.errors.items():
            ui.info(ui.red, "*", ui.reset, item, ":", error)


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
        """Return a short description of the item"""
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
    def process(self, index: int, count: int, item: T) -> Outcome:
        """
        Daughter classes should override this method to provide the code
        that processes the item.

        Instances can use self.parallel to know whether they are run
        in parallel with other instances.

        Note: you should use self.info_* and self.run_git so that
        no output is produced when running tasks in parallel.
        """
        pass


class SequentialExecutor(Generic[T]):
    """Run the task on all items one at a time, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T]) -> None:
        self.task = task

    def process(self, items: List[T]) -> Dict[str, Outcome]:
        result = {}
        count = len(items)
        for index, item in enumerate(items):
            item_desc = self.task.describe_item(item)
            try:
                outcome = self.task.process(index, count, item)
            except Error as e:
                outcome = Outcome.from_error(e)
            result[item_desc] = outcome
        return result


class ParallelExecutor(Generic[T]):
    """Run the tasks using `n` threads, while collecting errors that
    occur in the process.
    """

    def __init__(self, task: Task[T], num_jobs: int) -> None:
        self.task = task
        self.num_jobs = num_jobs
        self.done_count = 0
        self.lock = Lock()

    def process(self, items: List[T]) -> Dict[str, Outcome]:
        if not items:
            return {}
        result = {}
        with ThreadPoolExecutor(max_workers=self.num_jobs) as executor:
            count = len(items)
            futures_to_item = {
                executor.submit(self.process_item, index, count, item): item
                for (index, item) in enumerate(items)
            }
            for future in as_completed(futures_to_item):
                item = futures_to_item[future]
                item_desc = self.task.describe_item(item)
                try:
                    outcome = future.result()
                except Error as e:
                    outcome = Outcome.from_error(e)
                result[item_desc] = outcome
        erase_last_line()
        return result

    def process_item(self, index: int, count: int, item: T) -> Outcome:
        tokens = self.task.describe_process_start(item)
        if tokens:
            with self.lock:
                erase_last_line()
                ui.info_count(index, count, *tokens, end="\r")

        result = self.task.process(index, count, item)

        self.done_count += 1

        tokens = self.task.describe_process_end(item)
        if tokens:
            with self.lock:
                erase_last_line()
                ui.info_count(self.done_count - 1, count, *tokens, end="\r")
                if self.done_count == count:
                    ui.info()

        return result


def process_items(
    items: List[T], task: Task[T], *, num_jobs: Optional[int] = None
) -> OutcomeCollection:
    if num_jobs:
        res = process_items_parallel(items, task, num_jobs=num_jobs)
    else:
        res = process_items_sequence(items, task)
    return OutcomeCollection(res)


def process_items_parallel(
    items: List[T], task: Task[T], *, num_jobs: int
) -> Dict[str, Outcome]:
    task.parallel = True
    executor = ParallelExecutor(task, num_jobs=num_jobs)
    return executor.process(items)


def process_items_sequence(items: List[T], task: Task[T]) -> Dict[str, Outcome]:
    task.parallel = False
    executor = SequentialExecutor(task)
    return executor.process(items)
