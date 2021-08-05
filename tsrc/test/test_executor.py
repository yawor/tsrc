from typing import List

import cli_ui as ui

from tsrc.errors import Error
from tsrc.executor import Task, process_items_parallel, process_items_sequence


class Kaboom(Error):
    def __init__(self) -> None:
        self.message = "Kaboom"


class FakeTask(Task[str]):
    """This is a fake Task that can be used for testing.

    Note that it will raise an instance of the Kaboom exception
    when processing an item whose value is "failing"

    """

    def __init__(self) -> None:
        pass

    def describe_process_start(self, item: str) -> List[ui.Token]:
        return ["Frobnicating", item]

    def describe_process_end(self, item: str) -> List[ui.Token]:
        return [item, "ok"]

    def process(self, index: int, count: int, item: str) -> None:
        if item == "failing":
            raise Kaboom()

    def describe_item(self, item: str) -> str:
        return item


def test_sequence_nothing() -> None:
    task = FakeTask()
    items: List[str] = []
    outcome = process_items_sequence(items, task)
    assert not outcome.summary
    assert not outcome.errors


def test_sequence_happy() -> None:
    task = FakeTask()
    outcome = process_items_sequence(["foo", "bar"], task)
    assert not outcome.errors


def test_sequence_sad() -> None:
    task = FakeTask()
    outcome = process_items_sequence(["foo", "failing", "bar"], task)
    errors = outcome.errors
    assert len(errors) == 1
    assert errors["failing"] == "Kaboom"


def test_parallel_nothing() -> None:
    task = FakeTask()
    items: List[str] = []
    outcome = process_items_parallel(items, task, num_jobs=2)
    assert not outcome.errors
    assert not outcome.summary


def test_parallel_happy() -> None:
    task = FakeTask()
    ui.info("Frobnicating 4 items with two workers")
    outcome = process_items_parallel(["foo", "bar", "baz", "quux"], task, num_jobs=2)
    ui.info("Done")
    assert not outcome.errors


def test_parallel_sad() -> None:
    task = FakeTask()
    outcome = process_items_parallel(
        ["foo", "bar", "failing", "baz", "quux"], task, num_jobs=2
    )
    errors = outcome.errors
    assert len(errors) == 1
    assert errors["failing"] == "Kaboom"
