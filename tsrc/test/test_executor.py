import random
import time

from tsrc.errors import Error
from tsrc.executor import Task, run_parallel, run_sequence


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

    def process(self, index: int, count: int, item: str) -> None:
        # ui.info_count(index, count, "frobnicate", item)
        to_sleep = random.randrange(5, 15)
        time.sleep(to_sleep / 5)
        if item == "failing":
            # print(item, "ko :/")
            raise Kaboom()
        # ui.info(item, "ok !")


def test_doing_nothing() -> None:
    task = FakeTask()
    run_sequence([], task)


def test_happy() -> None:
    task = FakeTask()
    errors = run_sequence(["foo", "bar"], task)
    assert not errors


def test_collect_errors() -> None:
    task = FakeTask()
    errors = run_sequence(["foo", "failing", "bar"], task)
    assert len(errors) == 1
    item, error = errors[0]
    assert item == "failing"
    assert str(error) == "Kaboom"


def test_parallel_happy() -> None:
    task = FakeTask()
    errors = run_parallel(["foo", "bar", "baz", "quux"], task, max_workers=2)
    assert not errors


def test_parallel_sad() -> None:
    task = FakeTask()
    errors = run_parallel(["foo", "bar", "failing", "baz", "quux"], task, max_workers=2)
    assert len(errors) == 1
    item, error = errors[0]
    assert item == "failing"
    assert str(error) == "Kaboom"
