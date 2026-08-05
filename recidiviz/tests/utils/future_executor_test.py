# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Implements tests for FutureExecutor. """
import threading
from collections.abc import Iterator
from typing import Any

import pytest

from recidiviz.utils.future_executor import (
    map_fn_with_progress_bar_results,
    map_with_bounded_concurrency,
)


def test_map_fn_with_progress_bar_results(capfd: Any) -> None:
    def fake_futures_work_function(
        number_and_raises: tuple[int, ValueError | None]
    ) -> int:
        number, raises = number_and_raises
        if raises:
            raise raises
        return number

    ten_minutes = 10 * 60
    value_error = ValueError("An exception happened!")
    result = map_fn_with_progress_bar_results(
        work_fn=fake_futures_work_function,
        work_items=[
            (1, None),
            (2, None),
            (3, value_error),
        ],
        max_workers=8,
        overall_timeout_sec=ten_minutes,
        single_work_item_timeout_sec=ten_minutes,
        progress_bar_message="test progress message",
    )
    assert sorted(result.successes, key=lambda x: x[0]) == [
        ((1, None), 1),
        ((2, None), 2),
    ]
    assert result.exceptions == [((3, value_error), value_error)]
    assert capfd.readouterr().err == (
        "\x1b[?25l"
        "\r"
        "\rtest progress message |                                | 0/3"
        "\rtest progress message |##########                      | 1/3"
        "\rtest progress message |#####################           | 2/3"
        "\rtest progress message |################################| 3/3"
        "\n"
        "\x1b[?25h"
    )


def test_map_with_bounded_concurrency_returns_all_results() -> None:
    with map_with_bounded_concurrency(
        work_fn=lambda x: x * 2, items=range(50), max_concurrency=4
    ) as completed_items:
        assert sorted(c.result for c in completed_items) == [x * 2 for x in range(50)]


def test_map_with_bounded_concurrency_runs_work_concurrently() -> None:
    # Each call blocks on a barrier until `max_concurrency` calls are in
    # flight, so the batch can only complete if that many run concurrently.
    max_concurrency = 4
    barrier = threading.Barrier(max_concurrency, timeout=10)

    def work_fn(item: int) -> int:
        barrier.wait()
        return item

    with map_with_bounded_concurrency(
        work_fn=work_fn, items=range(max_concurrency), max_concurrency=max_concurrency
    ) as completed_items:
        assert len(list(completed_items)) == max_concurrency


def test_map_with_bounded_concurrency_never_exceeds_max_concurrency() -> None:
    max_concurrency = 3
    lock = threading.Lock()
    active = 0
    max_active = 0

    def work_fn(item: int) -> int:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            return item
        finally:
            with lock:
                active -= 1

    with map_with_bounded_concurrency(
        work_fn=work_fn, items=range(200), max_concurrency=max_concurrency
    ) as completed_items:
        assert len(list(completed_items)) == 200
    assert max_active <= max_concurrency


def test_map_with_bounded_concurrency_lazy_items_consumed_incrementally() -> None:
    # A generator of items is pulled from only as concurrency slots free up,
    # never drained up front.
    pulled = 0

    def item_generator() -> Iterator[int]:
        nonlocal pulled
        for i in range(100):
            pulled += 1
            yield i

    with map_with_bounded_concurrency(
        work_fn=lambda x: x, items=item_generator(), max_concurrency=2
    ) as completed_items:
        next(completed_items)
        assert pulled < 20
        list(completed_items)

    assert pulled == 100


def test_map_with_bounded_concurrency_early_exit_cancels_pending_work() -> None:
    # Breaking out of the results loop must not execute (or wait on) the rest
    # of the backlog: exiting the context cancels everything that has not yet
    # started.
    executed: list[int] = []

    def work_fn(item: int) -> int:
        executed.append(item)
        return item

    with map_with_bounded_concurrency(
        work_fn=work_fn, items=range(500), max_concurrency=2
    ) as completed_items:
        next(completed_items)

    # Only the items already submitted around the time of the early exit ran;
    # the hundreds of pending ones were cancelled, not executed.
    assert len(executed) < 20


def test_map_with_bounded_concurrency_pairs_each_result_with_its_item() -> None:
    # A work_fn whose return value cannot identify its input still lets the
    # caller tell which item produced which outcome.
    with map_with_bounded_concurrency(
        work_fn=lambda x: None if x % 2 else "kept",
        items=range(6),
        max_concurrency=3,
    ) as completed_items:
        skipped = sorted(c.item for c in completed_items if c.result is None)

    assert skipped == [1, 3, 5]


def test_map_with_bounded_concurrency_exception_raised_on_reading_result() -> None:
    def work_fn(item: int) -> int:
        if item == 1:
            raise ValueError("work_fn failed on item [1]")
        return item

    with map_with_bounded_concurrency(
        work_fn=work_fn, items=[0, 1], max_concurrency=1
    ) as completed_items:
        by_item = {c.item: c for c in completed_items}

    assert by_item[0].succeeded
    assert by_item[0].result == 0

    assert not by_item[1].succeeded
    assert isinstance(by_item[1].exception, ValueError)
    with pytest.raises(ValueError, match=r"^work_fn failed on item \[1\]$"):
        _ = by_item[1].result


def test_map_with_bounded_concurrency_one_failing_item_is_survivable() -> None:
    # The whole point of carrying the exception instead of raising it from the
    # iterator: a caller can record the failure and keep going. The iterator is a
    # generator, so an exception thrown from inside it would have ended iteration
    # at item 1 and items 2..9 would never be produced.
    def work_fn(item: int) -> int:
        if item == 1:
            raise ValueError("work_fn failed on item [1]")
        return item

    succeeded: list[int] = []
    failed: list[int] = []

    with map_with_bounded_concurrency(
        work_fn=work_fn, items=range(10), max_concurrency=2
    ) as completed_items:
        for completed in completed_items:
            try:
                succeeded.append(completed.result)
            except ValueError:
                failed.append(completed.item)

    assert sorted(succeeded) == [0, 2, 3, 4, 5, 6, 7, 8, 9]
    assert failed == [1]


def test_map_with_bounded_concurrency_invalid_max_concurrency() -> None:
    with pytest.raises(
        ValueError, match=r"^max_concurrency must be positive, got \[0\]$"
    ):
        with map_with_bounded_concurrency(
            work_fn=lambda x: x, items=[1], max_concurrency=0
        ):
            pass
