# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Helpers for processing work items in parallel with a ThreadPoolExecutor."""

import contextlib
import itertools
import sys
from collections.abc import Iterable, Iterator
from concurrent import futures
from typing import Callable, Generic, cast

import attr
from progress.bar import Bar

from recidiviz.utils import structured_logging
from recidiviz.utils.types import T, U


@attr.define(frozen=True, kw_only=True)
class CompletedWorkItem(Generic[T, U]):
    """One item's finished work: the item itself, plus either the value
    |work_fn| returned for it or the exception |work_fn| raised.

    Pairing the item with its outcome matters because a return value often
    cannot identify which item produced it. A work_fn that returns None to mean
    "nothing to do here" leaves the caller unable to say *which* item it
    skipped, and one that returns a bare count or status has the same problem.

    Capturing the exception instead of letting it escape the results iterator is
    what makes a single bad item survivable. That iterator is a generator, and a
    generator that raises is finished — so an exception thrown from inside it
    would end the whole run at the first failing item, no matter how the caller
    handled it. Reading `result` re-raises in the caller's own frame instead, so
    a caller that wraps it in try/except can record the failure and keep
    iterating, while a caller that does not still fails loudly.
    """

    item: T = attr.ib()
    """The input item this work was done for."""

    _value: U | None = attr.ib()
    """Raw value work_fn returned, or None if it raised. Read `result` instead:
    a work_fn whose return type includes None makes this ambiguous on its own,
    and `exception` is the real discriminator."""

    exception: BaseException | None = attr.ib()
    """The exception work_fn raised for this item, or None if it succeeded."""

    @property
    def succeeded(self) -> bool:
        return self.exception is None

    @property
    def result(self) -> U:
        """Returns work_fn's value for this item, re-raising the exception it
        raised if the work failed."""
        if self.exception is not None:
            raise self.exception
        # Narrowing only: when no exception was raised, _value is whatever
        # work_fn returned, which is a valid U even when U includes None.
        return cast(U, self._value)


@contextlib.contextmanager
def map_with_bounded_concurrency(
    *,
    work_fn: Callable[[T], U],
    items: Iterable[T],
    max_concurrency: int,
) -> Iterator[Iterator[CompletedWorkItem[T, U]]]:
    """Context manager whose entered value is an iterator producing one
    `CompletedWorkItem` per item as its work completes (not in input order),
    with at most |max_concurrency| items in flight at once.

    Pulling from |items| only as concurrency slots free up — rather than
    submitting everything up front — keeps peak memory proportional to
    |max_concurrency| rather than the total item count, and preserves the
    laziness of a generator passed as |items|.

    Exiting the context — including via an early `break` or an exception in
    the consuming loop — cancels all not-yet-started work, so partial
    consumption never blocks on the unprocessed backlog:

        with map_with_bounded_concurrency(
            work_fn=process, items=work_items, max_concurrency=8
        ) as completed_items:
            for completed in completed_items:
                result = completed.result  # raises if work_fn raised

    The iterator is only valid inside the `with` block. It never raises on
    |work_fn|'s behalf: a failure is carried on the yielded `CompletedWorkItem`
    and surfaces only when the caller reads `result`. A caller that wants one
    bad item to be survivable rather than fatal can therefore handle it and
    carry on iterating:

        for completed in completed_items:
            try:
                result = completed.result
            except SomeExpectedError:
                logging.error("Failed on [%s]", completed.item)
                continue
    """
    if max_concurrency <= 0:
        raise ValueError(f"max_concurrency must be positive, got [{max_concurrency}]")
    executor = futures.ThreadPoolExecutor(max_workers=max_concurrency)
    try:
        yield _bounded_result_iterator(
            executor=executor,
            work_fn=work_fn,
            items=items,
            max_concurrency=max_concurrency,
        )
    finally:
        # On abnormal exit this tears the run down promptly: queued futures are
        # cancelled before they start. On normal exit (iterator fully consumed)
        # it is a no-op. shutdown does not wait, so at most the in-flight
        # work_fn calls finish in the background.
        executor.shutdown(wait=False, cancel_futures=True)


def _completed_work_item(
    *, item: T, future: "futures.Future[U]"
) -> CompletedWorkItem[T, U]:
    """Returns |item|'s finished work, read out of its already-completed
    |future|. Captures a raised exception rather than propagating it, so the
    results generator stays alive across a failing item."""
    exception = future.exception()
    return CompletedWorkItem(
        item=item,
        value=None if exception is not None else future.result(),
        exception=exception,
    )


def _bounded_result_iterator(
    *,
    executor: futures.ThreadPoolExecutor,
    work_fn: Callable[[T], U],
    items: Iterable[T],
    max_concurrency: int,
) -> Iterator[CompletedWorkItem[T, U]]:
    """Generator that submits items to |executor| with at most
    |max_concurrency| in flight, pulling from |items| only as slots free up,
    and yields each item paired with its outcome as the work completes."""
    item_iter = iter(items)

    def submit(item: T) -> "futures.Future[U]":
        return executor.submit(work_fn, item)

    # Keyed by future so a completed future can be paired back to the item it
    # was submitted for; a plain set of futures loses that association.
    item_by_future = {
        submit(item): item for item in itertools.islice(item_iter, max_concurrency)
    }
    while item_by_future:
        done, _ = futures.wait(
            item_by_future.keys(), return_when=futures.FIRST_COMPLETED
        )
        completed = [(future, item_by_future.pop(future)) for future in done]
        # Refill before yielding so workers stay busy while the consumer
        # processes this batch of results.
        item_by_future.update(
            {submit(item): item for item in itertools.islice(item_iter, len(completed))}
        )
        for future, item in completed:
            yield _completed_work_item(item=item, future=future)


@attr.define(kw_only=True)
class ThreadPoolExecutorResult(Generic[T, U]):
    # The items that the operation succeeded for
    successes: list[tuple[T, U]]

    # The items with an operation that raised an exception
    exceptions: list[tuple[T, Exception]]


def map_fn_with_progress_bar_results(
    *,
    work_items: list[T],
    work_fn: Callable[[T], U],
    progress_bar_message: str,
    overall_timeout_sec: int,
    single_work_item_timeout_sec: int,
    max_workers: int = 32,
) -> ThreadPoolExecutorResult[T, U]:
    """Processes all items in |work_items| using the provided |work_fn| in parallel,
    showing a progress bar as work progresses.

    Args:
        work_items: The list of itemps to process, passed as a single arg to |work_fn|
        work_fn: The function used to process items
        progress_bar_message: The message on the progress bar
        overall_timeout_sec: The timeout for ALL items to complete
        single_work_item_timeout_sec: The timeout for any single work item
        max_workers: The maximum number of ThreadPoolExecutor workers to use
    """
    successes = []
    exceptions = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        with Bar(
            progress_bar_message, file=sys.stderr, max=len(work_items), check_tty=False
        ) as progress_bar:
            future_to_work_item = {
                executor.submit(
                    structured_logging.with_context(work_fn), work_item
                ): work_item
                for work_item in work_items
            }
            for future in futures.as_completed(
                future_to_work_item, timeout=overall_timeout_sec
            ):
                work_item = future_to_work_item[future]
                try:
                    result = future.result(timeout=single_work_item_timeout_sec)
                    successes.append((work_item, result))
                except Exception as e:
                    exceptions.append((work_item, e))
                finally:
                    progress_bar.next()
    return ThreadPoolExecutorResult(successes=successes, exceptions=exceptions)


def map_fn_with_results(
    *,
    work_items: list[T],
    work_fn: Callable[[T], U],
    overall_timeout_sec: int,
    single_work_item_timeout_sec: int,
    max_workers: int = 32,
) -> ThreadPoolExecutorResult[T, U]:
    """Processes all items in |work_items| using the provided |work_fn| in parallel

    Args:
        work_items: The list of itemps to process, passed as a single arg to |work_fn|
        work_fn: The function used to process items
        overall_timeout_sec: The timeout for ALL items to complete
        single_work_item_timeout_sec: The timeout for any single work item
        max_workers: The maximum number of ThreadPoolExecutor workers to use
    """
    successes = []
    exceptions = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_work_item = {
            executor.submit(
                structured_logging.with_context(work_fn), work_item
            ): work_item
            for work_item in work_items
        }
        for future in futures.as_completed(
            future_to_work_item, timeout=overall_timeout_sec
        ):
            work_item = future_to_work_item[future]
            try:
                result = future.result(timeout=single_work_item_timeout_sec)
                successes.append((work_item, result))
            except Exception as e:
                exceptions.append((work_item, e))
    return ThreadPoolExecutorResult(successes=successes, exceptions=exceptions)
