# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Util functions common to all tests."""
import os
import re
from re import Pattern
from types import TracebackType
from typing import Collection

import attr


def print_visible_header_label(label_text: str) -> None:
    """Prints three long lines of '*' characters, followed by a text label on
    its own line. Used to print a very visible header between other debug
    printouts.
    """
    for _ in range(0, 3):
        print("*" * 200)
    print(f"\n{label_text}")


def in_docker() -> bool:
    """Returns: True if running in a Docker container, else False"""
    if not os.path.exists("/proc/1/cgroup"):
        return False
    with open("/proc/1/cgroup", "rt", encoding="utf-8") as ifh:
        return "docker" in ifh.read()


def _format_excs(excs: Collection[object]) -> str:
    return "\n".join(f"\t-{e.__class__.__name__}: {e}" for e in excs)


@attr.define
class _AssertRaisesGroupException:
    """Util for validating exception group failures.

    message_regex           - regex for matching the top-level ExceptionGroup message
    included_sub_exceptions - list of tuples of expected Exceptions and regex patterns.
                              will assert that the exception group has at least one exception
                              that matches those two keys
    strict                  - boolean flag for if all actual sub-exceptions must have a
                              match in included_sub_exceptions.
    """

    message_regex: Pattern
    included_sub_exceptions: list[tuple[type[Exception], Pattern]]
    strict: bool

    def __enter__(self) -> "_AssertRaisesGroupException":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        _exc_tb: TracebackType,
    ) -> bool | None:
        if exc_type is None:
            raise ValueError(f"Expected a {BaseExceptionGroup} but found none")

        if not issubclass(exc_type, BaseExceptionGroup):
            raise ValueError(
                f"Expected [{BaseException.__name__}] but found [{exc_type.__name__}]"
            )

        if not isinstance(exc_val, BaseExceptionGroup):
            raise ValueError(
                f"Expected [{BaseException.__name__}] but found [{exc_type.__name__}]"
            )

        if not self.message_regex.search(str(exc_val)):
            raise ValueError(
                f"Expected top-level message to be [{self.message_regex}] but found [{str(exc_val)}]"
            )

        actual_sub_exceptions = {e: False for e in exc_val.exceptions}
        if not all(isinstance(e, Exception) for e in actual_sub_exceptions):
            raise ValueError("Found sub-exception that is not an Exception subclass")

        for (expected_class, expected_regex) in self.included_sub_exceptions:
            found_match = False
            for actual_exception in actual_sub_exceptions:
                if isinstance(
                    actual_exception, expected_class
                ) and expected_regex.search(str(actual_exception)):
                    actual_sub_exceptions[actual_exception] = True
                    found_match = True

            if not found_match:
                raise ValueError(
                    f"Expected to find [{expected_class.__name__}] with regex "
                    f"[{expected_regex.pattern}] in the following exceptions but did "
                    f"not: \n {_format_excs(actual_sub_exceptions.keys())}"
                )

        if self.strict and not all(actual_sub_exceptions.values()):
            execs_without_match_str = _format_excs(
                [
                    exc
                    for exc, was_matched in actual_sub_exceptions.items()
                    if not was_matched
                ]
            )
            raise ValueError(
                f"Found the following exceptions that had no match in "
                f"{self.included_sub_exceptions}: \n {execs_without_match_str}"
            )
        # returning a truth-y value indicates that the context manager handled the error
        # so it isn't re-raised by python after we exit
        return True


def assert_group_contains_regex(
    message_regex: str,
    sub_exceptions: list[tuple[type[Exception], str]],
    strict: bool = True,
) -> _AssertRaisesGroupException:
    """Validates that an ExceptionGroup was raised with a messages that matches
    |message_regex| and has that every member of |sub_exceptions| has a matching
    sub exception type and regex.
    """
    return _AssertRaisesGroupException(
        message_regex=re.compile(message_regex),
        included_sub_exceptions=[
            (err_cls, re.compile(err_rgx)) for (err_cls, err_rgx) in sub_exceptions
        ],
        strict=strict,
    )
