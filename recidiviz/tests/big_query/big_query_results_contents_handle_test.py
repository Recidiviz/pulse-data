# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for BigQueryResultsContentsHandle."""

from typing import Any, Dict, Iterator, Union

import pytest
from google.cloud.bigquery.table import Row

from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)


def test_simple_empty() -> None:
    handle: BigQueryResultsContentsHandle[
        Dict[str, Any]
    ] = BigQueryResultsContentsHandle(
        query_job=iter(())  # type: ignore
    )
    results = list(handle.get_contents_iterator())
    assert not results


def test_iterate_twice() -> None:
    class FakeQueryJob:
        def __iter__(self) -> Iterator[Row]:
            return iter(
                (
                    Row((2010, "00001"), {"bar": 0, "foo": 1}),
                    Row((2020, "00002"), {"bar": 0, "foo": 1}),
                    Row((2030, "00003"), {"bar": 0, "foo": 1}),
                )
            )

    handle: BigQueryResultsContentsHandle[
        Dict[str, Any]
    ] = BigQueryResultsContentsHandle(
        query_job=FakeQueryJob()  # type: ignore
    )

    expected_results = [
        {"bar": 2010, "foo": "00001"},
        {"bar": 2020, "foo": "00002"},
        {"bar": 2030, "foo": "00003"},
    ]

    iterator = handle.get_contents_iterator()

    assert {"bar": 2010, "foo": "00001"} == next(iterator)
    assert {"bar": 2020, "foo": "00002"} == next(iterator)
    assert {"bar": 2030, "foo": "00003"} == next(iterator)
    with pytest.raises(StopIteration):
        next(iterator)

    results = list(handle.get_contents_iterator())
    assert expected_results == results


def test_iterate_with_value_converter() -> None:
    def flip_types(field_name: str, value: Any) -> Union[str, int]:
        if field_name == "foo":
            return int(value)
        if field_name == "bar":
            return str(value)
        raise ValueError(f"Unexpected field name [{field_name}] for value [{value}].")

    handle = BigQueryResultsContentsHandle(
        query_job=iter(  # type: ignore
            (
                Row((2010, "00001"), {"bar": 0, "foo": 1}),
                Row((2020, "00002"), {"bar": 0, "foo": 1}),
                Row((2030, "00003"), {"bar": 0, "foo": 1}),
            )
        ),
        value_converter=flip_types,
    )
    expected_results = [
        {"bar": "2010", "foo": 1},
        {"bar": "2020", "foo": 2},
        {"bar": "2030", "foo": 3},
    ]
    results = list(handle.get_contents_iterator())
    assert expected_results == results


def test_more_than_max_rows() -> None:
    with pytest.raises(ValueError, match=r"^Found more than \[2\] rows in result\.$"):
        list(
            BigQueryResultsContentsHandle(
                query_job=iter(  # type: ignore
                    (
                        Row((2010, "00001"), {"bar": 0, "foo": 1}),
                        Row((2020, "00002"), {"bar": 0, "foo": 1}),
                        Row((2030, "00003"), {"bar": 0, "foo": 1}),
                    )
                ),
                max_expected_rows=2,
            ).get_contents_iterator()
        )


def test_query_object_must_be_iterator_of_gcloud_rows() -> None:
    with pytest.raises(ValueError, match=r"^Found unexpected type for row: \[.*\]\.$"):
        list(
            BigQueryResultsContentsHandle(
                query_job=iter(("test string",)),  # type: ignore
            ).get_contents_iterator()
        )
