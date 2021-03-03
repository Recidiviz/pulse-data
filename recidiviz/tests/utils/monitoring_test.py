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
"""Tests for utils/monitoring.py"""
import threading

import pytest
from mock import patch

from recidiviz.utils import monitoring


# pylint: disable=redefined-outer-name
@pytest.fixture
def mock_mmap():
    with patch("recidiviz.utils.monitoring.stats") as mock:
        yield mock.return_value.stats_recorder.new_measurement_map.return_value


def test_measurements(mock_mmap):
    tags = {"alice": "foo"}
    with monitoring.measurements(tags) as mmap:
        tags["bob"] = "bar"
        assert mmap == mock_mmap

    assert_recorded_tags(mock_mmap, [{"alice": "foo", "bob": "bar"}])


def test_measurements_with_exception(mock_mmap):
    with pytest.raises(Exception):
        tags = {"alice": "foo"}
        with monitoring.measurements(tags) as mmap:
            tags["bob"] = "bar"
            assert mmap == mock_mmap
            raise Exception

    assert_recorded_tags(mock_mmap, [{"alice": "foo", "bob": "bar"}])


def test_measurements_with_region_decorator(mock_mmap):
    @monitoring.with_region_tag
    def inner(_region_code):
        tags = {"alice": "foo"}
        with monitoring.measurements(tags) as mmap:
            tags["bob"] = "bar"
            assert mmap == mock_mmap

    inner("us_ny")

    assert_recorded_tags(
        mock_mmap, [{"alice": "foo", "bob": "bar", monitoring.TagKey.REGION: "us_ny"}]
    )


def test_tags_multiple_threads():
    results = {}

    @monitoring.with_region_tag
    def inner(region_code):
        results[region_code] = monitoring.context_tags()

    threads = [
        threading.Thread(target=inner, args=[region_code])
        for region_code in ["us_ny", "us_pa"]
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert {region: tags.map for region, tags in results.items()} == {
        "us_ny": {monitoring.TagKey.REGION: "us_ny"},
        "us_pa": {monitoring.TagKey.REGION: "us_pa"},
    }


def test_measurements_with_push_tags(mock_mmap):
    tags = {"alice": "foo"}

    # inside of pushed tag
    with monitoring.push_tags({"eve": "baz"}):
        with monitoring.measurements(tags) as mmap:
            tags["bob"] = "bar"
            assert mmap == mock_mmap

    # outside of pushed tag
    with monitoring.measurements(tags) as mmap:
        tags["other"] = "thing"
        assert mmap == mock_mmap

    assert_recorded_tags(
        mock_mmap,
        [
            {"alice": "foo", "bob": "bar", "eve": "baz"},
            {"alice": "foo", "bob": "bar", "other": "thing"},
        ],
    )


def test_measurements_with_push_tags_and_exception(mock_mmap):
    tags = {"alice": "foo"}

    # inside of pushed tag
    with pytest.raises(Exception):
        with monitoring.push_tags({"eve": "baz"}):
            with monitoring.measurements(tags) as mmap:
                tags["bob"] = "bar"
                assert mmap == mock_mmap
                raise Exception

    # outside of pushed tag
    with monitoring.measurements(tags) as mmap:
        tags["other"] = "thing"
        assert mmap == mock_mmap

    assert_recorded_tags(
        mock_mmap,
        [
            {"alice": "foo", "bob": "bar", "eve": "baz"},
            {"alice": "foo", "bob": "bar", "other": "thing"},
        ],
    )


def assert_recorded_tags(mock_mmap, expected_tag_calls):
    calls = mock_mmap.record.call_args_list
    assert len(calls) == len(expected_tag_calls)
    for call, expected_tags in zip(calls, expected_tag_calls):
        (args, kwargs) = call
        assert kwargs == {}
        (result_tags,) = args
        assert result_tags.map == expected_tags
