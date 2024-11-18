# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for big query labels utils."""

from unittest import TestCase

from recidiviz.big_query.big_query_job_labels import (
    BigQueryJobLabel,
    coalesce_job_labels,
)


class BigQueryLabelsTest(TestCase):
    """Tests associated with the BigQueryJobLabel class"""

    def test_coalesce_labels_simple(self) -> None:
        assert not coalesce_job_labels(should_throw_on_conflict=False)

        # simple combination
        assert coalesce_job_labels(
            BigQueryJobLabel(key="key", value="value"),
            BigQueryJobLabel(key="key2", value="value"),
            should_throw_on_conflict=False,
        ) == {"key": "value", "key2": "value"}

        # parent labels are expanded
        assert coalesce_job_labels(
            BigQueryJobLabel(
                key="child",
                value="iii",
                parents=[
                    BigQueryJobLabel(
                        key="parent",
                        value="ii",
                        parents=[
                            BigQueryJobLabel(key="grandparent_i", value="i"),
                            BigQueryJobLabel(key="grandparent_ii", value="i"),
                        ],
                    )
                ],
            ),
            should_throw_on_conflict=False,
        ) == {
            "child": "iii",
            "parent": "ii",
            "grandparent_i": "i",
            "grandparent_ii": "i",
        }

        # parent labels are expanded and combined
        assert coalesce_job_labels(
            BigQueryJobLabel(
                key="child",
                value="iii",
                parents=[
                    BigQueryJobLabel(
                        key="parent",
                        value="ii",
                        parents=[BigQueryJobLabel(key="grandparent", value="i")],
                    )
                ],
            ),
            BigQueryJobLabel(
                key="alto_sax",
                value="smallest",
                parents=[
                    BigQueryJobLabel(
                        key="tenor_sax",
                        value="a-little-bigger",
                    )
                ],
            ),
            should_throw_on_conflict=False,
        ) == {
            "child": "iii",
            "parent": "ii",
            "grandparent": "i",
            "alto_sax": "smallest",
            "tenor_sax": "a-little-bigger",
        }

    def test_coalesce_labels_conflicts(self) -> None:
        # same keys, same values is ok
        assert coalesce_job_labels(
            BigQueryJobLabel(
                key="key",
                value="value",
            ),
            BigQueryJobLabel(
                key="key",
                value="value",
            ),
            should_throw_on_conflict=True,
        ) == {"key": "value"}

        # same keys, different values, throws
        with self.assertRaisesRegex(
            ValueError, r"Found conflicting labels for key \[key\]: .*"
        ):
            coalesce_job_labels(
                BigQueryJobLabel(
                    key="key",
                    value="value",
                ),
                BigQueryJobLabel(
                    key="key",
                    value="-value-",
                ),
                should_throw_on_conflict=True,
            )

        # same keys, different values takes the first
        assert coalesce_job_labels(
            BigQueryJobLabel(
                key="key",
                value="value",
            ),
            BigQueryJobLabel(
                key="key",
                value="-value-",
            ),
            should_throw_on_conflict=False,
        ) == {"key": "value"}

    def test_attr_validators(self) -> None:
        with self.assertRaises(TypeError):
            # key has to be 1 char
            BigQueryJobLabel(key="", value="aaaaa")
        # value is okay with empty
        _ok = BigQueryJobLabel(key="aa", value="")

        with self.assertRaises(TypeError):
            # no spaces
            BigQueryJobLabel(key=" ", value="aaaaa")

        with self.assertRaises(TypeError):
            BigQueryJobLabel(key="$$", value="aaaaa")

        with self.assertRaises(TypeError):
            BigQueryJobLabel(key="aa", value=" ")
