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
"""Tests functionality of Mixins specifically for state entities."""
import datetime
import unittest

import attr

from recidiviz.common.date import DateOrDateTime
from recidiviz.persistence.entity.state.state_entity_mixins import LedgerEntityMixin


class TestLedgerEntity(unittest.TestCase):
    """Tests the setup and validation of LedgerEntity"""

    def test_ledger_entity_validation(self) -> None:
        @attr.s(eq=False, kw_only=True)
        class ExampleLedger(LedgerEntityMixin):
            start: datetime.datetime = attr.ib()
            end: datetime.datetime = attr.ib()

            @property
            def ledger_datetime_field(self) -> DateOrDateTime:
                return self.start

            def __attrs_post_init__(self) -> None:
                self.assert_datetime_less_than(self.start, self.end)

        _ = ExampleLedger(
            start=datetime.datetime(2022, 1, 1),
            end=datetime.datetime(2022, 1, 3),
            sequence_num=None,
        )

        with self.assertRaisesRegex(
            ValueError,
            "Found ExampleLedger  with  datetime 2022-01-01 00:00:00 after  datetime 2001-01-01 00:00:00.",
        ):
            _ = ExampleLedger(
                sequence_num=None,
                start=datetime.datetime(2022, 1, 1),
                end=datetime.datetime(2001, 1, 1),
            )

    def test_ledger_entity_validation_multiple_after_dates(self) -> None:
        @attr.s(eq=False, kw_only=True)
        class ExampleLedger(LedgerEntityMixin):
            START: datetime.datetime = attr.ib()
            END_1: datetime.datetime = attr.ib()
            END_2: datetime.datetime = attr.ib()

            @property
            def ledger_datetime_field(self) -> DateOrDateTime:
                return self.START

            def __attrs_post_init__(self) -> None:
                """A ledger entity may have one or more datetime fields that are strictly after the 'start' datetime field.
                Return them here."""
                self.assert_datetime_less_than(self.START, self.END_1)
                self.assert_datetime_less_than(self.START, self.END_2)

        _ = ExampleLedger(
            sequence_num=None,
            START=datetime.datetime(2022, 1, 1),
            END_1=datetime.datetime(2022, 1, 3),
            END_2=datetime.datetime(2022, 1, 6),
        )

        with self.assertRaisesRegex(
            ValueError,
            "Found ExampleLedger  with  datetime 2022-01-01 00:00:00 after  datetime 2001-01-01 00:00:00.",
        ):
            _ = ExampleLedger(
                sequence_num=None,
                START=datetime.datetime(2022, 1, 1),
                END_1=datetime.datetime(2001, 1, 1),
                END_2=datetime.datetime(2022, 1, 6),
            )

        with self.assertRaisesRegex(
            ValueError,
            "Found ExampleLedger  with  datetime 2022-01-01 00:00:00 after  datetime 1999-01-01 00:00:00.",
        ):
            _ = ExampleLedger(
                sequence_num=None,
                START=datetime.datetime(2022, 1, 1),
                END_1=datetime.datetime(2022, 1, 3),
                END_2=datetime.datetime(1999, 1, 1),
            )
