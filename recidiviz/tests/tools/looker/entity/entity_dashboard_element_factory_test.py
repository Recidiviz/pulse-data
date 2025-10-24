# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for EntityDashboardElementFactory."""
import unittest

from recidiviz.looker.lookml_dashboard_element import (
    FULL_SCREEN_WIDTH,
    SMALL_ELEMENT_HEIGHT,
    LookMLDashboardElement,
    LookMLElementType,
    LookMLListen,
    LookMLSort,
)
from recidiviz.tools.looker.entity.entity_dashboard_element_factory import (
    EntityDashboardElementFactory,
)


class TestEntityDashboardElementFactory(unittest.TestCase):
    """Tests the EntityDashboardElementFactory class."""

    def test_info_element(self) -> None:
        element = EntityDashboardElementFactory.info_element()
        self.assertIsInstance(element, LookMLDashboardElement)
        self.assertEqual(element.title, "Info")
        self.assertEqual(element.name, "info")
        self.assertEqual(element.type, LookMLElementType.TEXT)
        self.assertEqual(element.height, SMALL_ELEMENT_HEIGHT)
        self.assertEqual(element.width, FULL_SCREEN_WIDTH)

    def test_actions_element(self) -> None:
        explore = "test_explore"
        element = EntityDashboardElementFactory.actions_element(
            explore, listen=LookMLListen({}), model="recidiviz-testing"
        )
        self.assertIsInstance(element, LookMLDashboardElement)
        self.assertEqual(element.title, "Actions")
        self.assertEqual(element.name, "actions")
        self.assertEqual(element.explore, explore)
        self.assertEqual(element.type, LookMLElementType.SINGLE_VALUE)
        self.assertEqual(element.fields, [f"{explore}.actions"])
        self.assertEqual(element.height, SMALL_ELEMENT_HEIGHT)
        self.assertEqual(element.width, FULL_SCREEN_WIDTH)

    def test_person_periods_timeline_element(self) -> None:
        explore = "test_explore"
        element = EntityDashboardElementFactory.person_periods_timeline_element(
            explore=explore,
            person_periods_view_name="person_periods",
            listen=LookMLListen({}),
            model="recidiviz-testing",
        )
        self.assertIsInstance(element, LookMLDashboardElement)
        self.assertEqual(element.title, "Periods Timeline")
        self.assertEqual(element.name, "periods_timeline")
        self.assertEqual(element.explore, explore)
        self.assertEqual(element.type, LookMLElementType.LOOKER_TIMELINE)
        self.assertEqual(
            element.fields,
            [
                f"{explore}.person_id",
                "person_periods.period_type",
                "person_periods.start_date",
                "person_periods.end_date",
            ],
        )
        self.assertEqual(
            element.sorts, [LookMLSort("person_periods.start_date", desc=True)]
        )
        self.assertFalse(element.group_bars)
        self.assertTrue(element.show_legend)
        self.assertEqual(element.width, FULL_SCREEN_WIDTH)
