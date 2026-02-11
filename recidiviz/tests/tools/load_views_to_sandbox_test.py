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
"""Tests for functionality in load_views_to_sandbox.py"""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_address_formatter import (
    LimitZeroBigQueryAddressFormatterProvider,
)
from recidiviz.big_query.big_query_view_update_sandbox_context import (
    BigQueryViewUpdateSandboxContext,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.tools.load_views_to_sandbox import (
    SandboxChangedAddresses,
    ViewChangeType,
    load_collected_views_to_sandbox,
    parse_arguments,
    summary_for_auto_sandbox,
)


class TestSandboxChangedAddresses(unittest.TestCase):
    """Tests for the SandboxChangedAddresses class"""

    def test_empty(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={},
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )

        self.assertEqual(set(), info.changed_view_addresses_to_ignore)
        self.assertEqual(set(), info.changed_view_addresses_to_load)
        self.assertEqual(set(), info.added_views_to_load)
        self.assertEqual(set(), info.updated_views_to_load)
        self.assertFalse(info.has_changes_to_load)

    def test_only_changed_source_table_addresses(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={},
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses={
                BigQueryAddress.from_str("source_dataset.my_table")
            },
            force_include_addresses=None,
        )

        self.assertEqual(set(), info.changed_view_addresses_to_ignore)
        self.assertEqual(set(), info.changed_view_addresses_to_load)
        self.assertEqual(set(), info.added_views_to_load)
        self.assertEqual(set(), info.updated_views_to_load)
        # There should be changes!
        self.assertTrue(info.has_changes_to_load)

    def test_simple_no_ignores(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )

        self.assertEqual(set(), info.changed_view_addresses_to_ignore)
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
            },
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_simple_include_dataset(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include={"dataset_1"},
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )

        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.changed_view_addresses_to_ignore,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
            },
            info.added_views_to_load,
        )
        self.assertEqual(
            set(),
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_simple_ignore_dataset(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore={"dataset_1"},
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
            },
            info.changed_view_addresses_to_ignore,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            set(),
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_state_code_filter(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("us_xx_dataset.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str(
                    "dataset.us_yy_my_view"
                ): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            # This filter does not match any of the added/updated views
            state_code_filter=StateCode.US_WW,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )

        # Both state-specific views ignored because they don't match US_WW
        self.assertEqual(
            {
                BigQueryAddress.from_str("us_xx_dataset.my_view"),
                BigQueryAddress.from_str("dataset.us_yy_my_view"),
            },
            info.changed_view_addresses_to_ignore,
        )

        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
            },
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_state_code_filter_2(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("us_xx_dataset.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str(
                    "dataset.us_yy_my_view"
                ): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            # This filter matches one of the added/updated views
            state_code_filter=StateCode.US_XX,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )

        # US_YY views are ignored but US_XX are included
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset.us_yy_my_view"),
            },
            info.changed_view_addresses_to_ignore,
        )

        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
                BigQueryAddress.from_str("us_xx_dataset.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("us_xx_dataset.my_view"),
            },
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_state_code_filter_force_include(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("us_xx_dataset.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str(
                    "dataset.us_yy_my_view"
                ): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            # This filter matches one of the added/updated views
            state_code_filter=StateCode.US_YY,
            changed_source_table_addresses=set(),
            force_include_addresses={BigQueryAddress.from_str("us_xx_dataset.my_view")},
        )

        # US_XX view would be ignored, but it was force included
        self.assertEqual(set(), info.changed_view_addresses_to_ignore)

        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
                BigQueryAddress.from_str("us_xx_dataset.my_view"),
                BigQueryAddress.from_str("dataset.us_yy_my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("us_xx_dataset.my_view"),
            },
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
                BigQueryAddress.from_str("dataset.us_yy_my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_disallow_changed_datasets_to_ignore_and_include(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Can only set changed_datasets_to_include or changed_datasets_to_ignore, "
            r"but not both.",
        ):
            _ = SandboxChangedAddresses(
                view_address_to_change_type={
                    BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                    BigQueryAddress.from_str(
                        "dataset_2.my_view"
                    ): ViewChangeType.UPDATED,
                    BigQueryAddress.from_str(
                        "dataset_3.my_view"
                    ): ViewChangeType.UPDATED,
                },
                changed_datasets_to_include={"dataset_2"},
                changed_datasets_to_ignore={"dataset_1"},
                changed_addresses_to_include=None,
                changed_addresses_to_ignore=None,
                state_code_filter=None,
                changed_source_table_addresses=set(),
                force_include_addresses=None,
            )

    def test_simple_ignore_addresses(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore={
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.changed_view_addresses_to_ignore,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            set(),
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_combined_ignore_dataset_and_addresses(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_1.my_view_2"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore={"dataset_1"},
            changed_addresses_to_include=None,
            changed_addresses_to_ignore={
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )
        # Both dataset_1 views and dataset_3.my_view should be ignored
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_1.my_view_2"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.changed_view_addresses_to_ignore,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            set(),
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_simple_include_addresses(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include={
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )
        # Only dataset_2.my_view should be ignored (not in include list)
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_2.my_view"),
            },
            info.changed_view_addresses_to_ignore,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.changed_view_addresses_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_1.my_view"),
            },
            info.added_views_to_load,
        )
        self.assertEqual(
            {
                BigQueryAddress.from_str("dataset_3.my_view"),
            },
            info.updated_views_to_load,
        )
        self.assertTrue(info.has_changes_to_load)

    def test_disallow_changed_addresses_to_ignore_and_include(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot set changed_datasets_to_ignore or changed_addresses_to_ignore if "
            r"changed_addresses_to_include is already set. The "
            r"changed_addresses_to_include flag will exclude every other address not "
            r"in that list.",
        ):
            _ = SandboxChangedAddresses(
                view_address_to_change_type={
                    BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                },
                changed_datasets_to_include=None,
                changed_datasets_to_ignore=None,
                changed_addresses_to_include={
                    BigQueryAddress.from_str("dataset_1.my_view")
                },
                changed_addresses_to_ignore={
                    BigQueryAddress.from_str("dataset_2.my_view")
                },
                state_code_filter=None,
                changed_source_table_addresses=set(),
                force_include_addresses=None,
            )

    def test_disallow_dataset_ignore_with_address_include(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot set changed_datasets_to_ignore or changed_addresses_to_ignore if "
            r"changed_addresses_to_include is already set. The "
            r"changed_addresses_to_include flag will exclude every other address not "
            r"in that list.",
        ):
            _ = SandboxChangedAddresses(
                view_address_to_change_type={
                    BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                },
                changed_datasets_to_include=None,
                changed_datasets_to_ignore={"dataset_2"},
                changed_addresses_to_include={
                    BigQueryAddress.from_str("dataset_1.my_view")
                },
                changed_addresses_to_ignore=None,
                state_code_filter=None,
                changed_source_table_addresses=set(),
                force_include_addresses=None,
            )


class TestSummaryForAutoSandbox(unittest.TestCase):
    """Unittests for summary_for_auto_sandbox()."""

    def test_empty(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={},
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses=set(),
            force_include_addresses=None,
        )
        summary = summary_for_auto_sandbox(
            changed_addresses=info, all_view_addresses_to_load=set()
        )
        expected_summary = """
╒══════════════════════════╤══════════════════════╤════════════════════════════════════════════════════╕
│ Category                 │ BigQuery Addresses   │ Hint                                               │
╞══════════════════════════╪══════════════════════╪════════════════════════════════════════════════════╡
│ IGNORED view changes     │ <none>               │ This is the set of views on your branch that has   │
│                          │                      │ changed as compared to deployed views, but you've  │
│                          │                      │ exempted via the --changed_datasets_to_include,    │
│                          │                      │ --changed_datasets_to_ignore,                      │
│                          │                      │ --changed_addresses_to_include,                    │
│                          │                      │ --changed_addresses_to_ignore, or                  │
│                          │                      │ --state_code_filter flags. These views will not be │
│                          │                      │ loaded unless they are also included in the        │
│                          │                      │ DOWNSTREAM section below (can happen if they are   │
│                          │                      │ in the dependency chain between other changed      │
│                          │                      │ views and views you want to load).                 │
├──────────────────────────┼──────────────────────┼────────────────────────────────────────────────────┤
│ ADDED views to load      │ <none>               │ These are the views on your branch that have been  │
│                          │                      │ added as compared to deployed views and will be    │
│                          │                      │ treated as potential 'roots' of the graph of views │
│                          │                      │ to load to your sandbox. If you don't care about   │
│                          │                      │ some of these changes, you can move them to the    │
│                          │                      │ IGNORED section via the                            │
│                          │                      │ --changed_datasets_to_include,                     │
│                          │                      │ --changed_datasets_to_ignore,                      │
│                          │                      │ --changed_addresses_to_include, or                 │
│                          │                      │ --changed_addresses_to_ignore flags.               │
├──────────────────────────┼──────────────────────┼────────────────────────────────────────────────────┤
│ UPDATED views to load    │ <none>               │ These are the views on your branch that have been  │
│                          │                      │ updated (i.e. view query has changed) as compared  │
│                          │                      │ to deployed views and will be treated as potential │
│                          │                      │ 'roots' of the graph of views to load to your      │
│                          │                      │ sandbox. If you don't care about some of these     │
│                          │                      │ changes, you can move them to the IGNORED section  │
│                          │                      │ via the --changed_datasets_to_include,             │
│                          │                      │ --changed_datasets_to_ignore,                      │
│                          │                      │ --changed_addresses_to_include, or                 │
│                          │                      │ --changed_addresses_to_ignore flags.               │
├──────────────────────────┼──────────────────────┼────────────────────────────────────────────────────┤
│ DOWNSTREAM views to load │ <none>               │ These are the views that did not change            │
│                          │                      │ themselves, but will still be loaded as a          │
│                          │                      │ downstream dependency of one of the ADDED/UPDATED  │
│                          │                      │ views / tables. This set of views is impacted by   │
│                          │                      │ the --load_changed_views_only,                     │
│                          │                      │ --load_up_to_addresses, and --load_up_to_datasets  │
│                          │                      │ flags.                                             │
╘══════════════════════════╧══════════════════════╧════════════════════════════════════════════════════╛
""".strip()

        self.assertEqual(expected_summary, summary)

    def test_complex(self) -> None:
        info = SandboxChangedAddresses(
            view_address_to_change_type={
                BigQueryAddress.from_str("dataset_1.my_view"): ViewChangeType.ADDED,
                BigQueryAddress.from_str("dataset_2.my_view"): ViewChangeType.UPDATED,
                BigQueryAddress.from_str("dataset_3.my_view"): ViewChangeType.UPDATED,
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore={"dataset_2", "dataset_4"},
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=None,
            changed_source_table_addresses={
                BigQueryAddress.from_str("source_table_dataset.table_1"),
                BigQueryAddress.from_str("source_table_dataset.table_2"),
                BigQueryAddress.from_str("source_table_dataset.table_3"),
            },
            force_include_addresses=None,
        )
        summary = summary_for_auto_sandbox(
            changed_addresses=info,
            all_view_addresses_to_load={
                BigQueryAddress.from_str("dataset_1.my_view"),
                BigQueryAddress.from_str("dataset_1.my_view_2"),
                BigQueryAddress.from_str("dataset_3.my_view"),
                BigQueryAddress.from_str("dataset_4.my_view"),
            },
        )

        expected_summary = """
╒══════════════════════════╤════════════════════════════════╤════════════════════════════════════════════════════╕
│ Category                 │ BigQuery Addresses             │ Hint                                               │
╞══════════════════════════╪════════════════════════════════╪════════════════════════════════════════════════════╡
│ IGNORED view changes     │ * dataset_2.my_view            │ This is the set of views on your branch that has   │
│                          │                                │ changed as compared to deployed views, but you've  │
│                          │                                │ exempted via the --changed_datasets_to_include,    │
│                          │                                │ --changed_datasets_to_ignore,                      │
│                          │                                │ --changed_addresses_to_include,                    │
│                          │                                │ --changed_addresses_to_ignore, or                  │
│                          │                                │ --state_code_filter flags. These views will not be │
│                          │                                │ loaded unless they are also included in the        │
│                          │                                │ DOWNSTREAM section below (can happen if they are   │
│                          │                                │ in the dependency chain between other changed      │
│                          │                                │ views and views you want to load).                 │
├──────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────┤
│ UPDATED source tables    │ * source_table_dataset.table_1 │ This is the set of overridden source tables to     │
│  to read from            │ * source_table_dataset.table_2 │ read from as specified by the                      │
│                          │ * source_table_dataset.table_3 │ --input_source_table_dataset_overrides_json flag.  │
│                          │                                │ Views will read from the overridden version of the │
│                          │                                │ table in place of these tables.                    │
├──────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────┤
│ ADDED views to load      │ * dataset_1.my_view            │ These are the views on your branch that have been  │
│                          │                                │ added as compared to deployed views and will be    │
│                          │                                │ treated as potential 'roots' of the graph of views │
│                          │                                │ to load to your sandbox. If you don't care about   │
│                          │                                │ some of these changes, you can move them to the    │
│                          │                                │ IGNORED section via the                            │
│                          │                                │ --changed_datasets_to_include,                     │
│                          │                                │ --changed_datasets_to_ignore,                      │
│                          │                                │ --changed_addresses_to_include, or                 │
│                          │                                │ --changed_addresses_to_ignore flags.               │
├──────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────┤
│ UPDATED views to load    │ * dataset_3.my_view            │ These are the views on your branch that have been  │
│                          │                                │ updated (i.e. view query has changed) as compared  │
│                          │                                │ to deployed views and will be treated as potential │
│                          │                                │ 'roots' of the graph of views to load to your      │
│                          │                                │ sandbox. If you don't care about some of these     │
│                          │                                │ changes, you can move them to the IGNORED section  │
│                          │                                │ via the --changed_datasets_to_include,             │
│                          │                                │ --changed_datasets_to_ignore,                      │
│                          │                                │ --changed_addresses_to_include, or                 │
│                          │                                │ --changed_addresses_to_ignore flags.               │
├──────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────┤
│ DOWNSTREAM views to load │ * dataset_1.my_view_2          │ These are the views that did not change            │
│                          │ * dataset_4.my_view            │ themselves, but will still be loaded as a          │
│                          │                                │ downstream dependency of one of the ADDED/UPDATED  │
│                          │                                │ views / tables. This set of views is impacted by   │
│                          │                                │ the --load_changed_views_only,                     │
│                          │                                │ --load_up_to_addresses, and --load_up_to_datasets  │
│                          │                                │ flags.                                             │
╘══════════════════════════╧════════════════════════════════╧════════════════════════════════════════════════════╛
""".strip()
        self.assertEqual(expected_summary, summary)


class TestLoadCollectedViewsSchemasOnly(unittest.TestCase):
    """Tests that --schemas_only results in LimitZeroBigQueryAddressFormatterProvider."""

    LOAD_VIEWS_MODULE = "recidiviz.tools.load_views_to_sandbox"

    @patch(f"{LOAD_VIEWS_MODULE}.get_source_tables_to_pseudocolumns")
    @patch(f"{LOAD_VIEWS_MODULE}.get_deployed_addresses_without_state_code_column")
    @patch(f"{LOAD_VIEWS_MODULE}.metadata")
    @patch(f"{LOAD_VIEWS_MODULE}.validate_builders_not_in_current_source_datasets")
    @patch(
        f"{LOAD_VIEWS_MODULE}.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    @patch(f"{LOAD_VIEWS_MODULE}.check_deployed_view_schemas")
    def test_schemas_only_uses_limit_zero_provider(
        self,
        _mock_check_schemas: MagicMock,
        mock_deploy: MagicMock,
        _mock_validate: MagicMock,
        mock_metadata: MagicMock,
        mock_get_deployed_addrs: MagicMock,
        mock_get_pseudocolumns: MagicMock,
    ) -> None:
        mock_metadata.project_id.return_value = "recidiviz-staging"
        mock_get_deployed_addrs.return_value = set()
        mock_get_pseudocolumns.return_value = {}
        mock_deployment_results = MagicMock()
        mock_deployment_results.view_results = {}
        mock_deploy.return_value = (mock_deployment_results, None)

        fake_builder = MagicMock()
        fake_builder.address = BigQueryAddress.from_str("my_dataset.my_view")

        load_collected_views_to_sandbox(
            sandbox_dataset_prefix="test_prefix",
            state_code_filter=None,
            collected_builders=[fake_builder],
            input_source_table_dataset_overrides_dict=None,
            allow_slow_views=False,
            rematerialize_changed_views_only=False,
            failure_mode=MagicMock(),
            schemas_only=True,
        )

        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        sandbox_context: BigQueryViewUpdateSandboxContext = call_kwargs[
            "view_update_sandbox_context"
        ]
        self.assertIsInstance(
            sandbox_context.parent_address_formatter_provider,
            LimitZeroBigQueryAddressFormatterProvider,
        )


class TestParseArguments(unittest.TestCase):
    """Tests for parse_arguments() arg parsing."""

    def test_schemas_only_parses(self) -> None:
        """Argparse accepts --schemas_only without --state_code_filter."""
        with patch(
            "sys.argv",
            [
                "load_views_to_sandbox",
                "--sandbox_dataset_prefix",
                "test",
                "--schemas_only",
                "auto",
                "--load_changed_views_only",
            ],
        ):
            args = parse_arguments()
        self.assertTrue(args.schemas_only)
        self.assertIsNone(args.state_code_filter)

    def test_state_code_filter_parses(self) -> None:
        """Argparse accepts --state_code_filter without --schemas_only."""
        with patch(
            "sys.argv",
            [
                "load_views_to_sandbox",
                "--sandbox_dataset_prefix",
                "test",
                "--state_code_filter",
                "US_XX",
                "auto",
                "--load_changed_views_only",
            ],
        ):
            args = parse_arguments()
        self.assertFalse(args.schemas_only)
        self.assertEqual(args.state_code_filter, StateCode.US_XX)

    def test_schemas_only_and_state_code_are_mutually_exclusive(
        self,
    ) -> None:
        """Argparse rejects --schemas_only and --state_code_filter together."""
        with self.assertRaises(SystemExit):
            with patch(
                "sys.argv",
                [
                    "load_views_to_sandbox",
                    "--sandbox_dataset_prefix",
                    "test",
                    "--schemas_only",
                    "--state_code_filter",
                    "US_XX",
                    "auto",
                    "--load_changed_views_only",
                ],
            ):
                parse_arguments()
