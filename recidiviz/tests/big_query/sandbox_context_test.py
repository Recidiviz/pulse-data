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
"""Tests for BigQueryViewSandboxContext"""
import unittest

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.common.constants.states import StateCode


class BigQueryViewSandboxContextTest(unittest.TestCase):
    """Tests for BigQueryViewSandboxContext"""

    def test_sandbox_context(self) -> None:
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="inputs_prefix")
            .register_sandbox_override_for_entire_dataset("parent_dataset")
            .register_sandbox_override_for_address(
                BigQueryAddress.from_str("parent_dataset_2.table")
            )
            .build()
        )

        sandbox_context = BigQueryViewSandboxContext(
            output_sandbox_dataset_prefix="outputs_prefix",
            parent_address_overrides=overrides,
            parent_address_formatter_provider=None,
            state_code_filter=StateCode.US_XX,
        )

        self.assertEqual(
            BigQueryAddress.from_str("outputs_prefix_view_dataset.table"),
            sandbox_context.sandbox_view_address(
                original_view_address=BigQueryAddress.from_str("view_dataset.table")
            ),
        )
        self.assertEqual(
            BigQueryAddress.from_str("outputs_prefix_view_dataset.table_materialized"),
            sandbox_context.sandbox_materialized_address(
                original_materialized_address=BigQueryAddress.from_str(
                    "view_dataset.table_materialized"
                )
            ),
        )
        self.assertEqual(
            None,
            sandbox_context.sandbox_materialized_address(
                original_materialized_address=None
            ),
        )

        # Even though parents_dataset has a different override in the
        # parent_address_overrides, the sandbox output address for view
        # parent_dataset.view uses the output sandbox prefix.
        self.assertEqual(
            BigQueryAddress.from_str("outputs_prefix_parent_dataset.view"),
            sandbox_context.sandbox_view_address(
                original_view_address=BigQueryAddress.from_str("parent_dataset.view")
            ),
        )
        self.assertEqual(
            BigQueryAddress.from_str("outputs_prefix_parent_dataset.view_materialized"),
            sandbox_context.sandbox_materialized_address(
                original_materialized_address=BigQueryAddress.from_str(
                    "parent_dataset.view_materialized"
                )
            ),
        )
