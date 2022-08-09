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
"""Tests for WithMetadataBigQueryView"""
import unittest

from mock import patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)


class WithMetadataBigQueryViewTest(unittest.TestCase):
    """Tests for WithMetadataBigQueryView"""

    PROJECT_ID = "recidiviz-project-id"

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.PROJECT_ID

        self.view_delegate = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
        )

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_metadata_query_basic(self) -> None:
        view = WithMetadataQueryBigQueryViewBuilder(
            delegate=self.view_delegate,
            metadata_query="SELECT {value} as col, {state_code} as state_code",
            value="val",
            state_code="US_XX",
        ).build()

        self.assertEqual(view.metadata_query, "SELECT val as col, US_XX as state_code")

    def test_metadata_query_with_overrides(self) -> None:
        address_overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_override")
            .register_sandbox_override_for_entire_dataset("dataset")
            .build()
        )

        view = WithMetadataQueryBigQueryViewBuilder(
            delegate=self.view_delegate,
            metadata_query="SELECT value from `{project_id}.{view_dataset}.table`, {state_code} as state_code",
            state_code="US_XX",
            view_dataset="dataset",
        ).build(address_overrides=address_overrides)

        self.assertEqual(
            view.metadata_query,
            "SELECT value from `recidiviz-project-id.my_override_dataset.table`, US_XX as state_code",
        )
