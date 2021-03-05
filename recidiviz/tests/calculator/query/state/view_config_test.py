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
"""Tests for view_config.py."""

import unittest

import mock

from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    BigQueryViewBuilderShouldNotBuildError,
)
from recidiviz.calculator.query.state import view_config
from recidiviz.metrics.export import export_config
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.utils import fakes


class ViewExportConfigTest(unittest.TestCase):
    """Tests for the export variables in view_config.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    @mock.patch("google.cloud.bigquery.Client")
    def test_VIEW_COLLECTION_EXPORT_CONFIGS_types(
        self, _mock_client: mock.MagicMock
    ) -> None:
        """Make sure that all view_builders in the view_builders_to_export attribute of
        VIEW_COLLECTION_EXPORT_CONFIGS are of type BigQueryViewBuilder, and that running view_builder.build()
        produces a BigQueryView."""
        for dataset_export_config in export_config.VIEW_COLLECTION_EXPORT_CONFIGS:
            for view_builder in dataset_export_config.view_builders_to_export:
                self.assertIsInstance(view_builder, BigQueryViewBuilder)

                try:
                    view = view_builder.build()
                except BigQueryViewBuilderShouldNotBuildError:
                    continue

                self.assertIsInstance(view, BigQueryView)

    @staticmethod
    def test_building_all_views() -> None:
        """Tests that all view_builders in VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE successfully pass validations and build."""
        for view_builder in view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE:
            _ = view_builder.build()
