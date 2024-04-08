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
"""Tests for the schema defined in pathways/schema.py."""

from unittest import TestCase

from parameterized import parameterized
from sqlalchemy.inspection import inspect

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
)


class TestSchema(TestCase):
    """Tests for the schema defined in pathways/schema.py."""

    VIEW_BUILDERS = [
        (view_builder.view_id, view_builder)
        for view_builder in PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS
    ]

    @parameterized.expand(VIEW_BUILDERS)
    def test_column_order_matches_view(
        self,
        _view_id: str,
        view: WithMetadataQueryBigQueryViewBuilder[SelectedColumnsBigQueryViewBuilder],
    ) -> None:
        table = get_database_entity_by_table_name(pathways_schema, view.view_id)
        table_columns = [column.name for column in inspect(table).c]
        self.assertEqual(table_columns, view.delegate.columns)
