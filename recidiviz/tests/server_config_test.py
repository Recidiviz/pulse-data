# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for server_config.py."""
import unittest
from unittest.mock import Mock, patch

import mock
from mock import MagicMock

from recidiviz import server_config
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class TestServerConfig(unittest.TestCase):
    """Tests for server_config.py."""

    @patch(
        f"{server_config.__name__}.get_direct_ingest_states_existing_in_env",
        return_value=[StateCode.US_XX, StateCode.US_WW],
    )
    @patch(
        f"{server_config.__name__}.get_pathways_enabled_states",
        return_value=[StateCode.US_XX.value, StateCode.US_WW.value],
    )
    def test_get_database_keys_for_schema(
        self, state_codes_fn: Mock, _pathways_enabled_states: Mock
    ) -> None:
        all_keys = []
        for schema_type in SchemaType:
            all_keys.extend(server_config.database_keys_for_schema_type(schema_type))

        expected_all_keys = [
            SQLAlchemyDatabaseKey(SchemaType.JAILS, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.OPERATIONS, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.JUSTICE_COUNTS, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.CASE_TRIAGE, db_name="postgres"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_xx_primary"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_ww_primary"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_xx_secondary"),
            SQLAlchemyDatabaseKey(SchemaType.STATE, db_name="us_ww_secondary"),
            SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, db_name="us_xx"),
            SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, db_name="us_ww"),
        ]

        self.assertCountEqual(expected_all_keys, all_keys)

        state_codes_fn.assert_called()

    def test_get_database_keys_for_state_schema(
        self,
    ) -> None:
        self.assertEqual(
            22, len(server_config.database_keys_for_schema_type(SchemaType.STATE))
        )

    @patch("recidiviz.utils.environment.in_gcp_production")
    def test_get_database_keys_for_state_schema_in_production(
        self, mock_in_prod: mock.MagicMock
    ) -> None:
        mock_in_prod.return_value = True

        # Should skip primary/secondary in US_IX and US_OZ
        self.assertEqual(
            18, len(server_config.database_keys_for_schema_type(SchemaType.STATE))
        )
