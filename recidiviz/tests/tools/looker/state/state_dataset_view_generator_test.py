# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Unit tests for person details LookML View generation"""
import filecmp
import inspect
import os
import tempfile
import unittest
from typing import Any

from freezegun import freeze_time
from mock import Mock, patch

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.tests.persistence.database.schema_entity_converter import fake_schema
from recidiviz.tests.persistence.database.schema_entity_converter.fake_base_schema import (
    FakeBase,
)
from recidiviz.tools.looker.state.state_dataset_view_generator import (
    generate_state_views,
)


class StateViewGenerator(unittest.TestCase):
    """Tests LookML view generation functions for states"""

    @patch(
        "recidiviz.tools.looker.state.state_dataset_view_generator.state_schema",
        new=fake_schema,
    )
    @patch(
        "recidiviz.tools.looker.state.state_dataset_view_generator.get_all_table_classes_in_schema"
    )
    @patch("recidiviz.persistence.database.schema_utils._is_database_entity_subclass")
    @freeze_time("2000-06-30")
    def test_generate_lookml_views(
        self,
        mock_is_database_entity_subclass: Mock,
        mock_get_all_table_classes_in_schema: Mock,
    ) -> None:
        """
        Calls the function under test on a temporary directory and compares the
        result with the fixtures directory, filtering by names ending with
        "view.lkml"
        """

        # We need to make an exception in _is_database_entity_subclass for FakeBase
        def is_database_entity_subclass_for_testing(member: Any) -> bool:
            return (
                inspect.isclass(member)
                and issubclass(member, DatabaseEntity)
                and member is not DatabaseEntity
                and member is not FakeBase
            )

        mock_is_database_entity_subclass.side_effect = (
            is_database_entity_subclass_for_testing
        )

        mock_get_all_table_classes_in_schema.return_value = list(
            fake_schema.FakeBase.metadata.sorted_tables
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            generate_state_views(tmp_dir)
            fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")

            for fixtures_path, _, filenames in os.walk(fixtures_dir):
                # Get the fixtures inner directory corresponding to the temp inner directory
                relpath = os.path.relpath(fixtures_path, start=fixtures_dir)
                tmp_path = os.path.join(tmp_dir, relpath)

                # Ensure every .lkml file in the fixture directory is equal
                # byte-by-byte to the one in the temp directory
                lkml_filenames = filter(
                    lambda name: name.endswith(".view.lkml"), filenames
                )
                _, mismatch, errors = filecmp.cmpfiles(
                    tmp_path, fixtures_path, lkml_filenames, shallow=False
                )
                self.assertFalse(mismatch)
                self.assertFalse(errors)
