# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for utils/metadata.py."""
import unittest

from recidiviz.utils import metadata
from recidiviz.utils.metadata import local_project_id_override


class MetadataTest(unittest.TestCase):
    """Tests for utils/metadata.py."""

    def setUp(self) -> None:
        metadata.allow_local_metadata_call = True

    def tearDown(self) -> None:
        metadata.allow_local_metadata_call = False

    def test_local_project_id_override(self) -> None:
        original_project = metadata.project_id()

        with local_project_id_override("recidiviz-456"):
            self.assertEqual("recidiviz-456", metadata.project_id())

        self.assertEqual(original_project, metadata.project_id())

        with local_project_id_override("recidiviz-678"):
            self.assertEqual("recidiviz-678", metadata.project_id())

        self.assertEqual(original_project, metadata.project_id())

    def test_local_project_id_override_throws_if_called_nested(self) -> None:
        original_project = metadata.project_id()

        with local_project_id_override("recidiviz-456"):
            self.assertEqual("recidiviz-456", metadata.project_id())
            with self.assertRaises(ValueError):
                with local_project_id_override("recidiviz-678"):
                    pass
            self.assertEqual("recidiviz-456", metadata.project_id())

        self.assertEqual(original_project, metadata.project_id())
