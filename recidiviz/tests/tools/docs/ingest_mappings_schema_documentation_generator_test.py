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
"""Tests for ingest_mappings_schema_documentation_generator.py"""
import os
import unittest

from recidiviz.tools.docs.ingest_mappings_schema_documentation_generator import (
    generate_documentation,
)


@unittest.skipIf(os.environ.get("TRAVIS") == "true", "docs/ does not exist in Travis")
class IngestMappingsSchemaDocumentationGeneratorTest(unittest.TestCase):
    """Tests for ingest_mappings_schema_documentation_generator.py"""

    def test_generate_entity_documentation_matches_current_markdown_file(self) -> None:
        """Asserts that no one forgets to run the
        ingest_mappings_schema_documentation_generator script.
        """
        self.assertFalse(generate_documentation())
