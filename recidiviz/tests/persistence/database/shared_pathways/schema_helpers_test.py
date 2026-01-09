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
"""Tests for the schema defined in public_pathways/schema.py."""

from unittest import TestCase

from recidiviz.persistence.database.schema.shared_pathways.schema_helpers import (
    build_covered_indexes,
)


class TestSchemaHelpers(TestCase):
    """Tests for the Public/Private Pathways schema_helpers"""

    def test_build_covered_indexes(
        self,
    ) -> None:
        dimensions = ["gender", "sex", "race", "age_group"]
        includes = ["person_id"]
        result = build_covered_indexes(
            index_base_name="index_base_name", dimensions=dimensions, includes=includes
        )

        # Check we have the right number of indexes
        self.assertEqual(len(result), len(dimensions))

        # Check each index has the correct properties
        expected_indexes = {
            "index_base_name_age_group": {
                "columns": ["age_group"],
                "postgresql_include": ["gender", "race", "sex", "person_id"],
            },
            "index_base_name_gender": {
                "columns": ["gender"],
                "postgresql_include": ["age_group", "race", "sex", "person_id"],
            },
            "index_base_name_race": {
                "columns": ["race"],
                "postgresql_include": ["age_group", "gender", "sex", "person_id"],
            },
            "index_base_name_sex": {
                "columns": ["sex"],
                "postgresql_include": ["age_group", "gender", "race", "person_id"],
            },
        }

        for index in result:
            self.assertIn(index.name, expected_indexes)
            expected = expected_indexes[index.name]

            # Check the indexed columns
            actual_columns = [str(expr) for expr in index.expressions]
            self.assertEqual(actual_columns, expected["columns"])

            # Check the postgresql_include columns
            actual_include = dict(index.dialect_options["postgresql"])["include"]
            self.assertEqual(actual_include, expected["postgresql_include"])
