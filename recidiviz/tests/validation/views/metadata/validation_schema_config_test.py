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

"""Unit tests for validation_schema_config.py"""
from unittest import TestCase

from recidiviz.tests.ingest import fixtures
from recidiviz.validation.views.metadata.validation_schema_config import (
    get_external_validation_schema,
    DatasetSchemaInfo,
    TableSchemaInfo,
)


class TestDatasetSchemaInfoParsing(TestCase):
    """Tests parsing of YAML to create DatasetSchemaInfo objects."""

    def test_happy_path(self) -> None:
        yaml_path = fixtures.as_filepath("schema_config.yaml")
        validation_schema_config = DatasetSchemaInfo.from_yaml(yaml_path)

        expected = DatasetSchemaInfo(
            dataset="fixture_schema",
            tables=[
                TableSchemaInfo(
                    table_name="incarceration_population_by_facility",
                    columns=[
                        "date_of_stay",
                        "facility",
                        "month",
                        "population_count",
                        "region_code",
                        "year",
                    ],
                ),
                TableSchemaInfo(
                    table_name="incarceration_population_person_level",
                    columns=[
                        "date_of_stay",
                        "facility",
                        "person_external_id",
                        "region_code",
                    ],
                ),
            ],
        )

        self.assertEqual(expected, validation_schema_config)

    def test_external_validation_config_parses(self) -> None:
        validation_schema_config = get_external_validation_schema()

        self.assertIsNotNone(validation_schema_config.dataset)
        self.assertTrue(len(validation_schema_config.tables) > 0)

        for table in validation_schema_config.tables:
            self.assertIsNotNone(table.table_name)
            self.assertTrue(len(table.columns) > 0)

    def test_external_validation_config_always_has_region_code(self) -> None:
        validation_schema_config = get_external_validation_schema()

        for table in validation_schema_config.tables:
            self.assertTrue("region_code" in table.columns)
