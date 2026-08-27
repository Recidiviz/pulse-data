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
"""Tests for RawDataTableBigQuerySchemaBuilder"""
from unittest import TestCase

from google.cloud import bigquery

from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_NAME,
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module


class TestRawDataTableBigQuerySchemaBuilder(TestCase):
    """Unit tests for RawDataTableBigQuerySchemaBuilder"""

    def setUp(self) -> None:
        self.region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions_module
        )

    def test_recidiviz_managed_fields_have_not_changed(self) -> None:
        recidiviz_managed_fields = list(
            RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS
        )
        assert len(recidiviz_managed_fields) == 3

        assert recidiviz_managed_fields[0] == FILE_ID_COL_NAME
        assert recidiviz_managed_fields[1] == UPDATE_DATETIME_COL_NAME
        assert recidiviz_managed_fields[2] == IS_DELETED_COL_NAME

    def test_build_schmea_for_big_query_simple(self) -> None:
        config = self.region_raw_file_config.raw_file_configs["tagBasicData"]
        expected_raw_table_field_names = {
            col.name: col.description or "" for col in config.current_columns
        }
        actual_fields_no_managed = (
            RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=config, include_recidiviz_managed_fields=False
            )
        )

        for field in actual_fields_no_managed:
            assert field.name in expected_raw_table_field_names
            assert field.description == expected_raw_table_field_names[field.name]
            assert field.mode == "NULLABLE"
            assert field.field_type == bigquery.enums.SqlTypeNames.STRING.value

        for column in expected_raw_table_field_names:
            assert column in [field.name for field in actual_fields_no_managed]

        actual_fields_with_managed = (
            RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=config
            )
        )

        for field in actual_fields_with_managed:
            if field.name in RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS:
                assert (
                    field
                    == RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS[
                        field.name
                    ]
                )

            else:
                assert field.name in expected_raw_table_field_names
                assert field.description == expected_raw_table_field_names[field.name]
                assert field.mode == "NULLABLE"
                assert field.field_type == bigquery.enums.SqlTypeNames.STRING.value

    def test_build_schema_for_big_query_from_columns(self) -> None:
        config = self.region_raw_file_config.raw_file_configs["tagBasicData"]
        expected_raw_table_field_names = {
            col.name: col.description or "" for col in config.current_columns
        }
        expected_raw_table_field_names["extra_col_1"] = ""
        actual_fields_no_managed = (
            RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config_from_columns(
                raw_file_config=config,
                columns=list(expected_raw_table_field_names.keys()),
                include_recidiviz_managed_fields=False,
            )
        )

        for field in actual_fields_no_managed:
            assert field.name in expected_raw_table_field_names
            assert field.description == expected_raw_table_field_names[field.name]
            assert field.mode == "NULLABLE"
            assert field.field_type == bigquery.enums.SqlTypeNames.STRING.value

        for column in expected_raw_table_field_names:
            assert column in [field.name for field in actual_fields_no_managed]

        actual_fields_with_managed = (
            RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config_from_columns(
                raw_file_config=config,
                columns=list(expected_raw_table_field_names.keys()),
            )
        )

        for field in actual_fields_with_managed:
            if field.name in RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS:
                assert (
                    field
                    == RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS[
                        field.name
                    ]
                )

            else:
                assert field.name in expected_raw_table_field_names
                assert field.description == expected_raw_table_field_names[field.name]
                assert field.mode == "NULLABLE"
                assert field.field_type == bigquery.enums.SqlTypeNames.STRING.value
