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

"""Tests for validation/checks/sameness_check.py."""
from datetime import date
from typing import Any, Dict, List
from unittest import TestCase

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.table import Row
from mock import patch
from pandas import RangeIndex
from pandas._testing import assert_frame_equal

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.validation.checks.sameness_check import (
    ResultRow,
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
    SamenessPerRowValidationResultDetails,
    SamenessPerViewValidationResultDetails,
)
from recidiviz.validation.validation_config import (
    ValidationMaxAllowedErrorOverride,
    ValidationRegionConfig,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationCategory,
    ValidationCheckType,
    ValidationResultStatus,
)


def make_row(values: Dict[str, Any]) -> Row:
    return Row(list(values.values()), {key: i for i, key in enumerate(values.keys())})


class TestSamenessValidationCheckers(TestCase):
    """Tests for the SamenessPerRowValidationChecker and SamenessPerViewValidationChecker."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "project-id"

        self.client_patcher = patch(
            "recidiviz.validation.checks.sameness_check.BigQueryClientImpl"
        )
        self.mock_client = self.client_patcher.start().return_value

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def test_sameness_check_no_comparison_columns(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found only \[0\] comparison columns, expected at least 2\.$"
        ):
            _ = SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                region_configs={},
            )

    def test_sameness_check_bad_max_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Allowed error value must be between 0\.0 and 1\.0\. Found instead: \[1\.5\]$",
        ):
            _ = SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                comparison_columns=["a", "b", "c"],
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                hard_max_allowed_error=1.5,
                soft_max_allowed_error=1.5,
                region_configs={},
            )

    def test_sameness_check_validation_name(self) -> None:
        check = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            comparison_columns=["a", "b", "c"],
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from literally_anything",
            ),
            region_configs={},
        )
        self.assertEqual(check.validation_name, "test_view")

        check_with_name_suffix = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            validation_name_suffix="b_c_only",
            comparison_columns=["b", "c"],
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from literally_anything",
            ),
            region_configs={},
        )
        self.assertEqual(check_with_name_suffix.validation_name, "test_view_b_c_only")

    def test_sameness_check_numbers_different_values_no_allowed_error(self) -> None:
        self.mock_client.run_query_async.side_effect = [
            # Failed Rows
            [{"a": 98, "b": 100, "c": 99, "error_rate": 0.02}],
            # Total Row Count
            [[4]],
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                soft_max_allowed_error=0.0,
                hard_max_allowed_error=0.0,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                region_configs={},
            ),
        )
        result = job.validation.get_checker().run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[
                        (
                            ResultRow(label_values=(), comparison_values=(98, 100, 99)),
                            0.02,
                        )
                    ],
                    hard_max_allowed_error=0.0,
                    soft_max_allowed_error=0.0,
                    total_num_rows=4,
                ),
            ),
        )

    def test_sameness_check_numbers_multiple_rows_above_margin(self) -> None:
        self.mock_client.run_query_async.side_effect = [
            # Failed Rows
            [
                {"a": 97, "b": 100, "c": 99, "error_rate": 0.03},
                {"a": 14, "b": 21, "c": 14, "error_rate": 0.3333333333333333},
            ],
            # Total Row Count
            [[247]],
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                hard_max_allowed_error=0.02,
                soft_max_allowed_error=0.02,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                region_configs={},
            ),
        )
        result = job.validation.get_checker().run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[
                        (
                            ResultRow(
                                label_values=(), comparison_values=(97.0, 100.0, 99.0)
                            ),
                            0.03,
                        ),
                        (
                            ResultRow(
                                label_values=(), comparison_values=(14.0, 21.0, 14.0)
                            ),
                            0.3333333333333333,
                        ),
                    ],
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    total_num_rows=247,
                ),
            ),
        )

    def test_sameness_check_numbers_multiple_rows_and_some_above_hard_margin(
        self,
    ) -> None:
        self.mock_client.run_query_async.side_effect = [
            # Failed Rows
            [
                {"a": 20, "b": 100, "c": 99, "error_rate": 0.8, "error_type": "hard"},
                {
                    "a": 14,
                    "b": 21,
                    "c": 14,
                    "error_rate": 0.3333333333333333,
                    "error_type": "soft",
                },
            ],
            # Total Row Count
            [[109]],
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                hard_max_allowed_error=0.5,
                soft_max_allowed_error=0.03,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                region_configs={},
            ),
        )
        result = job.validation.get_checker().run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[
                        (
                            ResultRow(
                                label_values=(), comparison_values=(20.0, 100.0, 99.0)
                            ),
                            0.8,
                        ),
                        (
                            ResultRow(
                                label_values=(), comparison_values=(14.0, 21.0, 14.0)
                            ),
                            0.3333333333333333,
                        ),
                    ],
                    hard_max_allowed_error=0.5,
                    soft_max_allowed_error=0.03,
                    total_num_rows=109,
                ),
            ),
        )

    def test_sameness_check_incorrect_max_errors_raises_error(self) -> None:
        self.mock_client.run_query_async.side_effect = [
            # Failed Rows
            [{"a": 10, "b": 10, "c": 10}],
            # Total Row Count
            [[7]],
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"^Value cannot be less than soft_max_allowed_error\. "
            r"Found instead: 0\.01 vs\. 0\.02\. Make sure you are setting both errors\.$",
        ):
            _ = DataValidationJob(
                region_code="US_XX",
                validation=SamenessDataValidationCheck(
                    validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                    validation_type=ValidationCheckType.SAMENESS,
                    comparison_columns=["a", "b", "c"],
                    soft_max_allowed_error=0.02,
                    hard_max_allowed_error=0.01,
                    sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                    view_builder=SimpleBigQueryViewBuilder(
                        dataset_id="my_dataset",
                        view_id="test_view",
                        description="test_view description",
                        view_query_template="select * from literally_anything",
                    ),
                ),
            )

    def test_sameness_check_numbers_one_none(self) -> None:
        self.mock_client.run_query_async.side_effect = [
            # Failed Rows
            [{"a": 3, "b": 3, "c": None, "error_rate": 1.0, "error_type": "hard"}],
            # Total Row Count
            [[94]],
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                region_configs={},
            ),
        )

        result = job.validation.get_checker().run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[
                        (
                            ResultRow(
                                label_values=(), comparison_values=(3.0, 3.0, None)
                            ),
                            1.0,
                        )
                    ],
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    total_num_rows=94,
                ),
            ),
        )


class SamenessPerViewValidationCheckerTest(BigQueryEmulatorTestCase):
    """Tests for the SamenessPerViewValidationChecker error view builder queries."""

    address = BigQueryAddress.from_str("my_dataset.test_data")

    # Table schemas for tests have region_code, label, and then
    # a, b, and c as either string or date fields.
    region_code = bigquery.SchemaField("region_code", "STRING", mode="NULLABLE")
    label = bigquery.SchemaField("label", "STRING", mode="NULLABLE")
    a_string = bigquery.SchemaField("a", "STRING", mode="NULLABLE")
    b_string = bigquery.SchemaField("b", "STRING", mode="NULLABLE")
    c_string = bigquery.SchemaField("c", "STRING", mode="NULLABLE")
    a_date = bigquery.SchemaField("a", "DATE", mode="NULLABLE")
    b_date = bigquery.SchemaField("b", "DATE", mode="NULLABLE")
    c_date = bigquery.SchemaField("c", "DATE", mode="NULLABLE")

    def _create_view(self, view: BigQueryView) -> None:
        self.bq_client.create_or_update_view(view)
        if view.materialized_address:
            self.bq_client.materialize_view_to_table(
                view, use_query_cache=True, view_configuration_changed=True
            )

    def tearDown(self) -> None:
        for dataset in self.bq_client.list_datasets():
            for table in self.bq_client.list_tables(dataset.dataset_id):
                self.bq_client.delete_table(BigQueryAddress.from_table(table))
        super().tearDown()

    def test_strings(self) -> None:
        # Arrange
        self.create_mock_table(
            address=self.address,
            schema=[
                self.region_code,
                self.label,
                self.a_string,
                self.b_string,
                self.c_string,
            ],
            create_dataset=True,
        )

        data = [
            {"region_code": "US_XX", "label": "test", "a": "a", "b": "b", "c": "c"},
            {"region_code": "US_XX", "label": "test2", "a": "a", "b": "b", "c": "c"},
        ]

        self.load_rows_into_table(address=self.address, data=data)

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["label"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from `{project_id}.my_dataset.test_data`",
                ),
            ),
        )

        self._create_view(job.validation.view_builder.build())
        self._create_view(job.validation.error_view_builder.build())

        # Act
        result = job.validation.get_checker().run_check(job)

        # Assert
        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=2,
                    total_num_rows=2,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 1, "b": 1, "c": 1}),
                        (("test2",), {"a": 1, "b": 1, "c": 1}),
                    ],
                ),
            ),
        )

    def test_dates(self) -> None:
        # Arrange
        self.create_mock_table(
            address=self.address,
            schema=[
                self.region_code,
                self.label,
                self.a_date,
                self.b_date,
                self.c_date,
            ],
            create_dataset=True,
        )
        data = [
            {
                "region_code": "US_XX",
                "label": "test",
                "a": date(2020, 1, 2),
                "b": date(2020, 1, 3),
                "c": date(2020, 1, 4),
            }
        ]
        self.load_rows_into_table(address=self.address, data=data)

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["label"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from `{project_id}.my_dataset.test_data`",
                ),
            ),
        )
        self._create_view(job.validation.view_builder.build())
        self._create_view(job.validation.error_view_builder.build())

        # Act
        result = job.validation.get_checker().run_check(job)

        # Assert
        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=1,
                    total_num_rows=1,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 1, "b": 1, "c": 1}),
                    ],
                ),
            ),
        )

    def test_empty_string(self) -> None:
        # Arrange
        self.create_mock_table(
            address=self.address,
            schema=[
                self.region_code,
                self.label,
                self.a_string,
                self.b_string,
                self.c_string,
            ],
            create_dataset=True,
        )
        data = [
            {
                "region_code": "US_XX",
                "label": "test",
                "a": "same",
                "b": "same",
                "c": None,
            }
        ]
        self.load_rows_into_table(address=self.address, data=data)

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["label"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from `{project_id}.my_dataset.test_data`",
                ),
            ),
        )
        self._create_view(job.validation.view_builder.build())
        self._create_view(job.validation.error_view_builder.build())

        # Act
        result = job.validation.get_checker().run_check(job)

        # Assert
        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=1,
                    total_num_rows=1,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 1, "b": 1, "c": 0}),
                    ],
                ),
            ),
        )

    def test_empty_date(self) -> None:
        # Arrange
        self.create_mock_table(
            address=self.address,
            schema=[
                self.region_code,
                self.label,
                self.a_date,
                self.b_date,
                self.c_date,
            ],
            create_dataset=True,
        )
        data = [
            {
                "region_code": "US_XX",
                "label": "test",
                "a": date(2020, 1, 1),
                "b": date(2020, 1, 1),
                "c": None,
            }
        ]
        self.load_rows_into_table(address=self.address, data=data)

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["label"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from `{project_id}.my_dataset.test_data`",
                ),
            ),
        )
        self._create_view(job.validation.view_builder.build())
        self._create_view(job.validation.error_view_builder.build())

        # Act
        result = job.validation.get_checker().run_check(job)

        # Assert
        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=1,
                    total_num_rows=1,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 1, "b": 1, "c": 0}),
                    ],
                ),
            ),
        )

    def test_many_dates(self) -> None:
        # Arrange
        self.create_mock_table(
            address=self.address,
            schema=[
                self.region_code,
                self.label,
                self.a_date,
                self.b_date,
                self.c_date,
            ],
            create_dataset=True,
        )
        error_row = {
            "region_code": "US_XX",
            "label": "test",
            "a": date(2020, 1, 1),
            "b": date(2020, 1, 2),
            "c": date(2020, 1, 3),
        }
        match_row = {
            "region_code": "US_XX",
            "label": "test",
            "a": date(2020, 1, 1),
            "b": date(2020, 1, 1),
            "c": date(2020, 1, 1),
        }

        data = (2 * [error_row]) + (98 * [match_row])
        self.load_rows_into_table(address=self.address, data=data)

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["label"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                hard_max_allowed_error=0.02,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from `{project_id}.my_dataset.test_data`",
                ),
            ),
        )
        self._create_view(job.validation.view_builder.build())
        self._create_view(job.validation.error_view_builder.build())

        # Act
        result = job.validation.get_checker().run_check(job)

        # Assert
        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=2,
                    total_num_rows=100,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 100, "b": 100, "c": 100}),
                    ],
                ),
            ),
        )

    def test_multiple_partitions(self) -> None:
        # Arrange
        self.create_mock_table(
            address=self.address,
            schema=[
                self.region_code,
                bigquery.SchemaField("date", "STRING"),
                self.a_string,
                self.b_string,
            ],
            create_dataset=True,
        )
        all_rows = [
            ["US_XX", "2021-01-31", "1", "1"],
            ["US_XX", "2021-01-31", "2", None],
            ["US_XX", "2021-01-31", "3", "3"],
            ["US_XX", "2021-01-31", None, "4"],
            ["US_XX", "2020-12-31", "1", "1"],
            ["US_XX", "2020-12-31", "3", "3"],
            ["US_XX", "2020-12-31", None, "5"],
        ]
        header = ["region_code", "date", "a", "b"]
        data: list[dict[str, str]] = [
            dict(zip(header, row)) for row in all_rows  # type: ignore
        ]
        self.load_rows_into_table(address=self.address, data=data)

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b"],
                partition_columns=["region_code", "date"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                hard_max_allowed_error=0.0,
                soft_max_allowed_error=0.0,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from `{project_id}.my_dataset.test_data`",
                ),
            ),
        )
        self._create_view(job.validation.view_builder.build())
        self._create_view(job.validation.error_view_builder.build())

        # Act
        result = job.validation.get_checker().run_check(job)

        # Assert
        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=3,
                    total_num_rows=7,
                    hard_max_allowed_error=0.0,
                    soft_max_allowed_error=0.0,
                    non_null_counts_per_column_per_partition=[
                        (("US_XX", "2020-12-31"), {"a": 2, "b": 3}),
                        (("US_XX", "2021-01-31"), {"a": 3, "b": 3}),
                    ],
                ),
            ),
        )

    def test_no_partition_columns(self) -> None:
        # Arrange
        self.create_mock_table(
            address=self.address,
            schema=[
                self.region_code,
                self.label,
                self.a_string,
                self.b_string,
                self.c_string,
            ],
            create_dataset=True,
        )
        error_row = {
            "region_code": "US_XX",
            "label": "test",
            "a": "1",
            "b": "2",
            "c": "3",
        }
        match_row = {
            "region_code": "US_XX",
            "label": "test",
            "a": "1",
            "b": "1",
            "c": "1",
        }
        data = (2 * [error_row]) + (98 * [match_row])
        self.load_rows_into_table(address=self.address, data=data)

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from `{project_id}.my_dataset.test_data`",
                ),
            ),
        )
        self._create_view(job.validation.view_builder.build())
        self._create_view(job.validation.error_view_builder.build())

        # Act
        result = job.validation.get_checker().run_check(job)

        # Assert
        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=2,
                    total_num_rows=100,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (tuple(), {"a": 100, "b": 100, "c": 100}),
                    ],
                ),
            ),
        )


class TestSamenessPerRowValidationCheckerSQL(BigQueryEmulatorTestCase):
    """Tests for the TestSamenessPerRowValidationCheckerSQL error view builder queries."""

    address = BigQueryAddress.from_str("my_dataset.test_data")

    def _initialize_materialized_validation_view(
        self,
        mock_schema: List[bigquery.SchemaField],
        mock_data_rows: List[Dict[str, Any]],
    ) -> SimpleBigQueryViewBuilder:
        """Creates a materialized view whose results have the given schema/data."""
        self.create_mock_table(
            address=self.address, schema=mock_schema, create_dataset=True
        )
        self.load_rows_into_table(address=self.address, data=mock_data_rows)
        view_builder = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="test_view",
            description="test_view description",
            view_query_template="select * from `{project_id}.my_dataset.test_data`",
            should_materialize=True,
        )
        view = view_builder.build()
        self.bq_client.create_or_update_view(view)
        self.bq_client.materialize_view_to_table(
            view, use_query_cache=True, view_configuration_changed=True
        )
        return view_builder

    def _check_error_view_returns_expected_values(
        self, validation: SamenessDataValidationCheck, expected: pd.DataFrame
    ) -> None:
        result_job = self.bq_client.run_query_async(
            query_str=validation.error_view_builder.build().view_query,
            use_query_cache=False,
        )
        result = result_job.to_dataframe()
        assert_frame_equal(expected, result, check_dtype=False, check_column_type=False)

    def test_sameness_check_numbers_same_values(self) -> None:
        view_builder = self._initialize_materialized_validation_view(
            mock_schema=[
                bigquery.SchemaField("label", "INT64"),
                bigquery.SchemaField("a", "INT64"),
                bigquery.SchemaField("b", "INT64"),
                bigquery.SchemaField("c", "INT64"),
            ],
            mock_data_rows=[{"label": 0, "a": 10, "b": 10, "c": 10}],
        )
        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=view_builder,
            region_configs={},
        )
        expected = pd.DataFrame(
            [],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            index=RangeIndex(start=0, stop=0, step=1),
            dtype=int,
        )
        self._check_error_view_returns_expected_values(validation, expected)

    def test_sameness_check_numbers_different_values(self) -> None:
        view_builder = self._initialize_materialized_validation_view(
            mock_schema=[
                bigquery.SchemaField("label", "INT64"),
                bigquery.SchemaField("a", "INT64"),
                bigquery.SchemaField("b", "INT64"),
                bigquery.SchemaField("c", "INT64"),
            ],
            mock_data_rows=[{"label": 0, "a": 10, "b": 0, "c": 10}],
        )
        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=view_builder,
            region_configs={},
        )
        expected = pd.DataFrame(
            [[0, 10, 0, 10, 1, "hard"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            dtype=int,
        )
        self._check_error_view_returns_expected_values(validation, expected)

    def test_sameness_check_numbers_one_null_value(self) -> None:
        view_builder = self._initialize_materialized_validation_view(
            mock_schema=[
                bigquery.SchemaField("label", "INT64"),
                bigquery.SchemaField("a", "INT64"),
                bigquery.SchemaField("b", "INT64"),
                bigquery.SchemaField("c", "INT64"),
            ],
            mock_data_rows=[{"label": 0, "a": 10, "b": None, "c": 10}],
        )
        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=view_builder,
            region_configs={},
        )
        expected = pd.DataFrame(
            [[0, 10, None, 10, 1, "hard"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
        )
        self._check_error_view_returns_expected_values(validation, expected)

    def test_sameness_check_numbers_null_values(self) -> None:
        view_builder = self._initialize_materialized_validation_view(
            mock_schema=[
                bigquery.SchemaField("label", "INT64"),
                bigquery.SchemaField("a", "INT64"),
                bigquery.SchemaField("b", "INT64"),
                bigquery.SchemaField("c", "INT64"),
            ],
            mock_data_rows=[{"label": 0, "a": None, "b": None, "c": None}],
        )
        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=view_builder,
            region_configs={},
        )
        expected = pd.DataFrame(
            [],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            index=RangeIndex(start=0, stop=0, step=1),
            dtype=int,
        )
        self._check_error_view_returns_expected_values(validation, expected)

    def test_sameness_check_numbers_soft_failure(self) -> None:
        view_builder = self._initialize_materialized_validation_view(
            mock_schema=[
                bigquery.SchemaField("label", "INT64"),
                bigquery.SchemaField("a", "INT64"),
                bigquery.SchemaField("b", "INT64"),
                bigquery.SchemaField("c", "INT64"),
            ],
            mock_data_rows=[{"label": 0, "a": 10, "b": 5, "c": 10}],
        )
        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            soft_max_allowed_error=0.01,
            hard_max_allowed_error=1.0,
            view_builder=view_builder,
            region_configs={},
        )
        expected = pd.DataFrame(
            [[0, 10, 5, 10, 0.5, "soft"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
        )
        self._check_error_view_returns_expected_values(validation, expected)

    def test_sameness_check_numbers_region_config_override(self) -> None:
        view_builder = self._initialize_materialized_validation_view(
            mock_schema=[
                bigquery.SchemaField("label", "INT64"),
                bigquery.SchemaField("a", "INT64"),
                bigquery.SchemaField("b", "INT64"),
                bigquery.SchemaField("c", "INT64"),
                bigquery.SchemaField("region_code", "STRING"),
            ],
            mock_data_rows=[
                {"label": 0, "a": 10, "b": 5, "c": 10, "region_code": "US_XX"},
                {"label": 1, "a": 10, "b": 10, "c": 10, "region_code": "US_XX"},
                {"label": 3, "a": 10, "b": 0, "c": 10, "region_code": "US_YY"},
            ],
        )
        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            soft_max_allowed_error=1.0,
            hard_max_allowed_error=1.0,
            view_builder=view_builder,
            region_configs={
                "US_XX": ValidationRegionConfig(
                    region_code="US_XX",
                    num_allowed_rows_overrides={},
                    dev_mode=True,
                    exclusions={},
                    max_allowed_error_overrides={
                        "test_view": ValidationMaxAllowedErrorOverride(
                            region_code="US_XX",
                            hard_max_allowed_error_override=0.6,
                            soft_max_allowed_error_override=0.3,
                            validation_name="test_view",
                            override_reason="testing",
                        )
                    },
                )
            },
        )
        expected = pd.DataFrame(
            [[0, 10, 5, 10, "US_XX", 0.5, "soft"]],
            columns=["label", "a", "b", "c", "region_code", "error_rate", "error_type"],
        )
        self._check_error_view_returns_expected_values(validation, expected)

    def test_sameness_check_numbers_multiple_values(self) -> None:
        view_builder = self._initialize_materialized_validation_view(
            mock_schema=[
                bigquery.SchemaField("label", "INT64"),
                bigquery.SchemaField("a", "INT64"),
                bigquery.SchemaField("b", "INT64"),
                bigquery.SchemaField("c", "INT64"),
            ],
            mock_data_rows=[
                {"label": 0, "a": 10, "b": 10, "c": 10},
                {"label": 1, "a": 10, "b": 0, "c": 10},
            ],
        )
        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=view_builder,
            region_configs={},
        )
        error_expected = pd.DataFrame(
            [[1, 10, 0, 10, 1, "hard"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            dtype=int,
        )
        self._check_error_view_returns_expected_values(validation, error_expected)

        original_expected = pd.DataFrame(
            [[0, 10, 10, 10], [1, 10, 0, 10]],
            columns=["label", "a", "b", "c"],
            dtype=int,
        )
        original_result_job = self.bq_client.run_query_async(
            query_str=validation.view_builder.build().view_query, use_query_cache=False
        )
        original_result = original_result_job.to_dataframe()
        assert_frame_equal(original_expected, original_result, check_dtype=False)


class TestSamenessPerRowValidationResultDetails(TestCase):
    """Tests for the SamenessPerRowValidationResultDetails."""

    def test_success(self) -> None:
        result = SamenessPerRowValidationResultDetails(
            failed_rows=[], hard_max_allowed_error=0.0, soft_max_allowed_error=0.0
        )

        self.assertEqual(
            ValidationResultStatus.SUCCESS, result.validation_result_status()
        )
        self.assertIsNone(result.failure_description())

    def test_failure(self) -> None:
        result = SamenessPerRowValidationResultDetails(
            failed_rows=[
                (ResultRow(label_values=(), comparison_values=(98, 100, 99)), 0.02)
            ],
            hard_max_allowed_error=0.0,
            soft_max_allowed_error=0.0,
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "1 row(s) had unacceptable margins of error. Of those rows, 1 row(s) exceeded the "
            "hard threshold and 0 row(s) exceeded the soft threshold. "
            "The acceptable margin of error is only 0.0 (hard) and 0.0 (soft), "
            "but the validation returned rows with errors as high as 0.02.",
            result.failure_description(),
        )

    def test_failure_very_small_error(self) -> None:
        result = SamenessPerRowValidationResultDetails(
            failed_rows=[
                (ResultRow(label_values=(), comparison_values=(100000, 99999)), 0.00001)
            ],
            hard_max_allowed_error=0.0,
            soft_max_allowed_error=0.0,
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "1 row(s) had unacceptable margins of error. Of those rows, 1 row(s) exceeded the "
            "hard threshold and 0 row(s) exceeded the soft threshold. "
            "The acceptable margin of error is only 0.0 (hard) and 0.0 (soft), "
            "but the validation returned rows with errors as high as 0.0.",
            result.failure_description(),
        )

    def test_failure_multiple_rows(self) -> None:
        result = SamenessPerRowValidationResultDetails(
            failed_rows=[
                (ResultRow(label_values=(), comparison_values=(98, 100, 99)), 0.02),
                (ResultRow(label_values=(), comparison_values=(14, 21, 14)), 0.3333333),
            ],
            hard_max_allowed_error=0.01,
            soft_max_allowed_error=0.01,
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "2 row(s) had unacceptable margins of error. Of those rows, 2 row(s) exceeded the "
            "hard threshold and 0 row(s) exceeded the soft threshold. "
            "The acceptable margin of error is only 0.01 (hard) and 0.01 (soft), "
            "but the validation returned rows with errors as high as 0.3333.",
            result.failure_description(),
        )

    def test_hard_and_soft_failure_multiple_rows(self) -> None:
        result = SamenessPerRowValidationResultDetails(
            failed_rows=[
                (ResultRow(label_values=(), comparison_values=(98, 100, 99)), 0.02),
                (ResultRow(label_values=(), comparison_values=(14, 21, 14)), 0.3333333),
            ],
            hard_max_allowed_error=0.05,
            soft_max_allowed_error=0.01,
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "2 row(s) had unacceptable margins of error. Of those rows, "
            "1 row(s) exceeded the hard threshold and 1 row(s) exceeded the soft threshold. "
            "The acceptable margin of error is only 0.05 (hard) and 0.01 (soft), "
            "but the validation returned rows with errors as high as 0.3333.",
            result.failure_description(),
        )

    def test_soft_failure_multiple_rows(self) -> None:
        result = SamenessPerRowValidationResultDetails(
            failed_rows=[
                (ResultRow(label_values=(), comparison_values=(98, 100, 99)), 0.02),
                (ResultRow(label_values=(), comparison_values=(14, 21, 14)), 0.3333333),
            ],
            hard_max_allowed_error=0.5,
            soft_max_allowed_error=0.01,
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_SOFT, result.validation_result_status()
        )
        self.assertEqual(
            "2 row(s) exceeded the soft_max_allowed_error threshold. "
            "The acceptable margin of error is 0.01 (soft), "
            "but the validation returned rows with errors as high as 0.3333.",
            result.failure_description(),
        )


class TestSamenessPerViewValidationResultDetails(TestCase):
    """Tests for the SamenessPerViewValidationResultDetails."""

    def test_success_no_errors(self) -> None:
        result = SamenessPerViewValidationResultDetails(
            num_error_rows=0,
            total_num_rows=1,
            hard_max_allowed_error=0.0,
            soft_max_allowed_error=0.0,
            non_null_counts_per_column_per_partition=[
                (tuple(), {"a": 1, "b": 1, "c": 1}),
            ],
        )

        self.assertEqual(
            ValidationResultStatus.SUCCESS, result.validation_result_status()
        )
        self.assertIsNone(result.failure_description())

    def test_success_some_errors(self) -> None:
        result = SamenessPerViewValidationResultDetails(
            num_error_rows=5,
            total_num_rows=100,
            hard_max_allowed_error=0.05,
            soft_max_allowed_error=0.05,
            non_null_counts_per_column_per_partition=[
                (tuple(), {"a": 100, "b": 100, "c": 100}),
            ],
        )

        self.assertEqual(
            ValidationResultStatus.SUCCESS, result.validation_result_status()
        )
        self.assertIsNone(result.failure_description())

    def test_failure_some_errors(self) -> None:
        result = SamenessPerViewValidationResultDetails(
            num_error_rows=5,
            total_num_rows=100,
            hard_max_allowed_error=0.04,
            soft_max_allowed_error=0.04,
            non_null_counts_per_column_per_partition=[
                (tuple(), {"a": 100, "b": 100, "c": 100}),
            ],
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "5 out of 100 row(s) did not contain matching strings. "
            "The acceptable margin of error is 0.04 (hard), but the "
            "validation returned an error rate of 0.05.",
            result.failure_description(),
        )

    def test_soft_failure_some_errors(self) -> None:
        result = SamenessPerViewValidationResultDetails(
            num_error_rows=5,
            total_num_rows=100,
            hard_max_allowed_error=0.05,
            soft_max_allowed_error=0.02,
            non_null_counts_per_column_per_partition=[
                (tuple(), {"a": 100, "b": 100, "c": 100}),
            ],
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_SOFT, result.validation_result_status()
        )
        self.assertEqual(
            "5 out of 100 row(s) did not contain matching strings. "
            "The acceptable margin of error is 0.02 (soft), "
            "but the validation returned an error rate of 0.05.",
            result.failure_description(),
        )

    def test_failure_all_errors(self) -> None:
        result = SamenessPerViewValidationResultDetails(
            num_error_rows=1,
            total_num_rows=1,
            hard_max_allowed_error=0.0,
            soft_max_allowed_error=0.0,
            non_null_counts_per_column_per_partition=[
                (tuple(), {"a": 1, "b": 1, "c": 1}),
            ],
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "1 out of 1 row(s) did not contain matching strings. "
            "The acceptable margin of error is 0.0 (hard), but the "
            "validation returned an error rate of 1.0.",
            result.failure_description(),
        )

    def test_multiple_partitions(self) -> None:
        result = SamenessPerViewValidationResultDetails(
            num_error_rows=3,
            total_num_rows=7,
            hard_max_allowed_error=0.0,
            soft_max_allowed_error=0.0,
            non_null_counts_per_column_per_partition=[
                (("US_XX", "2021-01-31"), {"a": 3, "b": 3}),
                (("US_XX", "2020-12-31"), {"a": 2, "b": 3}),
            ],
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "3 out of 7 row(s) did not contain matching strings. "
            "The acceptable margin of error is 0.0 (hard), but the "
            "validation returned an error rate of 0.4286.",
            result.failure_description(),
        )
