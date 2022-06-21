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
from mock import Mock, patch
from pandas._testing import assert_frame_equal
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.tests.big_query.big_query_view_test_case import (
    BigQueryViewTestCase,
    PostgresTableSchema,
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

        self.good_string_row = {"p": "test", "a": "same", "b": "same", "c": "same"}
        self.bad_string_row = {
            "p": "test",
            "a": "a_value",
            "b": "b_value",
            "c": "c_value",
        }

        self.good_date_row = {
            "p": "test",
            "a": date(2020, 1, 1),
            "b": date(2020, 1, 1),
            "c": date(2020, 1, 1),
        }
        self.bad_date_row = {
            "p": "test",
            "a": date(2020, 1, 1),
            "b": date(2020, 1, 2),
            "c": date(2020, 1, 3),
        }

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def return_string_values_with_num_bad_rows(
        self, num_bad_rows: int
    ) -> List[Dict[str, str]]:
        return_values = [self.good_string_row] * (100 - num_bad_rows)
        return_values.extend([self.bad_string_row] * num_bad_rows)

        return return_values

    def return_date_values_with_num_bad_rows(
        self, num_bad_rows: int
    ) -> List[Dict[str, Any]]:
        return_values = [self.good_date_row] * (100 - num_bad_rows)
        return_values.extend([self.bad_date_row] * num_bad_rows)

        return return_values

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
        self.mock_client.run_query_async.return_value = [
            {"a": 98, "b": 100, "c": 99, "error_rate": 0.02}
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
                ),
            ),
        )

    def test_sameness_check_numbers_multiple_rows_above_margin(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"a": 97, "b": 100, "c": 99, "error_rate": 0.03},
            {"a": 14, "b": 21, "c": 14, "error_rate": 0.3333333333333333},
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
                ),
            ),
        )

    def test_sameness_check_numbers_multiple_rows_and_some_above_hard_margin(
        self,
    ) -> None:
        self.mock_client.run_query_async.return_value = [
            {"a": 20, "b": 100, "c": 99, "error_rate": 0.8, "error_type": "hard"},
            {
                "a": 14,
                "b": 21,
                "c": 14,
                "error_rate": 0.3333333333333333,
                "error_type": "soft",
            },
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
                ),
            ),
        )

    def test_sameness_check_incorrect_max_errors_raises_error(self) -> None:
        self.mock_client.run_query_async.return_value = [{"a": 10, "b": 10, "c": 10}]

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
        self.mock_client.run_query_async.return_value = [
            {"a": 3, "b": 3, "c": None, "error_rate": 1.0, "error_type": "hard"}
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
                ),
            ),
        )

    def test_sameness_check_strings_different_values_no_allowed_error(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": "a", "b": "b", "c": "c"}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)

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

    def test_sameness_check_dates_different_values_no_allowed_error(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {
                "p": "test",
                "a": date(2020, 1, 2),
                "b": date(2020, 1, 3),
                "c": date(2020, 1, 4),
            }
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)

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

    def test_sameness_check_strings_different_values_handle_empty_string(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": "same", "b": "same", "c": None}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)

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

    def test_sameness_check_date_different_values_handle_empty_date(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": date(2020, 1, 1), "b": date(2020, 1, 1), "c": None}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)
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

    def test_sameness_check_dates_different_values_within_margin(self) -> None:
        num_bad_rows = 2
        hard_max_allowed_error = num_bad_rows / 100

        self.mock_client.run_query_async.side_effect = [
            [self.bad_date_row] * num_bad_rows,
            self.return_date_values_with_num_bad_rows(num_bad_rows),
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                hard_max_allowed_error=hard_max_allowed_error,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)

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

    def test_sameness_check_dates_different_values_above_margin(self) -> None:
        num_bad_rows = 5
        max_allowed_error = (num_bad_rows - 1) / 100  # Below the number of bad rows

        self.mock_client.run_query_async.side_effect = [
            [self.bad_date_row] * num_bad_rows,
            self.return_date_values_with_num_bad_rows(num_bad_rows),
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                hard_max_allowed_error=max_allowed_error,
                soft_max_allowed_error=max_allowed_error,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=5,
                    total_num_rows=100,
                    hard_max_allowed_error=0.04,
                    soft_max_allowed_error=0.04,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 100, "b": 100, "c": 100}),
                    ],
                ),
            ),
        )

    def test_sameness_check_dates_multiple_dates(self) -> None:
        error_rows: List[object] = [
            {"region": "US_XX", "date": "2021-01-31", "a": date(2020, 1, 2), "b": None},
            {"region": "US_XX", "date": "2021-01-31", "a": None, "b": date(2020, 1, 4)},
            {"region": "US_XX", "date": "2020-12-31", "a": None, "b": date(2020, 1, 5)},
        ]
        normal_rows: List[object] = [
            # January 2021
            {
                "region": "US_XX",
                "date": "2021-01-31",
                "a": date(2020, 1, 1),
                "b": date(2020, 1, 1),
            },
            {
                "region": "US_XX",
                "date": "2021-01-31",
                "a": date(2020, 1, 3),
                "b": date(2020, 1, 3),
            },
            # December 2020
            {
                "region": "US_XX",
                "date": "2020-12-31",
                "a": date(2020, 1, 1),
                "b": date(2020, 1, 1),
            },
            {
                "region": "US_XX",
                "date": "2020-12-31",
                "a": date(2020, 1, 3),
                "b": date(2020, 1, 3),
            },
        ]
        self.mock_client.run_query_async.side_effect = [
            error_rows,
            normal_rows + error_rows,
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b"],
                partition_columns=["region", "date"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                hard_max_allowed_error=0.0,
                soft_max_allowed_error=0.0,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)

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
                        (("US_XX", "2021-01-31"), {"a": 3, "b": 3}),
                        (("US_XX", "2020-12-31"), {"a": 2, "b": 3}),
                    ],
                ),
            ),
        )

    def test_sameness_checks_no_partition_columns(self) -> None:
        num_bad_rows = 2
        hard_max_allowed_error = num_bad_rows / 100

        self.mock_client.run_query_async.side_effect = [
            [self.bad_string_row] * num_bad_rows,
            self.return_string_values_with_num_bad_rows(num_bad_rows),
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
                hard_max_allowed_error=hard_max_allowed_error,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = job.validation.get_checker().run_check(job)

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


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class TestSamenessPerRowValidationCheckerSQL(BigQueryViewTestCase):
    """Tests for the TestSamenessPerRowValidationCheckerSQL error view builder queries."""

    def test_sameness_check_numbers_same_values(self) -> None:

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.Integer(),
                    "a": sqltypes.Integer(),
                    "b": sqltypes.Integer(),
                    "c": sqltypes.Integer(),
                }
            ),
            mock_data=pd.DataFrame([[0, 10, 10, 10]], columns=["label", "a", "b", "c"]),
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
            region_configs={},
        )

        result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": int, "a": int, "b": int, "c": int},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        result = result.drop(
            ["potential_max_value", "potential_min_value", "max_value", "min_value"],
            axis=1,
        )

        # TODO(https://github.com/pandas-dev/pandas/issues/40077): Remove the explicit
        #  index argument once this issue is resolved.
        expected = pd.DataFrame(
            [],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            dtype=int,
            index=pd.RangeIndex(start=0, stop=0, step=1),
        )
        print(result.columns)
        assert_frame_equal(expected, result, check_dtype=False)

    def test_sameness_check_numbers_different_values(self) -> None:

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.Integer(),
                    "a": sqltypes.Integer(),
                    "b": sqltypes.Integer(),
                    "c": sqltypes.Integer(),
                }
            ),
            mock_data=pd.DataFrame([[0, 10, 0, 10]], columns=["label", "a", "b", "c"]),
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
            region_configs={},
        )

        result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": int, "a": int, "b": int, "c": int},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        result = result.drop(
            ["potential_max_value", "potential_min_value", "max_value", "min_value"],
            axis=1,
        )

        expected = pd.DataFrame(
            [[0, 10, 0, 10, 1, "hard"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            dtype=int,
        )
        assert_frame_equal(expected, result, check_dtype=False)

    def test_sameness_check_numbers_one_null_value(self) -> None:
        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.Integer(),
                    "a": sqltypes.Integer(),
                    "b": sqltypes.Integer(),
                    "c": sqltypes.Integer(),
                }
            ),
            mock_data=pd.DataFrame(
                [[0, 10, None, 10]], columns=["label", "a", "b", "c"]
            ),
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
            region_configs={},
        )

        result = self.query_view_for_builder(
            validation.error_view_builder,
            data_types={"label": int, "a": int, "b": object, "c": int},
            dimensions=["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        result = result.drop(
            ["potential_max_value", "potential_min_value", "max_value", "min_value"],
            axis=1,
        )

        expected = pd.DataFrame(
            [[0, 10, None, 10, 1, "hard"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
        )
        assert_frame_equal(expected, result, check_dtype=False, check_column_type=False)

    def test_sameness_check_numbers_null_values(self) -> None:
        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.Integer(),
                    "a": sqltypes.Integer(),
                    "b": sqltypes.Integer(),
                    "c": sqltypes.Integer(),
                }
            ),
            mock_data=pd.DataFrame(
                [[0, None, None, None]], columns=["label", "a", "b", "c"]
            ),
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
            region_configs={},
        )

        result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": int, "a": int, "b": int, "c": int},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        result = result.drop(
            ["potential_max_value", "potential_min_value", "max_value", "min_value"],
            axis=1,
        )

        # TODO(https://github.com/pandas-dev/pandas/issues/40077): Remove the explicit
        #  index argument once this issue is resolved.
        expected = pd.DataFrame(
            [],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            dtype=int,
            index=pd.RangeIndex(start=0, stop=0, step=1),
        )
        assert_frame_equal(expected, result, check_dtype=False)

    def test_sameness_check_numbers_soft_failure(self) -> None:

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.Integer(),
                    "a": sqltypes.Integer(),
                    "b": sqltypes.Integer(),
                    "c": sqltypes.Integer(),
                }
            ),
            mock_data=pd.DataFrame([[0, 10, 5, 10]], columns=["label", "a", "b", "c"]),
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            soft_max_allowed_error=0.01,
            hard_max_allowed_error=1.0,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
            region_configs={},
        )

        result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": int, "a": int, "b": int, "c": int},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        result = result.drop(
            ["potential_max_value", "potential_min_value", "max_value", "min_value"],
            axis=1,
        )

        expected = pd.DataFrame(
            [[0, 10, 5, 10, 0.5, "soft"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
        )
        assert_frame_equal(expected, result, check_dtype=False)

    def test_sameness_check_numbers_region_config_override(self) -> None:

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.Integer(),
                    "a": sqltypes.Integer(),
                    "b": sqltypes.Integer(),
                    "c": sqltypes.Integer(),
                    "region_code": sqltypes.String(),
                }
            ),
            mock_data=pd.DataFrame(
                [
                    [0, 10, 5, 10, "US_XX"],
                    [1, 10, 10, 10, "US_XX"],
                    [3, 10, 0, 10, "US_YY"],
                ],
                columns=["label", "a", "b", "c", "region_code"],
            ),
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            soft_max_allowed_error=1.0,
            hard_max_allowed_error=1.0,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
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

        result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": int, "a": int, "b": int, "c": int, "region_code": str},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        result = result.drop(
            ["potential_max_value", "potential_min_value", "max_value", "min_value"],
            axis=1,
        )

        expected = pd.DataFrame(
            [[0, 10, 5, 10, "US_XX", 0.5, "soft"]],
            columns=["label", "a", "b", "c", "region_code", "error_rate", "error_type"],
        )
        assert_frame_equal(expected, result, check_dtype=False)

    def test_sameness_check_numbers_multiple_values(self) -> None:

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.Integer(),
                    "a": sqltypes.Integer(),
                    "b": sqltypes.Integer(),
                    "c": sqltypes.Integer(),
                }
            ),
            mock_data=pd.DataFrame(
                [[0, 10, 10, 10], [1, 10, 0, 10]], columns=["label", "a", "b", "c"]
            ),
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
            region_configs={},
        )

        error_result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": int, "a": int, "b": int, "c": int},
            ["label"],
        )

        original_result = self.query_view_for_builder(
            validation.view_builder,
            {"label": int, "a": int, "b": int, "c": int},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        error_result = error_result.drop(
            ["potential_max_value", "potential_min_value", "max_value", "min_value"],
            axis=1,
        )

        error_expected = pd.DataFrame(
            [[1, 10, 0, 10, 1, "hard"]],
            columns=["label", "a", "b", "c", "error_rate", "error_type"],
            dtype=int,
        )
        assert_frame_equal(error_expected, error_result, check_dtype=False)

        original_expected = pd.DataFrame(
            [[0, 10, 10, 10], [1, 10, 0, 10]],
            columns=["label", "a", "b", "c"],
            dtype=int,
        )
        assert_frame_equal(original_expected, original_result, check_dtype=False)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class SamenessPerViewValidationChecker(BigQueryViewTestCase):
    """Tests for the SamenessPerViewValidationChecker error view builder queries."""

    def test_sameness_check_dates_same_values(self) -> None:

        mock_data = [
            [
                "test",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
            ]
        ]
        columns = ["label", "a", "b", "c"]
        mock_df = pd.DataFrame(mock_data, columns=columns)

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.String(),
                    "a": sqltypes.Date(),
                    "b": sqltypes.Date(),
                    "c": sqltypes.Date(),
                }
            ),
            mock_data=mock_df,
        )

        validation = SamenessDataValidationCheck(
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
        )

        error_result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": str, "a": str, "b": str, "c": str},
            ["label"],
        )

        original_result = self.query_view_for_builder(
            validation.view_builder,
            {"label": str, "a": str, "b": str, "c": str},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        error_result = error_result.drop(
            ["unique_count"],
            axis=1,
        )

        # TODO(https://github.com/pandas-dev/pandas/issues/40077): Remove the explicit
        #  index argument once this issue is resolved.
        error_expected = pd.DataFrame(
            [],
            columns=["label", "a", "b", "c"],
            dtype=str,
            index=pd.RangeIndex(start=0, stop=0, step=1),
        )
        assert_frame_equal(error_expected, error_result, check_dtype=False)

        original_expected = pd.DataFrame(mock_data, columns=columns)
        assert_frame_equal(original_expected, original_result, check_dtype=False)

    def test_sameness_check_dates_different_values(self) -> None:
        mock_data = [
            [
                "test",
                "2020-01-01",
                "2020-01-02",
                "2020-01-01",
            ],
            [
                "test2",
                "2020-01-03",
                "2020-01-03",
                "2020-01-03",
            ],
        ]
        columns = ["label", "a", "b", "c"]
        mock_df = pd.DataFrame(mock_data, columns=columns)

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.String(),
                    "a": sqltypes.Date(),
                    "b": sqltypes.Date(),
                    "c": sqltypes.Date(),
                }
            ),
            mock_data=mock_df,
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            partition_columns=["label"],
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            soft_max_allowed_error=0.02,
            hard_max_allowed_error=0.3,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
        )

        error_result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": str, "a": str, "b": str, "c": str},
            ["label"],
        )

        original_result = self.query_view_for_builder(
            validation.view_builder,
            {"label": str, "a": str, "b": str, "c": str},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        error_result = error_result.drop(
            ["unique_count"],
            axis=1,
        )

        error_expected = pd.DataFrame(
            [
                [
                    "test",
                    "2020-01-01",
                    "2020-01-02",
                    "2020-01-01",
                ]
            ],
            columns=["label", "a", "b", "c"],
            dtype=str,
        )
        assert_frame_equal(error_expected, error_result, check_dtype=False)

        original_expected = pd.DataFrame(mock_data, columns=columns)
        assert_frame_equal(original_expected, original_result, check_dtype=False)

    def test_sameness_check_empty_value(self) -> None:
        mock_data = [
            [
                "test",
                "2020-01-01",
                "",
                "2020-01-01",
            ],
            [
                "test2",
                "2020-01-03",
                "2020-01-03",
                "2020-01-03",
            ],
        ]
        columns = ["label", "a", "b", "c"]
        mock_df = pd.DataFrame(mock_data, columns=columns)

        self.create_mock_bq_table(
            dataset_id="my_dataset",
            table_id="test_data",
            mock_schema=PostgresTableSchema(
                {
                    "label": sqltypes.String(),
                    "a": sqltypes.String(),
                    "b": sqltypes.String(),
                    "c": sqltypes.String(),
                }
            ),
            mock_data=mock_df,
        )

        validation = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
            validation_type=ValidationCheckType.SAMENESS,
            comparison_columns=["a", "b", "c"],
            partition_columns=["label"],
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            soft_max_allowed_error=0.02,
            hard_max_allowed_error=0.3,
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from `{project_id}.my_dataset.test_data`",
            ),
        )

        error_result = self.query_view_for_builder(
            validation.error_view_builder,
            {"label": str, "a": str, "b": str, "c": str},
            ["label"],
        )

        original_result = self.query_view_for_builder(
            validation.view_builder,
            {"label": str, "a": str, "b": str, "c": str},
            ["label"],
        )

        # need to drop since Postgres doesn't properly remove columns with EXCEPT
        error_result = error_result.drop(
            ["unique_count"],
            axis=1,
        )

        error_expected = pd.DataFrame(
            [
                [
                    "test",
                    "2020-01-01",
                    "",
                    "2020-01-01",
                ]
            ],
            columns=["label", "a", "b", "c"],
            dtype=str,
        )
        assert_frame_equal(error_expected, error_result)

        original_expected = pd.DataFrame(mock_data, columns=columns)
        assert_frame_equal(original_expected, original_result)


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
