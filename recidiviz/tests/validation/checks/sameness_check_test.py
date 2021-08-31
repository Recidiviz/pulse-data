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

from mock import patch

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.validation.checks.sameness_check import (
    ResultRow,
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
    SamenessPerRowValidationResultDetails,
    SamenessPerViewValidationResultDetails,
    SamenessValidationChecker,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationCategory,
    ValidationCheckType,
    ValidationResultStatus,
)


class TestSamenessValidationChecker(TestCase):
    """Tests for the SamenessValidationChecker."""

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
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            )

    def test_sameness_check_bad_max_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Allowed error value must be between 0\.0 and 1\.0\. Found instead: \[1\.5\]$",
        ):
            _ = SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                comparison_columns=["a", "b", "c"],
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                hard_max_allowed_error=1.5,
                soft_max_allowed_error=1.5,
            )

    def test_sameness_check_validation_name(self) -> None:
        check = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=["a", "b", "c"],
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from literally_anything",
            ),
        )
        self.assertEqual(check.validation_name, "test_view")

        check_with_name_suffix = SamenessDataValidationCheck(
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            validation_type=ValidationCheckType.SAMENESS,
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            validation_name_suffix="b_c_only",
            comparison_columns=["b", "c"],
            view_builder=SimpleBigQueryViewBuilder(
                dataset_id="my_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="select * from literally_anything",
            ),
        )
        self.assertEqual(check_with_name_suffix.validation_name, "test_view_b_c_only")

    def test_sameness_check_numbers_same_values(self) -> None:
        self.mock_client.run_query_async.return_value = [{"a": 10, "b": 10, "c": 10}]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[],
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                ),
            ),
        )

    def test_sameness_check_numbers_different_values_no_allowed_error(self) -> None:
        self.mock_client.run_query_async.return_value = [{"a": 98, "b": 100, "c": 99}]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                soft_max_allowed_error=0.0,
                hard_max_allowed_error=0.0,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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

    def test_sameness_check_numbers_different_values_within_margin(self) -> None:
        self.mock_client.run_query_async.return_value = [{"a": 98, "b": 100, "c": 99}]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                hard_max_allowed_error=0.02,
                soft_max_allowed_error=0.02,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[],
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                ),
            ),
        )

    def test_sameness_check_numbers_different_values_above_margin(self) -> None:
        self.mock_client.run_query_async.return_value = [{"a": 97, "b": 100, "c": 99}]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                hard_max_allowed_error=0.02,
                soft_max_allowed_error=0.02,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[
                        (
                            ResultRow(label_values=(), comparison_values=(97, 100, 99)),
                            0.03,
                        )
                    ],
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                ),
            ),
        )

    def test_sameness_check_numbers_multiple_rows_above_margin(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"a": 97, "b": 100, "c": 99},
            {"a": 14, "b": 21, "c": 14},
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                hard_max_allowed_error=0.02,
                soft_max_allowed_error=0.02,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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

    def test_sameness_check_numbers_multiple_rows_and_some_above_soft_margin(
        self,
    ) -> None:
        self.mock_client.run_query_async.return_value = [
            {"a": 97, "b": 100, "c": 99},
            {"a": 14, "b": 21, "c": 14},
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                hard_max_allowed_error=0.5,
                soft_max_allowed_error=0.03,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[
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

    def test_sameness_check_different_max_error_values(self) -> None:
        self.mock_client.run_query_async.return_value = [{"a": 10, "b": 10, "c": 10}]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                soft_max_allowed_error=0.0,
                hard_max_allowed_error=0.5,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerRowValidationResultDetails(
                    failed_rows=[],
                    hard_max_allowed_error=0.5,
                    soft_max_allowed_error=0.0,
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
                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                    view_builder=SimpleBigQueryViewBuilder(
                        dataset_id="my_dataset",
                        view_id="test_view",
                        description="test_view description",
                        view_query_template="select * from literally_anything",
                    ),
                ),
            )

    def test_sameness_check_strings_same_values(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": "10", "b": "10", "c": "10"}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=0,
                    total_num_rows=1,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 1, "b": 1, "c": 1}),
                    ],
                ),
            ),
        )

    def test_sameness_check_strings_values_all_none(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": None, "b": None, "c": None}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=0,
                    total_num_rows=1,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 0, "b": 0, "c": 0})
                    ],
                ),
            ),
        )

    def test_sameness_check_dates_same_values(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {
                "p": "test",
                "a": date(2020, 1, 1),
                "b": date(2020, 1, 1),
                "c": date(2020, 1, 1),
            }
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.DATES,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=0,
                    total_num_rows=1,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 1, "b": 1, "c": 1}),
                    ],
                ),
            ),
        )

    def test_sameness_check_dates_values_all_none(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": None, "b": None, "c": None}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.DATES,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=SamenessPerViewValidationResultDetails(
                    num_error_rows=0,
                    total_num_rows=1,
                    hard_max_allowed_error=0.02,
                    soft_max_allowed_error=0.02,
                    non_null_counts_per_column_per_partition=[
                        (("test",), {"a": 0, "b": 0, "c": 0})
                    ],
                ),
            ),
        )

    def test_sameness_check_numbers_values_all_none(self) -> None:
        self.mock_client.run_query_async.return_value = [
            {"a": None, "b": None, "c": None}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Unexpected None value for column \[a\] in validation \[test_view\]\.$",
        ):
            _ = SamenessValidationChecker.run_check(job)

    def test_sameness_check_numbers_one_none(self) -> None:
        self.mock_client.run_query_async.return_value = [{"a": 3, "b": 3, "c": None}]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Unexpected None value for column \[c\] in validation \[test_view\]\.$",
        ):
            _ = SamenessValidationChecker.run_check(job)

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
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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
                sameness_check_type=SamenessDataValidationCheckType.DATES,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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
                sameness_check_type=SamenessDataValidationCheckType.DATES,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)
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

    def test_sameness_check_strings_different_values_handle_non_string_type(
        self,
    ) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": "same", "b": "same", "c": 1245}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^Unexpected type \[<class 'int'>\] for value \[1245\] in STRINGS validation \[test_view\]\.$",
        ):
            _ = SamenessValidationChecker.run_check(job)

    def test_sameness_check_dates_different_values_handle_non_date_type(
        self,
    ) -> None:
        self.mock_client.run_query_async.return_value = [
            {"p": "test", "a": date(2020, 1, 1), "b": date(2020, 1, 1), "c": 1245}
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.DATES,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^Unexpected type \[<class 'int'>\] for value \[1245\] in DATES validation \[test_view\]\.$",
        ):
            _ = SamenessValidationChecker.run_check(job)

    def test_sameness_check_strings_different_values_within_margin(self) -> None:
        num_bad_rows = 2
        hard_max_allowed_error = num_bad_rows / 100

        self.mock_client.run_query_async.return_value = (
            self.return_string_values_with_num_bad_rows(num_bad_rows)
        )

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                hard_max_allowed_error=hard_max_allowed_error,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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

    def test_sameness_check_dates_different_values_within_margin(self) -> None:
        num_bad_rows = 2
        hard_max_allowed_error = num_bad_rows / 100

        self.mock_client.run_query_async.return_value = (
            self.return_date_values_with_num_bad_rows(num_bad_rows)
        )

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.DATES,
                hard_max_allowed_error=hard_max_allowed_error,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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

    def test_sameness_check_strings_different_values_above_margin(self) -> None:
        num_bad_rows = 5
        max_allowed_error = (num_bad_rows - 1) / 100  # Below the number of bad rows

        self.mock_client.run_query_async.return_value = (
            self.return_string_values_with_num_bad_rows(num_bad_rows)
        )
        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
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
        result = SamenessValidationChecker.run_check(job)

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

    def test_sameness_check_dates_different_values_above_margin(self) -> None:
        num_bad_rows = 5
        max_allowed_error = (num_bad_rows - 1) / 100  # Below the number of bad rows

        self.mock_client.run_query_async.return_value = (
            self.return_date_values_with_num_bad_rows(num_bad_rows)
        )
        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                partition_columns=["p"],
                sameness_check_type=SamenessDataValidationCheckType.DATES,
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
        result = SamenessValidationChecker.run_check(job)

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

    def test_sameness_check_strings_multiple_dates(self) -> None:
        self.mock_client.run_query_async.return_value = [
            # January 2021
            {"region": "US_XX", "date": "2021-01-31", "a": "00", "b": "00"},
            {"region": "US_XX", "date": "2021-01-31", "a": "01", "b": None},
            {"region": "US_XX", "date": "2021-01-31", "a": "02", "b": "02"},
            {"region": "US_XX", "date": "2021-01-31", "a": None, "b": "03"},
            # December 2020
            {"region": "US_XX", "date": "2020-12-31", "a": "00", "b": "00"},
            {"region": "US_XX", "date": "2020-12-31", "a": "02", "b": "02"},
            {"region": "US_XX", "date": "2020-12-31", "a": None, "b": "04"},
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b"],
                partition_columns=["region", "date"],
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
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
        result = SamenessValidationChecker.run_check(job)

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

    def test_sameness_check_dates_multiple_dates(self) -> None:
        self.mock_client.run_query_async.return_value = [
            # January 2021
            {
                "region": "US_XX",
                "date": "2021-01-31",
                "a": date(2020, 1, 1),
                "b": date(2020, 1, 1),
            },
            {"region": "US_XX", "date": "2021-01-31", "a": date(2020, 1, 2), "b": None},
            {
                "region": "US_XX",
                "date": "2021-01-31",
                "a": date(2020, 1, 3),
                "b": date(2020, 1, 3),
            },
            {"region": "US_XX", "date": "2021-01-31", "a": None, "b": date(2020, 1, 4)},
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
            {"region": "US_XX", "date": "2020-12-31", "a": None, "b": date(2020, 1, 5)},
        ]

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b"],
                partition_columns=["region", "date"],
                sameness_check_type=SamenessDataValidationCheckType.DATES,
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
        result = SamenessValidationChecker.run_check(job)

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

        self.mock_client.run_query_async.return_value = (
            self.return_string_values_with_num_bad_rows(num_bad_rows)
        )

        job = DataValidationJob(
            region_code="US_XX",
            validation=SamenessDataValidationCheck(
                validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
                validation_type=ValidationCheckType.SAMENESS,
                comparison_columns=["a", "b", "c"],
                sameness_check_type=SamenessDataValidationCheckType.STRINGS,
                hard_max_allowed_error=hard_max_allowed_error,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = SamenessValidationChecker.run_check(job)

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
