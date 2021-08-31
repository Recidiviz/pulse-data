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

"""Tests for validation/checks/existence_check.py."""

from unittest import TestCase

from mock import patch

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.validation.checks.existence_check import (
    ExistenceDataValidationCheck,
    ExistenceValidationChecker,
    ExistenceValidationResultDetails,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationCategory,
    ValidationCheckType,
    ValidationResultStatus,
)


class TestExistenceValidationChecker(TestCase):
    """Tests for the ExistenceValidationChecker."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "project-id"

        self.client_patcher = patch(
            "recidiviz.validation.checks.existence_check.BigQueryClientImpl"
        )
        self.mock_client = self.client_patcher.start().return_value

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def test_existence_check_no_failures(self) -> None:
        self.mock_client.run_query_async.return_value = []

        job = DataValidationJob(
            region_code="US_VA",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                validation_type=ValidationCheckType.EXISTENCE,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = ExistenceValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=ExistenceValidationResultDetails(
                    num_invalid_rows=0, hard_num_allowed_rows=0, soft_num_allowed_rows=0
                ),
            ),
        )

    def test_existence_check_failures(self) -> None:
        self.mock_client.run_query_async.return_value = [
            "some result row",
            "some other result row",
        ]

        job = DataValidationJob(
            region_code="US_VA",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                validation_type=ValidationCheckType.EXISTENCE,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
            ),
        )
        result = ExistenceValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=ExistenceValidationResultDetails(
                    num_invalid_rows=2, hard_num_allowed_rows=0, soft_num_allowed_rows=0
                ),
            ),
        )

    def test_existence_check_failures_below_threshold(self) -> None:
        self.mock_client.run_query_async.return_value = [
            "some result row",
            "some other result row",
        ]

        job = DataValidationJob(
            region_code="US_VA",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                validation_type=ValidationCheckType.EXISTENCE,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                hard_num_allowed_rows=2,
                soft_num_allowed_rows=2,
            ),
        )
        result = ExistenceValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=ExistenceValidationResultDetails(
                    num_invalid_rows=2, hard_num_allowed_rows=2, soft_num_allowed_rows=2
                ),
            ),
        )

    def test_existence_check_failures_between_soft_and_hard_threshold(self) -> None:
        self.mock_client.run_query_async.return_value = [
            "some result row",
            "some other result row",
        ]

        job = DataValidationJob(
            region_code="US_VA",
            validation=ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                validation_type=ValidationCheckType.EXISTENCE,
                view_builder=SimpleBigQueryViewBuilder(
                    dataset_id="my_dataset",
                    view_id="test_view",
                    description="test_view description",
                    view_query_template="select * from literally_anything",
                ),
                hard_num_allowed_rows=2,
                soft_num_allowed_rows=0,
            ),
        )
        result = ExistenceValidationChecker.run_check(job)

        self.assertEqual(
            result,
            DataValidationJobResult(
                validation_job=job,
                result_details=ExistenceValidationResultDetails(
                    num_invalid_rows=2, hard_num_allowed_rows=2, soft_num_allowed_rows=0
                ),
            ),
        )

    def test_existence_check_incorrect_max_errors_raises_error(self) -> None:
        self.mock_client.run_query_async.return_value = [
            "some result row",
            "some other result row",
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"^soft_num_allowed_rows cannot be greater than hard_num_allowed_rows\. Found instead: 10 vs\. 2$",
        ):
            _ = DataValidationJob(
                region_code="US_VA",
                validation=ExistenceDataValidationCheck(
                    validation_category=ValidationCategory.INVARIANT,
                    validation_type=ValidationCheckType.EXISTENCE,
                    view_builder=SimpleBigQueryViewBuilder(
                        dataset_id="my_dataset",
                        view_id="test_view",
                        description="test_view description",
                        view_query_template="select * from literally_anything",
                    ),
                    hard_num_allowed_rows=2,
                    soft_num_allowed_rows=10,
                ),
            )


class TestExistenceValidationResultDetails(TestCase):
    """Tests for ExistenceValidationResultDetails."""

    def test_success(self) -> None:
        result = ExistenceValidationResultDetails(
            num_invalid_rows=0, hard_num_allowed_rows=0, soft_num_allowed_rows=0
        )

        self.assertEqual(
            ValidationResultStatus.SUCCESS, result.validation_result_status()
        )
        self.assertIsNone(result.failure_description())

    def test_success_some_invalid(self) -> None:
        result = ExistenceValidationResultDetails(
            num_invalid_rows=2, hard_num_allowed_rows=4, soft_num_allowed_rows=2
        )

        self.assertEqual(
            ValidationResultStatus.SUCCESS, result.validation_result_status()
        )
        self.assertIsNone(result.failure_description())

    def test_failure(self) -> None:
        result = ExistenceValidationResultDetails(
            num_invalid_rows=2, hard_num_allowed_rows=0, soft_num_allowed_rows=0
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_HARD, result.validation_result_status()
        )
        self.assertEqual(
            "Found [2] invalid rows, more than the allowed [0] (hard)",
            result.failure_description(),
        )

    def test_soft_failure(self) -> None:
        result = ExistenceValidationResultDetails(
            num_invalid_rows=2, hard_num_allowed_rows=2, soft_num_allowed_rows=0
        )

        self.assertEqual(
            ValidationResultStatus.FAIL_SOFT, result.validation_result_status()
        )
        self.assertEqual(
            "Found [2] invalid rows, more than the allowed [0] (soft)",
            result.failure_description(),
        )
