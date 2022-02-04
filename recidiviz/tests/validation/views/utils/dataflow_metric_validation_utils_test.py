# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the functions in the dataflow_metric_validation_utils file."""
import unittest
from typing import List, Type

from recidiviz.calculator.pipeline.metrics.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)
from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.validation.views.utils.dataflow_metric_validation_utils import (
    _metrics_with_enum_field_of_type,
    _validate_metric_has_all_fields,
    validation_query_for_metric,
    validation_query_for_metric_views_with_invalid_enums,
)


class TestDataflowMetricValidationUtils(unittest.TestCase):
    """Tests the functions in dataflow_metric_validation_utils.py"""

    def test_metrics_with_enum_field_of_type(self) -> None:
        enum_field_name = "admission_reason"
        enum_field_type = StateIncarcerationPeriodAdmissionReason

        metrics = _metrics_with_enum_field_of_type(
            enum_field_name=enum_field_name, enum_field_type=enum_field_type
        )

        expected_metrics = [
            IncarcerationAdmissionMetric,
            IncarcerationCommitmentFromSupervisionMetric,
            IncarcerationPopulationMetric,
            IncarcerationReleaseMetric,
        ]

        self.assertEqual(expected_metrics, metrics)

    def test_metrics_with_enum_field_of_type_none(self) -> None:
        enum_field_name = "invalid_enum_name"
        enum_field_type = StateIncarcerationPeriodAdmissionReason

        metrics = _metrics_with_enum_field_of_type(
            enum_field_name=enum_field_name, enum_field_type=enum_field_type
        )

        expected_metrics: List[Type[RecidivizMetric]] = []

        self.assertEqual(expected_metrics, metrics)

    def test_validate_metric_has_all_fields(self) -> None:
        metric = IncarcerationPopulationMetric
        fields = ["date_of_stay", "facility"]

        # Assert no error
        _validate_metric_has_all_fields(metric=metric, fields_to_validate=fields)

    def test_validate_metric_has_all_fields_invalid(self) -> None:
        metric = IncarcerationPopulationMetric
        fields = ["date_of_stay", "XXXXX"]

        with self.assertRaises(ValueError):
            _validate_metric_has_all_fields(metric=metric, fields_to_validate=fields)

    def test_validation_query_for_metric_views_with_invalid_enums(self) -> None:
        enum_field_name = "release_reason"
        enum_field_type = StateIncarcerationPeriodReleaseReason
        invalid_rows_filter_clause = "WHERE release_reason = 'BAD'"

        # Assert no error
        validation_query_for_metric_views_with_invalid_enums(
            enum_field_name=enum_field_name,
            enum_field_type=enum_field_type,
            additional_columns_to_select=["person_id"],
            invalid_rows_filter_clause=invalid_rows_filter_clause,
            validation_description="DESCRIPTION",
        )

    def test_validation_query_for_metric_views_with_invalid_enums_no_metrics(
        self,
    ) -> None:
        enum_field_name = "bad_field_name"
        enum_field_type = StateIncarcerationPeriodReleaseReason
        invalid_rows_filter_clause = "WHERE release_reason = 'BAD'"

        expected_error = (
            "No metric classes with the field bad_field_name that store "
            "the enum StateIncarcerationPeriodReleaseReason."
        )

        with self.assertRaises(ValueError) as e:
            validation_query_for_metric_views_with_invalid_enums(
                enum_field_name=enum_field_name,
                enum_field_type=enum_field_type,
                additional_columns_to_select=["person_id"],
                invalid_rows_filter_clause=invalid_rows_filter_clause,
                validation_description="DESCRIPTION",
            )

        self.assertEqual(expected_error, e.exception.args[0])

    def test_validation_query_for_metric_views_with_invalid_enums_bad_filter(
        self,
    ) -> None:
        enum_field_name = "release_reason"
        enum_field_type = StateIncarcerationPeriodReleaseReason
        invalid_rows_filter_clause = "HAVING release_reason = 'BAD'"

        expected_error = (
            "Invalid filter clause. Must start with 'WHERE'. "
            f"Found: {invalid_rows_filter_clause}."
        )

        with self.assertRaises(ValueError) as e:
            validation_query_for_metric_views_with_invalid_enums(
                enum_field_name=enum_field_name,
                enum_field_type=enum_field_type,
                additional_columns_to_select=["person_id"],
                invalid_rows_filter_clause=invalid_rows_filter_clause,
                validation_description="DESCRIPTION",
            )

        self.assertEqual(expected_error, e.exception.args[0])

    def test_validation_query_for_metric_views_with_invalid_enums_bad_additional_cols(
        self,
    ) -> None:
        enum_field_name = "release_reason"
        enum_field_type = StateIncarcerationPeriodReleaseReason
        invalid_rows_filter_clause = "WHERE release_reason = 'BAD'"
        additional_columns_to_select = ["XXX"]

        expected_error = (
            "The IncarcerationReleaseMetric does not contain metric field: XXX."
        )

        with self.assertRaises(ValueError) as e:
            validation_query_for_metric_views_with_invalid_enums(
                enum_field_name=enum_field_name,
                enum_field_type=enum_field_type,
                additional_columns_to_select=additional_columns_to_select,
                invalid_rows_filter_clause=invalid_rows_filter_clause,
                validation_description="DESCRIPTION",
            )

        self.assertEqual(expected_error, e.exception.args[0])

    def test_validation_query_for_metric(self) -> None:
        invalid_rows_filter_clause = "WHERE release_reason = 'BAD'"

        # Assert no error
        validation_query_for_metric(
            metric=IncarcerationReleaseMetric,
            additional_columns_to_select=["person_id"],
            invalid_rows_filter_clause=invalid_rows_filter_clause,
            validation_description="DESCRIPTION",
        )

    def test_validation_query_for_metric_bad_filter(self) -> None:
        invalid_rows_filter_clause = "HAVING release_reason = 'BAD'"

        expected_error = (
            "Invalid filter clause. Must start with 'WHERE'. "
            f"Found: {invalid_rows_filter_clause}."
        )

        with self.assertRaises(ValueError) as e:
            validation_query_for_metric(
                metric=IncarcerationReleaseMetric,
                additional_columns_to_select=["person_id"],
                invalid_rows_filter_clause=invalid_rows_filter_clause,
                validation_description="DESCRIPTION",
            )

        self.assertEqual(expected_error, e.exception.args[0])

    def test_validation_query_for_metric_bad_additional_col(self) -> None:
        additional_columns_to_select = ["XXX"]
        invalid_rows_filter_clause = "WHERE release_reason = 'BAD'"

        expected_error = (
            "The IncarcerationReleaseMetric does not contain metric field: XXX."
        )

        with self.assertRaises(ValueError) as e:
            validation_query_for_metric(
                metric=IncarcerationReleaseMetric,
                additional_columns_to_select=additional_columns_to_select,
                invalid_rows_filter_clause=invalid_rows_filter_clause,
                validation_description="DESCRIPTION",
            )

        self.assertEqual(expected_error, e.exception.args[0])
