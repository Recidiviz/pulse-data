# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for utils/execution_utils.py."""
# pylint: disable=protected-access
import argparse
import datetime
import random
import string
import unittest
from typing import Any, Dict, Iterable

from apache_beam.options.pipeline_options import PipelineOptions
from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils import execution_utils
from recidiviz.calculator.pipeline.utils.execution_utils import (
    extract_county_of_residence_from_rows,
    person_and_kwargs_for_identifier,
    select_all_by_person_query,
    select_query,
)
from recidiviz.persistence.entity.state.entities import StateAssessment, StatePerson


class TestGetJobID(unittest.TestCase):
    """Tests the function that gets the job id for a given pipeline."""

    def test_get_job_id_local_job(self) -> None:
        """Tests getting the job_id from given pipeline options."""

        args = ["--runner", "DirectRunner"]

        pipeline_options = PipelineOptions(args).get_all_options()

        job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        pipeline_options["job_timestamp"] = job_timestamp

        job_id = execution_utils.get_job_id(pipeline_options)

        assert job_id == job_timestamp + "_local_job"

    def test_get_job_id_missing_timestamp_local_job(self) -> None:
        """Tests getting the job_id when a job_timestamp is not provided for
        a locally running job.
        """

        args = ["--runner", "DirectRunner"]

        pipeline_options = PipelineOptions(args).get_all_options()

        with self.assertRaisesRegex(
            ValueError, "^Must provide a job_timestamp for local jobs.$"
        ):
            _ = execution_utils.get_job_id(pipeline_options)

    def test_get_job_id_no_project_invalid(self) -> None:
        """Tests getting the job_id when there is no provided project."""

        job_name = "".join(random.choice(string.ascii_lowercase) for _ in range(10))

        args = ["--runner", "DataflowRunner", "--job_name", job_name, "--region", "xxx"]

        pipeline_options = PipelineOptions(args).get_all_options()

        with self.assertRaisesRegex(
            ValueError, "No project provided in pipeline options:"
        ):
            _ = execution_utils.get_job_id(pipeline_options)

    def test_get_job_id_region_is_none(self) -> None:
        """Tests getting the job_id when there is no region provided.

        Note: here we are setting region as None instead of omitting it because
        PipelineOptions has a default value for the region.
        """

        args = ["--runner", "DataflowRunner", "--project", "xxx", "--region", None]

        pipeline_options = PipelineOptions(args).get_all_options()

        with self.assertRaisesRegex(
            ValueError, "No region provided in pipeline options:"
        ):
            _ = execution_utils.get_job_id(pipeline_options)

    def test_get_job_id_no_job_name_provided(self) -> None:
        """Tests getting the job_id when there is no job_name provided."""

        args = ["--runner", "DataflowRunner", "--project", "xxx", "--region", "uswest1"]

        pipeline_options = PipelineOptions(args).get_all_options()

        with self.assertRaisesRegex(
            ValueError, "No job_name provided in pipeline options:"
        ):
            _ = execution_utils.get_job_id(pipeline_options)


class TestCalculationEndMonthArg(unittest.TestCase):
    """Tests the calculation_end_month function."""

    def test_calculation_end_month_arg(self) -> None:
        value = "2009-01"

        return_value = execution_utils.calculation_end_month_arg(value)

        self.assertEqual(return_value, value)

    def test_calculation_end_month_arg_bad_month(self) -> None:
        value = "2009-31"

        with self.assertRaisesRegex(
            argparse.ArgumentTypeError,
            "calculation_end_month parameter must be in the format YYYY-MM.",
        ):
            _ = execution_utils.calculation_end_month_arg(value)

    def test_calculation_end_month_arg_bad_year(self) -> None:
        value = "001-03"

        with self.assertRaisesRegex(
            argparse.ArgumentTypeError,
            "calculation_end_month parameter must be in the format YYYY-MM.",
        ):
            _ = execution_utils.calculation_end_month_arg(value)

    @freeze_time("2019-11-01")
    def test_calculation_end_month_arg_after_this_month(self) -> None:
        value = "2030-01"

        with self.assertRaisesRegex(
            argparse.ArgumentTypeError,
            "calculation_end_month parameter cannot be a month in the future.",
        ):
            _ = execution_utils.calculation_end_month_arg(value)

    def test_calculation_end_month_arg_only_year(self) -> None:
        value = "2009"

        with self.assertRaisesRegex(
            argparse.ArgumentTypeError,
            "calculation_end_month parameter must be in the format YYYY-MM.",
        ):
            _ = execution_utils.calculation_end_month_arg(value)


class TestPersonAndKwargsForIdentifier(unittest.TestCase):
    """Tests the person_and_kwargs_for_identifier function."""

    def test_person_and_kwargs_for_identifier(self) -> None:
        person_input = StatePerson.new_with_defaults(state_code="US_XX", person_id=123)

        assessment = StateAssessment.new_with_defaults(state_code="US_XX")

        arg_to_entities_map: Dict[str, Iterable[Any]] = {
            StatePerson.__name__: iter([person_input]),
            StateAssessment.__name__: iter([assessment]),
        }

        person, kwargs = person_and_kwargs_for_identifier(arg_to_entities_map)

        expected_kwargs = {StateAssessment.__name__: [assessment]}

        self.assertEqual(person, person_input)
        self.assertEqual(expected_kwargs, kwargs)

    def test_person_and_kwargs_for_identifier_two_people_same_id(self) -> None:
        person_input_1 = StatePerson.new_with_defaults(
            state_code="US_XX", person_id=123
        )

        person_input_2 = StatePerson.new_with_defaults(
            state_code="US_XX", person_id=123
        )

        assessment = StateAssessment.new_with_defaults(state_code="US_XX")

        arg_to_entities_map: Dict[str, Iterable[Any]] = {
            # There should never be two StatePerson entities with the same person_id. This should fail loudly.
            StatePerson.__name__: iter([person_input_1, person_input_2]),
            StateAssessment.__name__: iter([assessment]),
        }

        with self.assertRaises(ValueError):
            _ = person_and_kwargs_for_identifier(arg_to_entities_map)

    def test_person_and_kwargs_for_identifier_no_person(self) -> None:
        assessment = StateAssessment.new_with_defaults(state_code="US_XX")

        arg_to_entities_map: Dict[str, Iterable[Any]] = {
            # There should never be two StatePerson entities with the same person_id. This should fail loudly.
            StateAssessment.__name__: iter([assessment])
        }

        with self.assertRaises(ValueError):
            _ = person_and_kwargs_for_identifier(arg_to_entities_map)


class TestSelectAllQuery(unittest.TestCase):
    """Tests for the select_all_by_person_query AND select_query functions."""

    def setUp(self) -> None:
        self.dataset = "project-id.my_dataset"
        self.table_id = "TABLE_WHERE_DATA_IS"

    def test_select_all_with_state_code_filter_only(self) -> None:
        expected_query = "SELECT * FROM `project-id.my_dataset.TABLE_WHERE_DATA_IS` WHERE state_code IN ('US_XX')"

        self.assertEqual(
            expected_query,
            select_all_by_person_query(
                self.dataset,
                self.table_id,
                state_code_filter="US_XX",
                person_id_filter_set=None,
            ),
        )

        self.assertEqual(
            expected_query,
            select_query(
                self.dataset,
                self.table_id,
                state_code_filter="US_XX",
                unifying_id_field="field_name",
                unifying_id_field_filter_set=None,
            ),
        )

    def test_select_all_state_code_and_ids_filter(self) -> None:
        expected_query = (
            "SELECT * FROM `project-id.my_dataset.TABLE_WHERE_DATA_IS` "
            "WHERE state_code IN ('US_XX') AND person_id IN (1234)"
        )

        self.assertEqual(
            expected_query,
            select_all_by_person_query(
                self.dataset,
                self.table_id,
                state_code_filter="US_XX",
                person_id_filter_set={1234},
            ),
        )

        expected_query = (
            "SELECT * FROM `project-id.my_dataset.TABLE_WHERE_DATA_IS` "
            "WHERE state_code IN ('US_XX') AND field_name IN (1234, 56)"
        )
        self.assertEqual(
            expected_query,
            select_query(
                self.dataset,
                self.table_id,
                state_code_filter="US_XX",
                unifying_id_field="field_name",
                unifying_id_field_filter_set={1234, 56},
            ),
        )


class TestExtractCountyOfResidenceFromRows(unittest.TestCase):
    """Tests for extract_county_of_residence_from_rows in execution_utils.py."""

    def test_no_rows(self) -> None:
        county_of_residence = extract_county_of_residence_from_rows([])
        self.assertIsNone(county_of_residence)

    def test_single_row(self) -> None:
        expected_county_of_residence = "county"
        rows = [
            {
                "state_code": "US_XX",
                "person_id": 123,
                "county_of_residence": expected_county_of_residence,
            }
        ]

        county_of_residence = extract_county_of_residence_from_rows(rows)
        self.assertEqual(expected_county_of_residence, county_of_residence)

    def test_multiple_rows_asserts(self) -> None:
        rows = [
            {
                "state_code": "US_XX",
                "person_id": 123,
                "county_of_residence": "county_1",
            },
            {
                "state_code": "US_XX",
                "person_id": 123,
                "county_of_residence": "county_2",
            },
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"^Found more than one county of residence for person with id \[123\]",
        ):
            _ = extract_county_of_residence_from_rows(rows)
