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

# pylint: disable=unused-import,wrong-import-order
# pylint: disable=protected-access


"""Tests for utils/execution_utils.py."""
import argparse
import unittest
import datetime
import pytest

import random
import string

from apache_beam.options.pipeline_options import PipelineOptions
from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils import execution_utils
from recidiviz.calculator.pipeline.utils.execution_utils import person_and_kwargs_for_identifier
from recidiviz.persistence.entity.state.entities import StatePerson, StateAssessment


class TestGetJobID(unittest.TestCase):
    """Tests the function that gets the job id for a given pipeline."""
    def test_get_job_id_local_job(self):
        """Tests getting the job_id from given pipeline options."""

        args = ['--runner', 'DirectRunner']

        pipeline_options = PipelineOptions(args).get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        pipeline_options['job_timestamp'] = job_timestamp

        job_id = execution_utils.get_job_id(pipeline_options)

        assert job_id == job_timestamp + '_local_job'

    def test_get_job_id_missing_timestamp_local_job(self):
        """Tests getting the job_id when a job_timestamp is not provided for
        a locally running job.
        """

        args = ['--runner', 'DirectRunner']

        pipeline_options = PipelineOptions(args).get_all_options()

        with pytest.raises(ValueError) as e:

            _ = execution_utils.get_job_id(pipeline_options)

        assert str(e.value) == "Must provide a job_timestamp for local jobs."

    def test_get_job_id_no_project_invalid(self):
        """Tests getting the job_id when there is no provided project."""

        job_name = ''.join(random.choice(string.ascii_lowercase)
                           for _ in range(10))

        args = ['--runner', 'DataflowRunner', '--job_name', job_name,
                '--region', 'xxx']

        pipeline_options = PipelineOptions(args).get_all_options()

        with pytest.raises(ValueError) as e:

            _ = execution_utils.get_job_id(pipeline_options)

        assert "No project provided in pipeline options:" in str(e.value)

    def test_get_job_id_region_is_none(self):
        """Tests getting the job_id when there is no region provided.

        Note: here we are setting region as None instead of omitting it because
        PipelineOptions has a default value for the region.
        """

        args = ['--runner', 'DataflowRunner', '--project', 'xxx', '--region',
                None]

        pipeline_options = PipelineOptions(args).get_all_options()

        with pytest.raises(ValueError) as e:

            _ = execution_utils.get_job_id(pipeline_options)

        assert "No region provided in pipeline options:" in str(e.value)

    def test_get_job_id_no_job_name_provided(self):
        """Tests getting the job_id when there is no job_name provided."""

        args = ['--runner', 'DataflowRunner', '--project', 'xxx']

        pipeline_options = PipelineOptions(args).get_all_options()

        with pytest.raises(ValueError) as e:

            _ = execution_utils.get_job_id(pipeline_options)

        assert "No job_name provided in pipeline options:" in str(e.value)


class TestCalculationEndMonthArg(unittest.TestCase):
    """Tests the calculation_end_month function."""
    def test_calculation_end_month_arg(self):
        value = '2009-01'

        return_value = execution_utils.calculation_end_month_arg(value)

        self.assertEqual(return_value, value)

    def test_calculation_end_month_arg_bad_month(self):
        value = '2009-31'

        with pytest.raises(argparse.ArgumentTypeError) as e:
            _ = execution_utils.calculation_end_month_arg(value)

        assert "calculation_end_month parameter must be in the format YYYY-MM." in str(e.value)

    def test_calculation_end_month_arg_bad_year(self):
        value = '001-03'

        with pytest.raises(argparse.ArgumentTypeError) as e:
            _ = execution_utils.calculation_end_month_arg(value)

        assert "calculation_end_month parameter must be in the format YYYY-MM." in str(e.value)

    @freeze_time('2019-11-01')
    def test_calculation_end_month_arg_after_this_month(self):
        value = '2030-01'

        with pytest.raises(argparse.ArgumentTypeError) as e:
            _ = execution_utils.calculation_end_month_arg(value)

        assert "calculation_end_month parameter cannot be a month in the future." in str(e.value)

    def test_calculation_end_month_arg_only_year(self):
        value = '2009'

        with pytest.raises(argparse.ArgumentTypeError) as e:
            _ = execution_utils.calculation_end_month_arg(value)

        assert "calculation_end_month parameter must be in the format YYYY-MM." in str(e.value)


class TestPersonAndKwargsForIdentifier(unittest.TestCase):
    """Tests the person_and_kwargs_for_identifier function."""
    def test_person_and_kwargs_for_identifier(self):
        person_input = StatePerson.new_with_defaults(
            person_id=123
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX'
        )

        arg_to_entities_map = {
            'person': iter([person_input]),
            'assessments': iter([assessment])
        }

        person, kwargs = person_and_kwargs_for_identifier(arg_to_entities_map)

        expected_kwargs = {
            'assessments': [assessment]
        }

        self.assertEqual(person, person_input)
        self.assertEqual(expected_kwargs, kwargs)

    def test_person_and_kwargs_for_identifier_two_people_same_id(self):
        person_input_1 = StatePerson.new_with_defaults(
            person_id=123
        )

        person_input_2 = StatePerson.new_with_defaults(
            person_id=123
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX'
        )

        arg_to_entities_map = {
            # There should never be two StatePerson entities with the same person_id. This should fail loudly.
            'person': iter([person_input_1, person_input_2]),
            'assessments': iter([assessment])
        }

        with pytest.raises(ValueError):
            _ = person_and_kwargs_for_identifier(arg_to_entities_map)

    def test_person_and_kwargs_for_identifier_no_person(self):
        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX'
        )

        arg_to_entities_map = {
            # There should never be two StatePerson entities with the same person_id. This should fail loudly.
            'assessments': iter([assessment])
        }

        with pytest.raises(ValueError):
            _ = person_and_kwargs_for_identifier(arg_to_entities_map)
