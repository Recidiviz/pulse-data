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

import unittest
import datetime
import pytest

import random
import string

from apache_beam.options.pipeline_options import PipelineOptions


from recidiviz.calculator.pipeline.utils import execution_utils


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
