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
"""Unittests for helpers in pipeline_args_utils.py."""
import unittest
from argparse import Namespace

from recidiviz.calculator.pipeline.incarceration import pipeline as incarceration_pipeline
from recidiviz.calculator.pipeline.utils.pipeline_args_utils import get_apache_beam_pipeline_options_from_args


class TestPipelineArgsUtils(unittest.TestCase):
    """Unittests for helpers in pipeline_args_utils.py."""

    DEFAULT_INCARCERATION_PIPELINE_ARGS =   \
        Namespace(calculation_month_limit=1, include_age=True, include_ethnicity=True, include_gender=True,
                  include_race=True, input='state', methodology='BOTH', output='dataflow_metrics',
                  person_filter_ids=None, reference_input='dashboard_views', state_code=None)

    DEFAULT_APACHE_BEAM_OPTIONS_DICT = {
        'runner': 'DataflowRunner',
        'project': 'recidiviz-staging',
        'job_name': 'incarceration-args-test',
        'staging_location': 'gs://recidiviz-staging-dataflow-templates/staging/',
        'temp_location': 'gs://recidiviz-staging-dataflow-templates/temp/',
        'region': 'us-west1', 'machine_type': 'n1-standard-4',
        'network': 'default',
        'subnetwork':
            'https://www.googleapis.com/compute/v1/projects/recidiviz-staging/regions/us-west1/subnetworks/default',
        'use_public_ips': False,
        'experiments': ['shuffle_mode=service'],
        'setup_file': './setup.py'
    }

    def test_minimal_incarceration_pipeline_args(self):
        # Arrange
        argv = ['--job_name', 'incarceration-args-test',
                '--project', 'recidiviz-staging']

        # Act
        incarceration_pipeline_args, apache_beam_args = incarceration_pipeline.parse_arguments(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert
        self.assertEqual(incarceration_pipeline_args, self.DEFAULT_INCARCERATION_PIPELINE_ARGS)
        self.assertEqual(pipeline_options.get_all_options(drop_default=True), self.DEFAULT_APACHE_BEAM_OPTIONS_DICT)

    def test_minimal_incarceration_pipeline_args_save_to_template(self):
        # Arrange
        argv = ['--job_name', 'incarceration-args-test',
                '--project', 'recidiviz-staging',
                '--save_as_template']
        # Act
        incarceration_pipeline_args, apache_beam_args = incarceration_pipeline.parse_arguments(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert
        self.assertEqual(incarceration_pipeline_args, self.DEFAULT_INCARCERATION_PIPELINE_ARGS)

        expected_apache_beam_options_dict = self.DEFAULT_APACHE_BEAM_OPTIONS_DICT.copy()
        expected_apache_beam_options_dict['template_location'] = \
            'gs://recidiviz-staging-dataflow-templates/templates/incarceration-args-test'

        self.assertEqual(pipeline_options.get_all_options(drop_default=True), expected_apache_beam_options_dict)

    def test_incarceration_pipeline_args_defaults_changed(self):
        # Arrange
        argv = [
            '--job_name', 'incarceration-args-test',
            '--runner', 'DirectRunner',
            '--project', 'recidiviz-staging',
            '--setup_file', './setup2.py',
            '--bucket', 'recidiviz-123-my-bucket',
            '--region=us-central1',
            '--input', 'county',
            '--reference_input', 'dashboard_views_2',
            '--output', 'dataflow_metrics_2',
            '--methodology=EVENT',
            '--calculation_month_limit=6',
            '--include_race=False',
            '--include_age=False',
            '--include_ethnicity=False',
            '--include_gender=False',
            '--save_as_template'
        ]

        # Act
        incarceration_pipeline_args, apache_beam_args = incarceration_pipeline.parse_arguments(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert
        expected_incarceration_pipeline_args = \
            Namespace(calculation_month_limit=6, include_age=False, include_ethnicity=False, include_gender=False,
                      include_race=False, input='county', methodology='EVENT', output='dataflow_metrics_2',
                      person_filter_ids=None, reference_input='dashboard_views_2', state_code=None)

        self.assertEqual(incarceration_pipeline_args, expected_incarceration_pipeline_args)

        expected_apache_beam_options_dict = {
            'runner': 'DirectRunner',
            'project': 'recidiviz-staging',
            'job_name': 'incarceration-args-test',

            # Locations based on the overriden bucket, not the project!
            'staging_location': 'gs://recidiviz-123-my-bucket/staging/',
            'temp_location': 'gs://recidiviz-123-my-bucket/temp/',
            'template_location': 'gs://recidiviz-123-my-bucket/templates/incarceration-args-test',

            'machine_type': 'n1-standard-4',
            'network': 'default',
            'subnetwork':
            'https://www.googleapis.com/compute/v1/projects/recidiviz-staging/regions/us-central1/subnetworks/default',
            'use_public_ips': False,
            'experiments': ['shuffle_mode=service'],
            'setup_file': './setup2.py'
        }

        self.assertEqual(pipeline_options.get_all_options(drop_default=True), expected_apache_beam_options_dict)

    def test_incarceration_pipeline_specify_person_id_filters(self):
        # Arrange
        argv = ['--job_name', 'incarceration-args-test',
                '--project', 'recidiviz-staging',
                '--person_filter_ids', '685253', '12345', '99999',
                '--setup_file', './setup.py']

        # Act
        incarceration_pipeline_args, apache_beam_args = incarceration_pipeline.parse_arguments(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert

        expected_incarceration_pipeline_args = Namespace(**self.DEFAULT_INCARCERATION_PIPELINE_ARGS.__dict__)
        expected_incarceration_pipeline_args.person_filter_ids = [685253, 12345, 99999]

        self.assertEqual(incarceration_pipeline_args, expected_incarceration_pipeline_args)
        self.assertEqual(pipeline_options.get_all_options(drop_default=True), self.DEFAULT_APACHE_BEAM_OPTIONS_DICT)
