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

from recidiviz.calculator.pipeline.incarceration.pipeline import IncarcerationPipeline
from recidiviz.calculator.pipeline.utils.pipeline_args_utils import (
    get_apache_beam_pipeline_options_from_args,
)


class TestPipelineArgsUtils(unittest.TestCase):
    """Unittests for helpers in pipeline_args_utils.py."""

    DEFAULT_INCARCERATION_PIPELINE_ARGS = Namespace(
        calculation_month_count=1,
        calculation_end_month=None,
        data_input="state",
        output="dataflow_metrics",
        metric_types={"ALL"},
        person_filter_ids=None,
        reference_view_input="reference_views",
        static_reference_input="static_reference_tables",
        state_code=None,
    )

    DEFAULT_APACHE_BEAM_OPTIONS_DICT = {
        "runner": "DataflowRunner",
        "project": "recidiviz-staging",
        "job_name": "incarceration-args-test",
        "staging_location": "gs://recidiviz-staging-dataflow-templates/staging/",
        "temp_location": "gs://recidiviz-staging-dataflow-templates/temp/",
        "region": "us-west1",
        "machine_type": "n1-standard-4",
        "network": "default",
        "subnetwork": "https://www.googleapis.com/compute/v1/projects/recidiviz-staging/"
        "regions/us-west1/subnetworks/default",
        "use_public_ips": False,
        "experiments": ["shuffle_mode=service", "use_beam_bq_sink"],
        "extra_packages": ["dist/recidiviz-calculation-pipelines.tar.gz"],
        "disk_size_gb": 50,
    }

    TEST_PIPELINE = IncarcerationPipeline()

    def test_minimal_incarceration_pipeline_args(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--project",
            "recidiviz-staging",
            "--extra_package",
            "dist/recidiviz-calculation-pipelines.tar.gz",
        ]

        # Act
        (
            incarceration_pipeline_args,
            apache_beam_args,
        ) = self.TEST_PIPELINE.get_arg_parser().parse_known_args(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert
        self.assertEqual(
            incarceration_pipeline_args, self.DEFAULT_INCARCERATION_PIPELINE_ARGS
        )
        self.assertEqual(
            pipeline_options.get_all_options(drop_default=True),
            self.DEFAULT_APACHE_BEAM_OPTIONS_DICT,
        )

    def test_minimal_incarceration_pipeline_args_save_to_template(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--project",
            "recidiviz-staging",
            "--save_as_template",
            "--extra_package",
            "dist/recidiviz-calculation-pipelines.tar.gz",
        ]
        # Act
        (
            incarceration_pipeline_args,
            apache_beam_args,
        ) = self.TEST_PIPELINE.get_arg_parser().parse_known_args(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert
        self.assertEqual(
            incarceration_pipeline_args, self.DEFAULT_INCARCERATION_PIPELINE_ARGS
        )

        expected_apache_beam_options_dict = self.DEFAULT_APACHE_BEAM_OPTIONS_DICT.copy()
        expected_apache_beam_options_dict[
            "template_location"
        ] = "gs://recidiviz-staging-dataflow-templates/templates/incarceration-args-test"

        self.assertEqual(
            pipeline_options.get_all_options(drop_default=True),
            expected_apache_beam_options_dict,
        )

    def test_incarceration_pipeline_args_defaults_changed(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--runner",
            "DirectRunner",
            "--project",
            "recidiviz-staging",
            "--extra_package",
            "dist/recidiviz-calculation-pipelines.tar.gz",
            "--bucket",
            "recidiviz-123-my-bucket",
            "--region=us-central1",
            "--data_input",
            "county",
            "--reference_view_input",
            "reference_views_2",
            "--static_reference_input",
            "static_reference_2",
            "--output",
            "dataflow_metrics_2",
            "--calculation_month_count=6",
            "--calculation_end_month=2009-07",
            "--save_as_template",
        ]

        # Act
        (
            incarceration_pipeline_args,
            apache_beam_args,
        ) = self.TEST_PIPELINE.get_arg_parser().parse_known_args(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert
        expected_incarceration_pipeline_args = Namespace(
            calculation_month_count=6,
            calculation_end_month="2009-07",
            data_input="county",
            output="dataflow_metrics_2",
            metric_types={"ALL"},
            person_filter_ids=None,
            reference_view_input="reference_views_2",
            static_reference_input="static_reference_2",
            state_code=None,
        )

        self.assertEqual(
            incarceration_pipeline_args, expected_incarceration_pipeline_args
        )

        expected_apache_beam_options_dict = {
            "runner": "DirectRunner",
            "project": "recidiviz-staging",
            "job_name": "incarceration-args-test",
            # Locations based on the overriden bucket, not the project!
            "staging_location": "gs://recidiviz-123-my-bucket/staging/",
            "temp_location": "gs://recidiviz-123-my-bucket/temp/",
            "template_location": "gs://recidiviz-123-my-bucket/templates/incarceration-args-test",
            "region": "us-central1",
            "machine_type": "n1-standard-4",
            "network": "default",
            "subnetwork": "https://www.googleapis.com/compute/v1/projects/recidiviz-staging/"
            "regions/us-central1/subnetworks/default",
            "use_public_ips": False,
            "experiments": ["shuffle_mode=service", "use_beam_bq_sink"],
            "extra_packages": ["dist/recidiviz-calculation-pipelines.tar.gz"],
            "disk_size_gb": 50,
        }

        self.assertEqual(
            expected_apache_beam_options_dict,
            pipeline_options.get_all_options(drop_default=True),
        )

    def test_incarceration_pipeline_specify_person_id_filters(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--project",
            "recidiviz-staging",
            "--person_filter_ids",
            "685253",
            "12345",
            "99999",
            "--extra_package",
            "dist/recidiviz-calculation-pipelines.tar.gz",
        ]

        # Act
        (
            incarceration_pipeline_args,
            apache_beam_args,
        ) = self.TEST_PIPELINE.get_arg_parser().parse_known_args(argv)
        pipeline_options = get_apache_beam_pipeline_options_from_args(apache_beam_args)

        # Assert

        expected_incarceration_pipeline_args = Namespace(
            **self.DEFAULT_INCARCERATION_PIPELINE_ARGS.__dict__
        )
        expected_incarceration_pipeline_args.person_filter_ids = [685253, 12345, 99999]

        self.assertEqual(
            incarceration_pipeline_args, expected_incarceration_pipeline_args
        )
        self.assertEqual(
            pipeline_options.get_all_options(drop_default=True),
            self.DEFAULT_APACHE_BEAM_OPTIONS_DICT,
        )

    def test_incarceration_pipeline_args_additional_bad_arg(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--runner",
            "DirectRunner",
            "--project",
            "recidiviz-staging",
            "--bucket",
            "recidiviz-123-my-bucket",
            "--region=us-central1",
            "--data_input",
            "county",
            "--reference_view_input",
            "reference_views_2",
            "--output",
            "dataflow_metrics_2",
            "--calculation_month_count=6",
            "--calculation_month_count_bad=6",
            "--calculation_end_month=2009-07",
            "--save_as_template",
        ]

        # Act
        (
            _incarceration_pipeline_args,
            apache_beam_args,
        ) = self.TEST_PIPELINE.get_arg_parser().parse_known_args(argv)

        with self.assertRaises(SystemExit) as e:
            _ = get_apache_beam_pipeline_options_from_args(apache_beam_args)
        self.assertEqual(2, e.exception.code)

    def test_incarceration_pipeline_args_missing_arg(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--runner",
            "DirectRunner",
            # project arg omitted here
            "--extra_package",
            "dist/recidiviz-calculation-pipelines.tar.gz",
            "--bucket",
            "recidiviz-123-my-bucket",
            "--region=us-central1",
            "--data_input",
            "county",
            "--reference_view_input",
            "reference_views_2",
            "--output",
            "dataflow_metrics_2",
            "--calculation_month_count=6",
            "--calculation_end_month=2009-07",
            "--save_as_template",
        ]

        # Act
        (
            _incarceration_pipeline_args,
            apache_beam_args,
        ) = self.TEST_PIPELINE.get_arg_parser().parse_known_args(argv)

        with self.assertRaises(SystemExit) as e:
            _ = get_apache_beam_pipeline_options_from_args(apache_beam_args)
        self.assertEqual(2, e.exception.code)
