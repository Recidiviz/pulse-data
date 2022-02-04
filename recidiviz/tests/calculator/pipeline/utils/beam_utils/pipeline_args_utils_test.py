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
from typing import List

import attr
from apache_beam.options.pipeline_options import PipelineOptions

from recidiviz.calculator.dataflow_config import DATAFLOW_TABLES_TO_METRIC_TYPES
from recidiviz.calculator.pipeline.base_pipeline import BasePipeline
from recidiviz.calculator.pipeline.calculation_pipeline import (
    CalculationPipelineJobArgs,
)
from recidiviz.calculator.pipeline.incarceration.pipeline import (
    IncarcerationPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.utils.beam_utils.pipeline_args_utils import (
    derive_apache_beam_pipeline_args,
)


class TestPipelineArgsUtils(unittest.TestCase):
    """Unittests for helpers in pipeline_args_utils.py."""

    ALL_METRICS = {
        metric: True
        for table, metric in DATAFLOW_TABLES_TO_METRIC_TYPES.items()
        if "incarceration" in table
    }

    default_beam_args: List[str] = [
        "--project",
        "recidiviz-staging",
        "--job_name",
        "incarceration-args-test",
    ]

    pipeline_options = PipelineOptions(
        derive_apache_beam_pipeline_args(default_beam_args)
    )

    DEFAULT_INCARCERATION_PIPELINE_ARGS = CalculationPipelineJobArgs(
        state_code="US_XX",
        project_id="recidiviz-staging",
        input_dataset="state",
        output_dataset="dataflow_metrics",
        metric_inclusions=ALL_METRICS,
        person_id_filter_set=None,
        reference_dataset="reference_views",
        static_reference_dataset="static_reference_tables",
        calculation_month_count=1,
        calculation_end_month=None,
        job_name="incarceration-args-test",
        region="us-west1",
        # These are dummy beam pipeline options just to properly instantiate this
        # class. The equality of expected beam pipeline options are all tested
        # separately from the rest of the attributes on the CalculationPipelineJobArgs.
        apache_beam_pipeline_options=pipeline_options,
    )

    DEFAULT_APACHE_BEAM_OPTIONS_DICT = {
        "runner": "DataflowRunner",
        "project": "recidiviz-staging",
        "job_name": "incarceration-args-test",
        "save_main_session": True,
        # This location holds execution files necessary for running each pipeline and is
        # only updated when templates are deployed.
        "staging_location": "gs://recidiviz-staging-dataflow-templates/staging/",
        # This location holds actual temp files generated during each pipeline run.
        "temp_location": "gs://recidiviz-staging-dataflow-templates-scratch/temp/",
        "region": "us-west1",
        "machine_type": "n1-standard-4",
        "network": "default",
        "subnetwork": "https://www.googleapis.com/compute/v1/projects/recidiviz-staging/"
        "regions/us-west1/subnetworks/default",
        "use_public_ips": False,
        "experiments": ["shuffle_mode=service", "use_beam_bq_sink", "use_runner_v2"],
        "extra_packages": ["dist/recidiviz-calculation-pipelines.tar.gz"],
        "disk_size_gb": 50,
    }

    def _assert_pipeline_args_equal_exclude_beam_options(
        self,
        expected_pipeline_args: CalculationPipelineJobArgs,
        actual_pipeline_args: CalculationPipelineJobArgs,
    ):
        """Asserts that every field except for the apache_beam_pipeline_options on
        the expected CalculationPipelineJobArgs matches the actual
        CalculationPipelineJobArgs."""
        for field in attr.fields_dict(expected_pipeline_args.__class__).keys():
            if field == "apache_beam_pipeline_options":
                continue
            self.assertEqual(
                getattr(expected_pipeline_args, field),
                getattr(actual_pipeline_args, field),
            )

    def test_minimal_incarceration_pipeline_args(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--project",
            "recidiviz-staging",
            "--extra_package",
            "dist/recidiviz-calculation-pipelines.tar.gz",
            "--state_code",
            "US_XX",
        ]

        # Act
        pipeline = BasePipeline(
            pipeline_run_delegate=IncarcerationPipelineRunDelegate.build_from_args(argv)
        )
        incarceration_pipeline_args = pipeline.pipeline_run_delegate.pipeline_job_args
        pipeline_options = (
            pipeline.pipeline_run_delegate.pipeline_job_args.apache_beam_pipeline_options
        )

        # Assert
        self._assert_pipeline_args_equal_exclude_beam_options(
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
            "--state_code",
            "US_XX",
        ]
        # Act
        pipeline = BasePipeline(
            pipeline_run_delegate=IncarcerationPipelineRunDelegate.build_from_args(argv)
        )
        incarceration_pipeline_args = pipeline.pipeline_run_delegate.pipeline_job_args
        pipeline_options = (
            pipeline.pipeline_run_delegate.pipeline_job_args.apache_beam_pipeline_options
        )

        # Assert
        self._assert_pipeline_args_equal_exclude_beam_options(
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
            "--state_code",
            "US_XX",
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
        pipeline = BasePipeline(
            pipeline_run_delegate=IncarcerationPipelineRunDelegate.build_from_args(argv)
        )
        incarceration_pipeline_args = pipeline.pipeline_run_delegate.pipeline_job_args
        pipeline_options = (
            pipeline.pipeline_run_delegate.pipeline_job_args.apache_beam_pipeline_options
        )

        # Assert
        expected_incarceration_pipeline_args = attr.evolve(
            self.DEFAULT_INCARCERATION_PIPELINE_ARGS,
            calculation_month_count=6,
            calculation_end_month="2009-07",
            input_dataset="county",
            output_dataset="dataflow_metrics_2",
            reference_dataset="reference_views_2",
            static_reference_dataset="static_reference_2",
            region="us-central1",
        )

        self._assert_pipeline_args_equal_exclude_beam_options(
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
            "experiments": [
                "shuffle_mode=service",
                "use_beam_bq_sink",
                "use_runner_v2",
            ],
            "extra_packages": ["dist/recidiviz-calculation-pipelines.tar.gz"],
            "disk_size_gb": 50,
            "save_main_session": True,
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
            "--state_code",
            "US_XX",
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
        pipeline = BasePipeline(
            pipeline_run_delegate=IncarcerationPipelineRunDelegate.build_from_args(argv)
        )
        incarceration_pipeline_args = pipeline.pipeline_run_delegate.pipeline_job_args
        pipeline_options = (
            pipeline.pipeline_run_delegate.pipeline_job_args.apache_beam_pipeline_options
        )

        # Assert
        expected_incarceration_pipeline_args = attr.evolve(
            self.DEFAULT_INCARCERATION_PIPELINE_ARGS,
            person_id_filter_set={685253, 12345, 99999},
        )

        self._assert_pipeline_args_equal_exclude_beam_options(
            incarceration_pipeline_args, expected_incarceration_pipeline_args
        )
        self.assertEqual(
            self.DEFAULT_APACHE_BEAM_OPTIONS_DICT,
            pipeline_options.get_all_options(drop_default=True),
        )

    def test_incarceration_pipeline_args_additional_bad_arg(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--state_code",
            "US_XX",
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
        with self.assertRaises(SystemExit) as e:
            _ = BasePipeline(
                pipeline_run_delegate=IncarcerationPipelineRunDelegate.build_from_args(
                    argv
                )
            )
        self.assertEqual(2, e.exception.code)

    def test_incarceration_pipeline_args_missing_arg(self) -> None:
        # Arrange
        argv = [
            "--job_name",
            "incarceration-args-test",
            "--state_code",
            "US_XX",
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
        with self.assertRaises(SystemExit) as e:
            _ = BasePipeline(
                pipeline_run_delegate=IncarcerationPipelineRunDelegate.build_from_args(
                    argv
                )
            )
        self.assertEqual(2, e.exception.code)
