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
"""Helper that runs a test version of the pipeline in the provided module."""
import datetime
from typing import Any, Callable, Dict, Optional, Set

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from mock import patch

from recidiviz.calculator.pipeline.base_pipeline import BasePipeline
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    FakeReadFromBigQuery,
    FakeWriteToBigQuery,
)


def test_pipeline_options() -> Dict[str, str]:
    """Returns a dictionary of PipelineOptions augmented with the local job_timestamp
    for the job_id of the test runs."""
    all_pipeline_options = PipelineOptions().get_all_options()

    job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
    all_pipeline_options["job_timestamp"] = job_timestamp
    return all_pipeline_options


def run_test_pipeline(
    pipeline: BasePipeline,
    state_code: str,
    dataset: str,
    read_from_bq_constructor: Callable[[str], FakeReadFromBigQuery],
    write_to_bq_constructor: Callable[[str, str], FakeWriteToBigQuery],
    include_calculation_limit_args: bool = True,
    unifying_id_field_filter_set: Optional[Set[int]] = None,
    metric_types_filter: Optional[Set[str]] = None,
) -> None:
    """Runs a test version of the pipeline in the provided module with BQ I/O mocked out."""

    project_id, dataset_id = dataset.split(".")

    def pipeline_constructor(options: PipelineOptions) -> TestPipeline:
        non_default_options = options.get_all_options(drop_default=True)
        expected_non_default_options = {
            "project": project_id,
            "save_main_session": True,
        }

        if not expected_non_default_options == non_default_options:
            raise ValueError(
                f"Expected non-default options [{expected_non_default_options}] do not match actual "
                f"non-default options [{non_default_options}]"
            )
        return TestPipeline()

    calculation_limit_args: Dict[str, Any] = {}
    if include_calculation_limit_args:
        calculation_limit_args = {
            "calculation_end_month": None,
            "calculation_month_count": -1,
        }

    with patch(
        "recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery",
        read_from_bq_constructor,
    ):
        with patch(
            "recidiviz.calculator.pipeline.utils.beam_utils.ReadFromBigQuery",
            read_from_bq_constructor,
        ):
            with patch(
                "recidiviz.calculator.pipeline.base_pipeline.WriteAppendToBigQuery",
                write_to_bq_constructor,
            ):
                with patch(
                    "recidiviz.calculator.pipeline.base_pipeline.beam.Pipeline",
                    pipeline_constructor,
                ):
                    metric_types = (
                        list(metric_types_filter) if metric_types_filter else ["ALL"]
                    )
                    person_filter_ids = (
                        list(unifying_id_field_filter_set)
                        if unifying_id_field_filter_set
                        else None
                    )
                    pipeline.run(  # type: ignore[attr-defined]
                        apache_beam_pipeline_options=PipelineOptions(
                            project=project_id
                        ),
                        data_input=dataset_id,
                        reference_view_input=dataset_id,
                        static_reference_input=dataset_id,
                        output=dataset,
                        metric_types=metric_types,
                        state_code=state_code,
                        person_filter_ids=person_filter_ids,
                        **calculation_limit_args,
                    )
