# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Driver script to launch test flex template calculation pipeline jobs with output directed to
sandbox dataflow datasets.

See http://go/run-dataflow/ for more information on running Dataflow pipelines.

usage: python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
          --pipeline PIPELINE_NAME \
          --type PIPELINE_TYPE \
          --project PROJECT \
          --state_code STATE_CODE \
          --output_sandbox_prefix OUTPUT_SANDBOX_PREFIX \
          [--input_dataset_overrides_json INPUT_DATASET_OVERRIDES_JSON]
          [--skip_build]

          ..and any other pipeline-specific args

Examples:
    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline recidivism_metrics \
        --type metrics \
        --project recidiviz-staging \
        --output_sandbox_prefix username \
        --input_dataset_overrides_json '{"normalized_state": "my_sandbox_normalized_state"}'
        --state_code US_XX \
        --calculation_month_count 36
        # Note: The --metric_types arg must be last since it is a list
        --metric_types "REINCARCERATION_COUNT REINCARCERATION_RATE"

    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline comprehensive_normalization \
        --type normalization \
        --project recidiviz-staging \
        --output_sandbox_prefix username \
        --input_dataset_overrides_json '{"us_xx_state_primary": "my_sandbox_us_xx_state_primary"}' \
        --state_code US_XX

    python -m recidiviz.tools.calculator.run_sandbox_calculation_pipeline \
        --pipeline us_ix_case_note_extracted_entities_supplemental \
        --type supplemental \
        --project recidiviz-staging \
        --output_sandbox_prefix username \
        --state_code US_IX

You must also include any arguments required by the given pipeline.

NOTE: To run ingest pipelines, use
    recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline instead.
"""
from __future__ import absolute_import

import argparse
import logging
from typing import Dict, List, Tuple, Type

from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.pipelines.pipeline_parameters import PipelineParameters
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.pipelines.utils.pipeline_run_utils import collect_all_pipeline_classes
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    run_sandbox_dataflow_pipeline,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.metadata import local_project_id_override

PIPELINE_PARAMETER_TYPES: Dict[str, Type[PipelineParameters]] = {
    "metrics": MetricsPipelineParameters,
    "normalization": NormalizationPipelineParameters,
    "supplemental": SupplementalPipelineParameters,
}


def parse_run_arguments() -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to start a sandbox pipeline to a Namespace."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--type",
        type=str,
        dest="pipeline_type",
        help="type of the pipeline",
        choices=["metrics", "normalization", "supplemental"],
        required=True,
    )

    parser.add_argument(
        "--skip_build",
        dest="skip_build",
        help="If set to true, the image will not be rebuilt and submitted. "
        "Useful for if you did not change pipeline code and want to run "
        "pipelines with different parameters (e.g. for different states) "
        "for the same pipeline image",
        required=False,
        action=argparse.BooleanOptionalAction,
    )

    parser.add_argument(
        "--pipeline",
        type=str,
        dest="pipeline",
        help="The name of the specific pipeline to run (e.g. 'incarceration_metrics').",
        choices=[p.pipeline_name().lower() for p in collect_all_pipeline_classes()],
    )

    return parser.parse_known_args()


def parse_pipeline_parameters(
    known_args: argparse.Namespace, remaining_args: List[str]
) -> PipelineParameters:
    parameter_cls = PIPELINE_PARAMETER_TYPES[known_args.pipeline_type]
    return parameter_cls.parse_from_args(
        remaining_args + ["--pipeline", known_args.pipeline], sandbox_pipeline=True
    )


def main() -> None:
    known_args, remaining_args = parse_run_arguments()
    params = parse_pipeline_parameters(known_args, remaining_args)
    # Have the user confirm that the sandbox dataflow dataset exists.
    for attr in dir(params):
        if attr.endswith("output") and isinstance(getattr(params, attr), str):
            prompt_for_confirmation(
                "Have you already created a sandbox dataflow dataset called "
                f"`{getattr(params, attr)}` using `create_or_update_dataflow_sandbox`?",
            )
    with local_project_id_override(params.project):
        run_sandbox_dataflow_pipeline(params, known_args.skip_build)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
