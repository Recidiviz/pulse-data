# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Script that creates sandbox datasets (as appropriate) and launches a sandbox ingest
pipline for the given state.

Usage:
    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project PROJECT_ID \
        --state_code US_XX \
        --sandbox_prefix SANDBOX_PREFIX \
        [--ingest_instance INSTANCE] \
        [--skip_build True/False] 

Examples:
    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project recidiviz-staging \
        --state_code US_CA \
        --sandbox_prefix my_prefix

    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project recidiviz-staging \
        --state_code US_CA \
        --sandbox_prefix my_prefix \
        --ingest_instance SECONDARY \
        --materialization_method original \
        --ingest_view_results_only True \
        --skip_build True
"""
import argparse
import json
import logging
from datetime import datetime
from typing import List, Tuple

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import state_dataset_for_state_code
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataflow_dataset,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegateImpl,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.pipelines.ingest.pipeline_parameters import (
    IngestPipelineParameters,
    MaterializationMethod,
)
from recidiviz.pipelines.ingest.pipeline_utils import ingest_pipeline_name
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    create_or_update_ingest_output_sandbox,
)
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    run_sandbox_dataflow_pipeline,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.metadata import local_project_id_override


def parse_run_arguments() -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to start a sandbox pipeline to a Namespace."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state_code",
        help="The state code that the export should occur for",
        type=StateCode,
        choices=get_existing_direct_ingest_states(),
        required=True,
    )

    parser.add_argument(
        "--ingest_instance",
        help="The ingest instance data is from",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        required=False,
        default=DirectIngestInstance.PRIMARY,
    )

    parser.add_argument(
        "--sandbox_prefix",
        required=True,
        type=str,
        help="The prefix to use for any sandbox output datasets as well as the pipeline job name.",
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

    return parser.parse_known_args()


def get_extra_pipeline_parameter_args(
    state_code: StateCode, ingest_instance: DirectIngestInstance, sandbox_prefix: str
) -> List[str]:
    """Returns additional pipeline command-line args that can be inferred from the
    state code, instance and sandbox prefix.
    """

    standard_job_name = ingest_pipeline_name(state_code, ingest_instance)

    job_name = f"{sandbox_prefix}-{standard_job_name}-test".replace("_", "-")

    sandbox_output_dataset = state_dataset_for_state_code(
        state_code,
        ingest_instance,
        sandbox_dataset_prefix=sandbox_prefix,
    )

    sandbox_ingest_view_results_output_dataset = (
        BigQueryAddressOverrides.format_sandbox_dataset(
            sandbox_prefix,
            ingest_view_materialization_results_dataflow_dataset(
                state_code, ingest_instance
            ),
        )
    )

    right_now = datetime.now()

    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=IngestViewResultsParserDelegateImpl(
            region=region,
            schema_type=SchemaType.STATE,
            ingest_instance=ingest_instance,
            results_update_datetime=right_now,
        ),
    )
    view_collector = DirectIngestViewQueryBuilderCollector(
        region,
        ingest_manifest_collector.launchable_ingest_views(),
    )

    # TODO(#24519) Update to use Cloud SQL proxy and reuse query from raw file metadata.
    raw_data_upper_bound_dates_json = json.dumps(
        {
            raw_data_dependency.raw_file_config.file_tag: right_now.isoformat()
            for ingest_view in ingest_manifest_collector.launchable_ingest_views()
            for raw_data_dependency in view_collector.get_query_builder_by_view_name(
                ingest_view
            ).raw_table_dependency_configs
        }
    )

    return [
        # TODO(#18108): Once we have a distinct entrypoint for each pipeline type, we
        #  likely won't need this arg.
        "--pipeline",
        "ingest",
        "--state_code",
        state_code.value,
        "--job_name",
        job_name,
        "--sandbox_output_dataset",
        sandbox_output_dataset,
        "--sandbox_ingest_view_results_output_dataset",
        sandbox_ingest_view_results_output_dataset,
        # TODO(#22144): Establish a more permanent testing service account(s) to use
        #  here.
        "--service_account_email"
        "emily-temporary-sa-testing-dat@recidiviz-staging.iam.gserviceaccount.com",
        "--raw_data_upper_bound_dates_json",
        raw_data_upper_bound_dates_json,
    ]


def main() -> None:
    """Creates sandbox datasets (as appropriate) and launches a sandbox ingest
    pipline as specified by the script args.
    """
    known_args, remaining_args = parse_run_arguments()
    remaining_args += get_extra_pipeline_parameter_args(
        known_args.state_code, known_args.ingest_instance, known_args.sandbox_prefix
    )

    params = IngestPipelineParameters.parse_from_args(
        remaining_args, sandbox_pipeline=True
    )

    prompt_for_confirmation(
        f"Starting ingest pipeline [{params.job_name}] for [{params.state_code}] in "
        f"[{params.project}] which will output to datasets "
        f"[{params.ingest_view_results_output}] and [{params.output}] - continue?"
    )

    if params.materialization_method == MaterializationMethod.ORIGINAL.value:
        prompt_for_confirmation(
            "Pipeline will use materialization method ORIGINAL which can be very "
            "expensive (>$100 per run) - continue?"
        )

    with local_project_id_override(params.project):
        bq_client = BigQueryClientImpl()
        create_or_update_ingest_output_sandbox(
            bq_client,
            known_args.state_code,
            known_args.ingest_instance,
            known_args.sandbox_prefix,
            allow_overwrite=True,
        )

        run_sandbox_dataflow_pipeline(params, known_args.skip_build)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
