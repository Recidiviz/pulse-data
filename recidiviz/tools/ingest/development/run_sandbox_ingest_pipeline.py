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
pipeline for the given state.

Usage:
    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project PROJECT_ID \
        --state_code US_XX \
        --output_sandbox_prefix output_sandbox_prefix \
        [--raw_data_source_instance INSTANCE] \
        [--skip_build True/False] 

Examples:
    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project recidiviz-staging \
        --state_code US_TX \
        --output_sandbox_prefix santy_staff

    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project recidiviz-staging \
        --state_code US_CA \
        --output_sandbox_prefix my_prefix \
        --raw_data_source_instance SECONDARY \
        --ingest_view_results_only True \
        --pre_normalization_only True \
        --skip_build True \
        --ingest_views_to_run "person staff" \
        --service_account_email something@recidiviz-staging.iam.gserviceaccount.com 
"""
import argparse
import json
import logging
from datetime import datetime
from typing import List, Tuple

from tabulate import tabulate

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    create_or_update_dataflow_sandbox,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_sandbox_pipeline_username,
    run_sandbox_dataflow_pipeline,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def parse_run_arguments() -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to start a sandbox pipeline to a Namespace."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project",
        type=str,
        help="ID of the GCP project.",
        choices=DATA_PLATFORM_GCP_PROJECTS,
        required=True,
    )
    parser.add_argument(
        "--state_code",
        help="The state code that the export should occur for",
        type=StateCode,
        choices=get_existing_direct_ingest_states(),
        required=True,
    )

    parser.add_argument(
        "--raw_data_source_instance",
        help="The ingest instance data is from",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        required=False,
        default=DirectIngestInstance.PRIMARY,
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


def get_raw_data_upper_bound_dates_json_for_sandbox_pipeline(
    project_id: str,
    state_code: StateCode,
    raw_data_source_instance: DirectIngestInstance,
) -> str:
    """Builds JSON describing the raw data upper bound dates that should be used for a
    sandbox ingest pipeline.

    Will crash if a local_project_id_override is set already.
    """
    right_now = datetime.now()

    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    launchable_ingest_views = ingest_manifest_collector.launchable_ingest_views(
        # Use the context for the project we will be running against, not the one that
        # corresponds to the local environment so that we don't identify local-only
        # views as launchable here.
        IngestViewContentsContext.build_for_project(project_id=project_id),
    )
    view_collector = DirectIngestViewQueryBuilderCollector(
        region,
        launchable_ingest_views,
    )

    raw_table_dependencies = {
        raw_data_dependency.raw_file_config.file_tag
        for ingest_view in launchable_ingest_views
        for raw_data_dependency in view_collector.get_query_builder_by_view_name(
            ingest_view
        ).raw_table_dependency_configs
    }

    raw_file_metadata_manager = DirectIngestRawFileMetadataManager(
        state_code.value, raw_data_source_instance
    )

    with local_project_id_override(project_id), cloudsql_proxy_control.connection(
        schema_type=SchemaType.OPERATIONS
    ), SessionFactory.for_proxy(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
    ) as session:
        raw_data_max_upper_bounds = raw_file_metadata_manager.get_max_update_datetimes(
            session
        )

    raw_data_upper_bound_dates_json = json.dumps(
        {
            file_tag: (
                right_now.isoformat()
                if file_tag not in raw_data_max_upper_bounds
                else raw_data_max_upper_bounds[file_tag].strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )
            )
            for file_tag in raw_table_dependencies
        }
    )
    return raw_data_upper_bound_dates_json


def get_extra_pipeline_parameter_args(
    project: str,
    state_code: StateCode,
    raw_data_source_instance: DirectIngestInstance,
) -> List[str]:
    """Returns additional pipeline command-line args that can be inferred from the
    state code, instance and sandbox prefix.
    """
    raw_data_upper_bound_dates_json = (
        get_raw_data_upper_bound_dates_json_for_sandbox_pipeline(
            project,
            state_code,
            raw_data_source_instance,
        )
    )

    return [
        # TODO(#18108): Once we have a distinct entrypoint for each pipeline type, we
        #  likely won't need this arg.
        "--pipeline",
        INGEST_PIPELINE_NAME,
        "--project",
        project,
        "--state_code",
        state_code.value,
        "--raw_data_source_instance",
        raw_data_source_instance.value,
        "--raw_data_upper_bound_dates_json",
        raw_data_upper_bound_dates_json,
        "--sandbox_username",
        get_sandbox_pipeline_username(),
    ]


def run_sandbox_ingest_pipeline(
    params: IngestPipelineParameters, skip_build: bool
) -> None:
    """Creates appropriate sandbox datasets then runs the sandbox pipeline with the
    given parameters.
    """
    output_sandbox_prefix = params.output_sandbox_prefix
    if not output_sandbox_prefix:
        raise ValueError("Must specify an --output_sandbox_prefix")

    upper_bounds = json.loads(
        params.raw_data_upper_bound_dates_json,
    )
    logging.info(
        "Using raw data watermarks from latest run:\n%s",
        tabulate(
            [(tbl, upper_bounds[tbl]) for tbl in sorted(upper_bounds)],
            headers=["Table", "Raw Data Watermark"],
        ),
    )

    if params.ingest_views_to_run and not params.pre_normalization_only:
        prompt_for_confirmation(
            f"⚠️This pipeline will run entity normalization against a limited set of "
            f"ingest views: {params.ingest_views_to_run}. Normalization may crash or "
            f"produce unpredictable results if this set of views was not selected "
            f"carefully. For example, you cannot normalize entities that reference "
            f"staff external_id values without also ingesting the view(s) that produce "
            f"StateStaff. It's generally advised that you pair the "
            f"--ingest_views_to_run argument with --pre_normalization_only. Are you "
            f"sure you want to proceed?"
        )

    output_datasets = [
        params.ingest_view_results_output,
        params.pre_normalization_output,
    ]
    if not params.pre_normalization_only:
        output_datasets.append(params.normalized_output)

    prompt_for_confirmation(
        "\n\nCreating Sandbox Pipeline With Parameters:\n"
        + tabulate(
            [
                ("Job Name", params.job_name),
                ("State Code", params.state_code),
                ("Project", params.project),
                ("Output Datasets", output_datasets),
            ],
            headers=["Parameter", "Value"],
            tablefmt="rounded_grid",
        )
        + "\nWould you like to continue?"
    )

    create_or_update_dataflow_sandbox(
        sandbox_dataset_prefix=output_sandbox_prefix,
        pipelines=[INGEST_PIPELINE_NAME],
        recreate=True,
        state_code_filter=StateCode(params.state_code),
    )

    run_sandbox_dataflow_pipeline(params, skip_build)


def main() -> None:
    """Creates sandbox datasets (as appropriate) and launches a sandbox ingest
    pipline as specified by the script args.
    """
    known_args, remaining_args = parse_run_arguments()
    remaining_args += get_extra_pipeline_parameter_args(
        known_args.project,
        known_args.state_code,
        known_args.raw_data_source_instance,
    )

    params = IngestPipelineParameters.parse_from_args(
        remaining_args, sandbox_pipeline=True
    )
    if params.state_code != known_args.state_code.value:
        raise ValueError(
            f"Generated params state_code [{params.state_code}] does not match the "
            f"input state_code [{known_args.state_code.value}]."
        )
    if params.raw_data_source_instance != known_args.raw_data_source_instance.value:
        raise ValueError(
            f"Generated params raw_data_source_instance "
            f"[{params.raw_data_source_instance}] does not match the input "
            f"raw_data_source_instance [{known_args.raw_data_source_instance.value}]."
        )

    with local_project_id_override(params.project):
        run_sandbox_ingest_pipeline(params, skip_build=known_args.skip_build)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
