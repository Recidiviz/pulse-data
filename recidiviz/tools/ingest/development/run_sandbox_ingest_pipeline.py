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
        --output_sandbox_prefix output_sandbox_prefix \
        [--ingest_instance INSTANCE] \
        [--skip_build True/False] 

Examples:
    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project recidiviz-staging \
        --state_code US_XX \
        --output_sandbox_prefix my_prefix

    python -m recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline \
        --project recidiviz-staging \
        --state_code US_CA \
        --output_sandbox_prefix my_prefix \
        --ingest_instance SECONDARY \
        --ingest_view_results_only True \
        --skip_build True \
        --ingest_views_to_run "person staff" \
        --service_account_email_override something@recidiviz-staging.iam.gserviceaccount.com 
"""
import argparse
import json
import logging
from datetime import datetime

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
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
from recidiviz.pipelines.ingest.pipeline_parameters import (
    INGEST_PIPELINE_NAME,
    IngestPipelineParameters,
)
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    create_or_update_ingest_output_sandbox,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_sandbox_pipeline_username,
    run_sandbox_dataflow_pipeline,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def parse_run_arguments() -> argparse.Namespace:
    """Parses the arguments needed to start a sandbox pipeline to a Namespace."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project",
        type=str,
        help="ID of the GCP project.",
        choices=GCP_PROJECTS,
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
        "--ingest_instance",
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

    parser.add_argument(
        "--output_sandbox_prefix",
        dest="output_sandbox_prefix",
        help="This prefix string will be placed before any dataset name output by this pipeline.",
        required=True,
        type=str,
    )

    parser.add_argument(
        "--ingest_view_results_only",
        dest="ingest_view_results_only",
        help="This will cause the pipeline to skip all actions after 'Write ingest_view results to table.'",
        required=False,
        default=False,
    )

    parser.add_argument(
        "--ingest_views_to_run",
        dest="ingest_views_to_run",
        help="The ingest views you would like to run with this pipeline, separated by a space. If this is None, the pipeline will run all launchable views in the state.",
        required=False,
        type=str,
    )

    parser.add_argument(
        "--service_account_email_override",
        dest="service_account_email_override",
        help="The GCP service account to run this pipeline that is not the default. You probably do not need to set it.",
        required=False,
        type=str,
    )

    return parser.parse_args()


def build_sandbox_ingest_pipeline_params(
    args: argparse.Namespace,
) -> IngestPipelineParameters:
    """
    Returns PipelineParams built from the given command-line args,
    as well as parameters inferred from the state code, instance, and sandbox prefix.
    """
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=args.state_code.value
    )

    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    launchable_ingest_views = ingest_manifest_collector.launchable_ingest_views(
        ingest_instance=args.ingest_instance
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

    with local_project_id_override(args.project), cloudsql_proxy_control.connection(
        schema_type=SchemaType.OPERATIONS
    ), SessionFactory.for_proxy(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
    ) as session:
        raw_file_metadata_manager = DirectIngestRawFileMetadataManager(
            args.state_code.value, args.ingest_instance
        )
        raw_data_max_upper_bounds = raw_file_metadata_manager.get_max_update_datetimes(
            session
        )

    right_now = datetime.now()
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
    params = IngestPipelineParameters(
        # TODO(#18108): Once we have a distinct entrypoint for each pipeline type, we
        #  likely won't need this arg.
        pipeline=INGEST_PIPELINE_NAME,
        project=args.project,
        state_code=args.state_code.value,
        ingest_instance=args.ingest_instance.value,
        output_sandbox_prefix=args.output_sandbox_prefix,
        ingest_views_to_run=args.ingest_views_to_run,
        raw_data_upper_bound_dates_json=raw_data_upper_bound_dates_json,
        sandbox_username=get_sandbox_pipeline_username(),
        service_account_email_override=args.service_account_email_override,
    )

    return params


def run_sandbox_ingest_pipeline(
    params: IngestPipelineParameters, skip_build: bool
) -> None:
    output_sandbox_prefix = params.output_sandbox_prefix
    if not output_sandbox_prefix:
        raise ValueError("Must specify an --output_sandbox_prefix")

    logging.info(
        "Using raw data watermarks from latest run: %s",
        params.raw_data_upper_bound_dates_json,
    )

    prompt_for_confirmation(
        f"Starting ingest pipeline [{params.job_name}] for [{params.state_code}] in "
        f"[{params.project}] which will output to datasets "
        f"[{params.ingest_view_results_output}] and [{params.output}] - continue?"
    )

    bq_client = BigQueryClientImpl()
    create_or_update_ingest_output_sandbox(
        bq_client,
        StateCode(params.state_code),
        DirectIngestInstance(params.ingest_instance),
        output_sandbox_prefix,
        allow_overwrite=True,
    )

    run_sandbox_dataflow_pipeline(params, skip_build)


def main() -> None:
    """Creates sandbox datasets (as appropriate) and launches a sandbox ingest
    pipline as specified by the script args.
    """
    known_args = parse_run_arguments()
    params = build_sandbox_ingest_pipeline_params(known_args)
    with local_project_id_override(params.project):
        run_sandbox_ingest_pipeline(params, skip_build=known_args.skip_build)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
