# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared helpers for `run_sandbox_*_ingest_pipeline` scripts."""
import argparse
import json
from datetime import datetime

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.pipelines.ingest.ingest_pipeline_type_utils import (
    manifest_compiler_delegate_for_pipeline_type,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_sandbox_pipeline_username,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def common_sandbox_argument_parser() -> argparse.ArgumentParser:
    """Returns an `ArgumentParser` populated with the args shared by every
    `run_sandbox_*_ingest_pipeline` script. Each script extends this with its
    own keying argument (`--state_code` for activity, `--tenant` for identity)
    before parsing.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project",
        type=str,
        help="ID of the GCP project.",
        choices=DATA_PLATFORM_GCP_PROJECTS,
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

    return parser


def get_raw_data_upper_bound_dates_json_for_sandbox_pipeline(
    *,
    project_id: str,
    state_code: StateCode,
    raw_data_source_instance: DirectIngestInstance,
    ingest_pipeline_type: IngestPipelineType,
) -> str:
    """Builds JSON describing the raw data upper bound dates that should be used for a
    sandbox ingest pipeline of the given `ingest_pipeline_type`.

    Will crash if a `local_project_id_override` is set already.
    """
    right_now = datetime.now()

    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=manifest_compiler_delegate_for_pipeline_type(
            region=region, ingest_pipeline_type=ingest_pipeline_type
        ),
        ingest_pipeline_type=ingest_pipeline_type,
    )
    launchable_ingest_views = ingest_manifest_collector.launchable_ingest_views(
        # Use the context for the project we will be running against, not the one that
        # corresponds to the local environment so that we don't identify local-only
        # views as launchable here.
        IngestViewContentsContext.build_for_project(
            project_id=project_id,
            is_sandbox=False,
            state_code=state_code,
        ),
    )
    view_collector = DirectIngestViewQueryBuilderCollector(
        region=region,
        ingest_pipeline_type=ingest_pipeline_type,
        expected_ingest_views=launchable_ingest_views,
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


def build_extra_pipeline_parameter_args(
    *,
    project: str,
    state_code: StateCode,
    raw_data_source_instance: DirectIngestInstance,
    ingest_pipeline_type: IngestPipelineType,
) -> list[str]:
    """Returns the shared portion of the additional pipeline command-line args
    that can be inferred from the project, state code, instance, and
    `ingest_pipeline_type`.

    Does not include the per-pipeline keying arg (`--state_code` for activity,
    `--tenant` for identity). Callers must pass that separately.
    """
    raw_data_upper_bound_dates_json = (
        get_raw_data_upper_bound_dates_json_for_sandbox_pipeline(
            project_id=project,
            state_code=state_code,
            raw_data_source_instance=raw_data_source_instance,
            ingest_pipeline_type=ingest_pipeline_type,
        )
    )

    return [
        # TODO(#18108): Once we have a distinct entrypoint for each pipeline type, we
        # likely won't need this arg.
        "--pipeline",
        ingest_pipeline_type.pipeline_name,
        "--project",
        project,
        "--raw_data_source_instance",
        raw_data_source_instance.value,
        "--raw_data_upper_bound_dates_json",
        raw_data_upper_bound_dates_json,
        "--sandbox_username",
        get_sandbox_pipeline_username(),
    ]
