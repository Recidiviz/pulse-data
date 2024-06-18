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
"""
Update BigQuery table schemas during deployment
"""
import argparse
import logging
import os
from pprint import pprint

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_view_source_table_configs,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    RawDataSourceTableLabel,
    SchemaTypeSourceTableLabel,
    UnionedStateAgnosticSourceTableLabel,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.source_table_update_manager import (
    SourceTableCollectionUpdateConfig,
    SourceTableUpdateManager,
)
from recidiviz.tools.deploy.logging import get_deploy_logs_dir
from recidiviz.tools.utils.script_helpers import interactive_prompt_retry_on_exception
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def build_source_table_collection_update_configs(
    source_table_repository: SourceTableRepository,
    bq_client: BigQueryClient,
) -> list[SourceTableCollectionUpdateConfig]:
    """Builds a list of update configs"""
    # TODO(#30356): Add static yaml tables to this list
    update_configs = [
        SourceTableCollectionUpdateConfig(
            source_table_collection=source_table_collection,
            allow_field_deletions=False,
        )
        for source_table_collection in source_table_repository.collections_labelled_with(
            RawDataSourceTableLabel
        )
    ]

    update_configs.extend(
        SourceTableCollectionUpdateConfig(
            source_table_collection=source_table_collection,
            allow_field_deletions=True,
        )
        for source_table_collection in source_table_repository.collections_labelled_with(
            DataflowPipelineSourceTableLabel
        )
    )

    update_configs.extend(
        SourceTableCollectionUpdateConfig(
            source_table_collection=source_table_collection,
            allow_field_deletions=True,
        )
        for source_table_collection in source_table_repository.collections_labelled_with(
            UnionedStateAgnosticSourceTableLabel
        )
    )

    update_configs.extend(
        SourceTableCollectionUpdateConfig(
            source_table_collection=source_table_collection,
            allow_field_deletions=True,
        )
        for source_table_collection in source_table_repository.collections_labelled_with(
            SchemaTypeSourceTableLabel
        )
    )

    # TODO(#30495): These will not need to be added separately once ingest views define
    #  their schemas in the YAML mappings definitions and we can collect these ingest
    #  view tables with all the other source tables.
    logging.info("Building source table configs for ingest views...")
    update_configs.extend(
        SourceTableCollectionUpdateConfig(
            source_table_collection=source_table_collection,
            allow_field_deletions=True,
            recreate_on_update_error=True,
        )
        for source_table_collection in build_ingest_view_source_table_configs(
            bq_client=bq_client,
            state_codes=get_direct_ingest_states_existing_in_env(),
        )
    )

    return update_configs


def update_all_source_table_schemas(
    source_table_repository: SourceTableRepository,
    update_manager: SourceTableUpdateManager | None = None,
    *,
    dry_run: bool = True,
) -> None:
    """Given a repository of source tables, update BigQuery to match"""
    if not update_manager:
        update_manager = SourceTableUpdateManager(client=BigQueryClientImpl())

    update_configs = build_source_table_collection_update_configs(
        source_table_repository=source_table_repository, bq_client=update_manager.client
    )

    def _retry_fn() -> None:
        if dry_run:
            results = update_manager.dry_run(
                update_configs=update_configs,
                log_file=os.path.join(
                    get_deploy_logs_dir(), "update_all_source_table_schemas.log"
                ),
            )
            pprint(results)
        else:
            update_manager.update_async(
                update_configs=update_configs,
                log_file=os.path.join(
                    get_deploy_logs_dir(), "update_all_source_table_schemas.log"
                ),
            )

    interactive_prompt_retry_on_exception(
        input_text="Exception encountered when updating source table schemas - retry?",
        accepted_response_override="yes",
        exit_on_cancel=True,
        fn=_retry_fn,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
    )
    args, _ = parser.parse_known_args()
    with local_project_id_override(args.project_id):
        repository = build_source_table_repository_for_collected_schemata()

        update_all_source_table_schemas(
            source_table_repository=repository,
            dry_run=args.dry_run,
        )
