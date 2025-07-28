# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
This script can be used to load the raw data latest views for a state based on local
raw data configuration YAML files. This script by default will load ALL views and ALL
columns, regardless of whether any of the columns contain documentation. This can be
used to allow you to explore raw data that has been uploaded before you understand
enough to document the column so that you can use it in a downstream view.

Basic usage:
python -m recidiviz.tools.load_raw_data_latest_views_to_sandbox \
   --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
   --state_code US_OZ

To load what will actually be loaded as part of our normal view deploy:
python -m recidiviz.tools.load_raw_data_latest_views_to_sandbox \
   --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
   --state_code US_OZ \
   --exclude_undocumented_views_and_columns
"""
import argparse
import logging

from more_itertools import one

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewCollector,
)
from recidiviz.tools.load_views_to_sandbox import load_collected_views_to_sandbox
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def _load_raw_data_latest_views_to_sandbox(
    *,
    sandbox_dataset_prefix: str,
    state_code: StateCode,
    raw_data_source_instance: DirectIngestInstance,
    exclude_undocumented_views_and_columns: bool,
) -> None:
    """Load the raw data latest views for the given state to a sandbox."""

    print("Collecting all latest raw data view builders...")
    all_builders = DirectIngestRawDataTableLatestViewCollector(
        region_code=state_code.value.lower(),
        raw_data_source_instance=raw_data_source_instance,
        filter_to_documented=exclude_undocumented_views_and_columns,
    ).collect_view_builders()

    bq_client = BigQueryClientImpl()

    sandbox_context = BigQueryViewSandboxContext(
        parent_address_overrides=None,
        parent_address_formatter_provider=None,
        output_sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    print("Filtering tables that don't exist or can't produce a valid query...")
    raw_data_table_to_builder = {}

    for builder in sorted(all_builders, key=lambda b: b.address.to_str()):
        parent_tables = builder.build(sandbox_context=sandbox_context).parent_tables
        # We assume each view only queries from exactly one raw data table
        parent_raw_data_table = one(parent_tables)
        raw_data_table_to_builder[parent_raw_data_table] = builder

    raw_tables_dataset_id = one({a.dataset_id for a in raw_data_table_to_builder})

    existing_raw_data_tables = {
        ProjectSpecificBigQueryAddress.from_list_item(
            table_ref
        ).to_project_agnostic_address()
        for table_ref in bq_client.list_tables(raw_tables_dataset_id)
    }

    builders_to_load: list[BigQueryViewBuilder] = []
    for raw_data_table, builder in raw_data_table_to_builder.items():
        if not builder.has_valid_query():
            print(
                f"!! Skipping load of view [{builder.address.to_str()}] because the "
                f"config for raw data table [{raw_data_table.to_str()}] does not have "
                f"enough information to build a query (e.g. no primary key columns "
                f"defined).",
            )
            continue
        if raw_data_table not in existing_raw_data_tables:
            print(
                f"!! Skipping load of view [{builder.address.to_str()}] because parent "
                f"raw data table [{raw_data_table.to_str()}] does not yet exist "
                f"in [{metadata.project_id()}].",
            )
            continue
        builders_to_load.append(builder)

    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        state_code_filter=None,
        collected_builders=builders_to_load,
        # These views will always read from the standard raw data dataset for the
        # given raw_data_source_instance.
        input_source_table_dataset_overrides_dict=None,
        # We don't expect any of the latest views to be slow
        allow_slow_views=False,
        materialize_changed_views_only=False,
        # We're loading a view graph of depth 1 -- let's load all views in order to
        # surface all errors we could run into
        failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
    )


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        required=False,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="A prefix to append to all names of the datasets where these views will "
        "be loaded.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--state_code",
        dest="state_code",
        help="The state code to load latest views for.",
        type=StateCode,
        required=True,
    )

    parser.add_argument(
        "--raw_data_source_instance",
        help=(
            "The raw data instance for the raw data dataset that views should read "
            "from."
        ),
        type=DirectIngestInstance,
        required=False,
        default=DirectIngestInstance.PRIMARY,
    )

    parser.add_argument(
        "--exclude_undocumented_views_and_columns",
        dest="exclude_undocumented_views_and_columns",
        action="store_true",
        default=False,
        help="If set, we will only load views that are documented (have at least one "
        "documented column). Otherwise, this script loads ALL views with ALL "
        "columns, regardless of whether they are documented.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments()

    with local_project_id_override(args.project_id):
        _load_raw_data_latest_views_to_sandbox(
            sandbox_dataset_prefix=args.sandbox_dataset_prefix,
            state_code=args.state_code,
            raw_data_source_instance=args.raw_data_source_instance,
            exclude_undocumented_views_and_columns=args.exclude_undocumented_views_and_columns,
        )
