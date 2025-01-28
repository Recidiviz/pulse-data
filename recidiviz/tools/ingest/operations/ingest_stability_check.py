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
"""Runs various checks to ensure stability and correctness of ingest for a given region.

Currently the script does the following:
* Verifies that raw data files have reasonable primary keys set.
* Verifies that ingest views are deterministic.

Usage:
python -m recidiviz.tools.ingest.operations.ingest_stability_check \
    --project-id recidiviz-staging \
    --state-code US_ME

If you have made a change to a raw data config or ingest view and just want to validate
that change, you can filter to only checks that category (either "raw_data" or
"ingest_view"). For example:

python -m recidiviz.tools.ingest.operations.ingest_stability_check \
    --project-id recidiviz-staging \
    --state-code US_OZ \
    --category ingest_view
"""

import argparse
import datetime
import enum
import sys
import uuid
from concurrent import futures
from typing import Dict, Set

import attr
from google.cloud import bigquery
from more_itertools import one
from tqdm import tqdm

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClient,
    BigQueryClientImpl,
)
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
)
from recidiviz.utils import metadata
from recidiviz.utils.context import on_exit
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.types import assert_type


@attr.s(frozen=True, kw_only=True)
class IngestViewDeterminismResult:
    # The temporary dataset where the differences are stored
    dataset_id: str = attr.ib()

    # The set of views that are nondeterministic, along with the number of rows that
    # were different between the two runs.
    nondeterministic_views: Dict[str, int] = attr.ib()


# TODO(#18491): Use shared `FutureExecutor` to manage the parallelism.


def verify_raw_data_primary_keys(
    bq_client: BigQueryClient,
    state_code: StateCode,
    instance: DirectIngestInstance,
) -> Set[str]:
    """Verifies that each of this state's raw files has a sufficient primary key defined

    The primary key is not sufficient if multiple rows are returned for a single primary
    key in a single file."""
    region_raw_file_config = DirectIngestRegionRawFileConfig(state_code.value)
    raw_table_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code,
        instance=instance,
        sandbox_dataset_prefix=None,
    )

    progress = tqdm(
        total=len(region_raw_file_config.raw_file_configs),
        desc="Verifying raw data primary keys",
    )

    # Even though the BQ jobs run asynchronously, even starting them takes a bit so we
    # use a threadpool to start them in parallel.
    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:

        def query_raw_table(
            raw_file_tag: str, raw_file_config: DirectIngestRawFileConfig
        ) -> bigquery.QueryJob:
            with on_exit(progress.update):
                job = bq_client.run_query_async(
                    query_str=f"""
                    SELECT file_id, {raw_file_config.primary_key_str}, COUNT(*) as num
                    FROM `{metadata.project_id()}.{raw_table_dataset_id}.{raw_file_tag}`
                    GROUP BY file_id, {raw_file_config.primary_key_str}
                    HAVING num > 1;
                    """,
                    # It is fine to cache because missing very recent data should not
                    # impact the results. If someone is rapidly iterating on configs,
                    # caching makes runs for files that weren't changed much faster.
                    use_query_cache=True,
                )

                # Start the job and wait for it to finish.
                job.result()
                return job

        job_futures = {
            executor.submit(
                query_raw_table, raw_file_tag, raw_file_config
            ): raw_file_tag
            for raw_file_tag, raw_file_config in region_raw_file_config.raw_file_configs.items()
            # Don't run the query if there are no primary keys
            if raw_file_config.primary_key_cols
        }

        bad_key_file_tags = set()
        for f in futures.as_completed(job_futures):
            raw_file_tag = job_futures[f]
            query_job = f.result()

            results = query_job.result()
            if results.total_rows:
                bad_key_file_tags.add(raw_file_tag)
        progress.close()
        return bad_key_file_tags


def _materialize_twice_and_return_num_different_rows(
    bq_client: BigQueryClient,
    temp_results_dataset_id: str,
    ingest_view_query_builder: DirectIngestViewQueryBuilder,
) -> int:
    """Materializes the ingest view query for the given query builder two separate
    times, then returns the number of rows that are different between two.

    The different rows are also materialized to a separate table in the provided
    dataset.
    """
    current_datetime = datetime.datetime.now()

    view_query = ingest_view_query_builder.build_query(
        config=DirectIngestViewQueryBuilder.QueryStructureConfig(
            raw_data_datetime_upper_bound=current_datetime,
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
        )
    )

    address_1 = BigQueryAddress(
        dataset_id=temp_results_dataset_id,
        table_id=f"{ingest_view_query_builder.ingest_view_name}__1",
    ).to_project_specific_address(metadata.project_id())
    address_2 = BigQueryAddress(
        dataset_id=temp_results_dataset_id,
        table_id=f"{ingest_view_query_builder.ingest_view_name}__2",
    ).to_project_specific_address(metadata.project_id())

    bq_client.insert_into_table_from_query(
        destination_address=BigQueryAddress(
            dataset_id=address_1.dataset_id, table_id=address_1.table_id
        ),
        query=view_query,
        use_query_cache=False,
    )
    bq_client.insert_into_table_from_query(
        destination_address=BigQueryAddress(
            dataset_id=address_2.dataset_id, table_id=address_2.table_id
        ),
        query=view_query,
        use_query_cache=False,
    )

    diff_address = BigQueryAddress(
        dataset_id=temp_results_dataset_id,
        table_id=f"{ingest_view_query_builder.ingest_view_name}__diff",
    ).to_project_specific_address(metadata.project_id())
    diff_query = (
        f"{address_1.select_query()} EXCEPT DISTINCT ({address_2.select_query()});"
    )

    bq_client.insert_into_table_from_query(
        destination_address=diff_address.to_project_agnostic_address(),
        query=diff_query,
        use_query_cache=False,
    )

    count_query = (
        f"""SELECT COUNT(*) AS cnt FROM {diff_address.format_address_for_query()};"""
    )

    return one(
        bq_client.run_query_async(query_str=count_query, use_query_cache=False).result()
    )["cnt"]


def verify_ingest_view_determinism(
    bq_client: BigQueryClient, state_code: StateCode
) -> IngestViewDeterminismResult:
    """Verifies that each of this state's ingest views are deterministic."""
    region = direct_ingest_regions.get_direct_ingest_region(state_code.value)
    dataset_prefix = f"stability_{str(uuid.uuid4())[:6]}"

    temp_results_dataset_id = ingest_view_materialization_results_dataset(
        state_code=state_code, sandbox_dataset_prefix=dataset_prefix
    )
    bq_client.create_dataset_if_necessary(
        temp_results_dataset_id,
        default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )

    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    launched_ingest_views = ingest_manifest_collector.launchable_ingest_views(
        # Since this is a script run locally, we use the context for the specified
        # project so that we don't include local-only views.
        IngestViewContentsContextImpl.build_for_project(
            project_id=metadata.project_id()
        ),
    )

    view_query_builders = DirectIngestViewQueryBuilderCollector(
        region,
        expected_ingest_views=launched_ingest_views,
    ).get_query_builders()

    progress = tqdm(
        total=len(launched_ingest_views),
        desc="Verifying ingest view determinism",
    )

    num_diff_rows_futures: Dict[futures.Future[int], str]
    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:

        def materialize_ingest_view(
            ingest_view_query_builder: DirectIngestViewQueryBuilder,
        ) -> int:
            with on_exit(progress.update):
                return _materialize_twice_and_return_num_different_rows(
                    bq_client=bq_client,
                    temp_results_dataset_id=temp_results_dataset_id,
                    ingest_view_query_builder=ingest_view_query_builder,
                )

        num_diff_rows_futures = {
            executor.submit(
                materialize_ingest_view, query_builder
            ): query_builder.ingest_view_name
            for query_builder in view_query_builders
        }

    nondeterministic_views = {}
    for f in futures.as_completed(num_diff_rows_futures):
        ingest_view_name = num_diff_rows_futures[f]
        different_rows_count = assert_type(f.result(), int)

        if different_rows_count:
            nondeterministic_views[ingest_view_name] = different_rows_count

    progress.close()
    return IngestViewDeterminismResult(
        dataset_id=temp_results_dataset_id,
        nondeterministic_views=nondeterministic_views,
    )


class CheckCategory(enum.Enum):
    RAW_DATA = "raw_data"
    INGEST_VIEW = "ingest_view"


def main(
    state_code: StateCode,
    categories: Set[CheckCategory],
    ingest_instance: DirectIngestInstance,
) -> int:
    """Runs the various ingest stability checks and reports the results."""
    bq_client = BigQueryClientImpl()

    bad_key_file_tags = None
    if CheckCategory.RAW_DATA in categories:
        bad_key_file_tags = verify_raw_data_primary_keys(
            bq_client, state_code, ingest_instance
        )

    determinism_result = None
    if CheckCategory.INGEST_VIEW in categories:
        determinism_result = verify_ingest_view_determinism(bq_client, state_code)

    any_failures = False

    if bad_key_file_tags:
        any_failures = True

        print(f"\nFound {len(bad_key_file_tags)} files with insufficient primary keys:")
        for raw_file_tag in sorted(bad_key_file_tags):
            print(f"- {raw_file_tag}")

    if determinism_result and determinism_result.nondeterministic_views:
        any_failures = True

        print(
            f"\nFound {len(determinism_result.nondeterministic_views)} non deterministic ingest views:"
        )
        for ingest_view_name in sorted(determinism_result.nondeterministic_views):
            print(
                f"- {ingest_view_name}: "
                f"{determinism_result.nondeterministic_views[ingest_view_name]} rows "
                "were different"
            )
        print(
            f"See differences in the [{determinism_result.dataset_id}] temporary dataset."
        )

    if any_failures:
        print("\n\N{CROSS MARK} At least now we know? \N{SHRUG}")
        return 1
    print("\n\N{WHITE HEAVY CHECK MARK} All good!")
    return 0


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        required=True,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="The project to pull data from, e.g. 'recidiviz-staging'",
    )

    parser.add_argument(
        "--state-code",
        help="The state to run ingest stability checks for, in the form US_XX.",
        type=str,
        choices=[state.value for state in StateCode],
        required=True,
    )

    parser.add_argument(
        "--category",
        help="The set of checks to run, defaults to all.",
        choices=[category.value for category in CheckCategory],
        nargs="+",
        default=[category.value for category in CheckCategory],
        required=False,
    )

    parser.add_argument(
        "--ingest-instance",
        choices=[instance.value for instance in DirectIngestInstance],
        help="The instance into which we are ingesting. Defaults to secondary so that "
        "it runs the most up to date version of each ingest view.",
        default=DirectIngestInstance.SECONDARY.value,
        required=False,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    with metadata.local_project_id_override(GCP_PROJECT_STAGING):
        sys.exit(
            main(
                StateCode(args.state_code),
                {CheckCategory(category) for category in args.category},
                ingest_instance=DirectIngestInstance(args.ingest_instance),
            )
        )
