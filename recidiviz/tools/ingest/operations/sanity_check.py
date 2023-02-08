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
"""Runs various sanity checks to ensure correctness in ingest for a given region.

Currently the script does the following:
* Verifies that raw data files have reasonable primary keys set.
* Verifies that ingest views are deterministic.

Usage:
python -m recidiviz.tools.ingest.operations.sanity_check \
    --project-id recidiviz-staging \
    --state-code US_ME

If you have made a change to a raw data config or ingest view and just want to validate
that change, you can filter to only checks that category (either "raw_data" or
"ingest_view"). For example:

python -m recidiviz.tools.ingest.operations.sanity_check \
    --project-id recidiviz-staging \
    --state-code US_ME \
    --category raw_data
"""

import argparse
import datetime
import enum
import sys
import uuid
from concurrent import futures
from typing import Dict, Optional, Set

import attr
from google.cloud import bigquery
from tqdm import tqdm

from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClient,
    BigQueryClientImpl,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.null_direct_ingest_view_materialization_metadata_manager import (
    NullDirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    IngestViewContentsSummary,
    InstanceIngestViewContentsImpl,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.utils import metadata
from recidiviz.utils.context import on_exit
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


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
    raw_data_source_instance: DirectIngestInstance,
) -> Set[str]:
    """Verifies that each of this state's raw files has a sufficient primary key defined

    The primary key is not sufficient if multiple rows are returned for a single primary
    key in a single file."""
    region_raw_file_config = DirectIngestRegionRawFileConfig(state_code.value)
    raw_table_dataset_id = raw_tables_dataset_for_region(
        state_code=state_code,
        instance=raw_data_source_instance,
        sandbox_dataset_prefix=None,
    )

    progress = tqdm(
        total=len(region_raw_file_config.raw_file_configs),
        desc="Verifying raw data primary keys",
    )

    job_futures = {}
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
        ) -> Optional[bigquery.QueryJob]:
            with on_exit(progress.update):
                if not raw_file_config.primary_key_cols:
                    return None

                # TODO(#18359): make it so this doesn't fail on files if we know they
                # don't have a good primary key and we always get them historically.

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
        }

    bad_key_file_tags = set()
    for f in futures.as_completed(job_futures):
        raw_file_tag = job_futures[f]
        query_job = f.result()

        if query_job is None:
            continue

        results = query_job.result()
        if results.total_rows:
            bad_key_file_tags.add(raw_file_tag)
    progress.close()
    return bad_key_file_tags


def verify_ingest_view_determinism(
    bq_client: BigQueryClient,
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    raw_data_source_instance: DirectIngestInstance,
) -> IngestViewDeterminismResult:
    """Verifies that each of this state's ingest views  are deterministic."""
    region = direct_ingest_regions.get_direct_ingest_region(state_code.value)
    dataset_prefix = f"sanity_{str(uuid.uuid4())[:6]}"

    # TODO(#10128): We can't instantiate the full controller without patching classes
    # or inventing new fakes, but all we need is the ingest view rank list so we get the
    # class and call the protected class method. Once state specific controllers are
    # simple delegates and don't inherit from base controller, we will be able to
    # instantiate the full delegate here.
    controller_cls = DirectIngestControllerFactory.get_controller_class(region=region)
    ingest_view_rank_list = (
        controller_cls._get_ingest_view_rank_list(  # pylint: disable=protected-access
            ingest_instance=ingest_instance
        )
    )

    ingest_view_contents = InstanceIngestViewContentsImpl(
        big_query_client=bq_client,
        region_code=state_code.value,
        ingest_instance=ingest_instance,
        dataset_prefix=dataset_prefix,
    )

    ingest_view_materializer = IngestViewMaterializerImpl(
        region=region,
        ingest_instance=ingest_instance,
        raw_data_source_instance=raw_data_source_instance,
        ingest_view_contents=ingest_view_contents,
        metadata_manager=NullDirectIngestViewMaterializationMetadataManager(
            region_code=state_code.value, ingest_instance=ingest_instance
        ),
        big_query_client=bq_client,
        view_collector=DirectIngestPreProcessedIngestViewCollector(
            region,
            controller_ingest_view_rank_list=ingest_view_rank_list,
        ),
        launched_ingest_views=ingest_view_rank_list,
    )

    current_datetime = datetime.datetime.now()

    progress = tqdm(
        total=len(ingest_view_rank_list),
        desc="Verifying ingest view determinism",
    )

    summary_futures: Dict[futures.Future[Optional[IngestViewContentsSummary]], str]
    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:

        def materialize_ingest_view(
            ingest_view_name: str,
        ) -> Optional[IngestViewContentsSummary]:
            with on_exit(progress.update):
                # If there are results when materializing an ingest view with the lower
                # bound and upper bound set to the same date, then it is not deterministic.
                ingest_view_materializer.materialize_view_for_args(
                    IngestViewMaterializationArgs(
                        ingest_view_name=ingest_view_name,
                        lower_bound_datetime_exclusive=current_datetime,
                        upper_bound_datetime_inclusive=current_datetime,
                        ingest_instance=ingest_instance,
                    )
                )

                return ingest_view_contents.get_ingest_view_contents_summary(
                    ingest_view_name=ingest_view_name
                )

        summary_futures = {
            executor.submit(materialize_ingest_view, ingest_view_name): ingest_view_name
            for ingest_view_name in ingest_view_rank_list
        }

    nondeterministic_views = {}
    for f in futures.as_completed(summary_futures):
        ingest_view_name = summary_futures[f]
        summary = f.result()

        if summary is None:
            raise ValueError(
                f"Expected ingest view summary. Did the `{ingest_view_contents.results_dataset()}` "
                "dataset expire during this run?"
            )

        if summary.num_unprocessed_rows:
            nondeterministic_views[ingest_view_name] = summary.num_unprocessed_rows

    progress.close()
    return IngestViewDeterminismResult(
        dataset_id=ingest_view_contents.results_dataset(),
        nondeterministic_views=nondeterministic_views,
    )


class CheckCategory(enum.Enum):
    RAW_DATA = "raw_data"
    INGEST_VIEW = "ingest_view"


def main(
    state_code: StateCode,
    categories: Set[CheckCategory],
    ingest_instance: DirectIngestInstance,
    raw_data_source_instance: DirectIngestInstance,
) -> int:
    """Runs the various ingest sanity checks and reports the results."""
    bq_client = BigQueryClientImpl()

    bad_key_file_tags = None
    if CheckCategory.RAW_DATA in categories:
        bad_key_file_tags = verify_raw_data_primary_keys(
            bq_client, state_code, raw_data_source_instance=raw_data_source_instance
        )

    determinism_result = None
    if CheckCategory.INGEST_VIEW in categories:
        determinism_result = verify_ingest_view_determinism(
            bq_client,
            state_code,
            ingest_instance=ingest_instance,
            raw_data_source_instance=raw_data_source_instance,
        )

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
        help="The state to run sanity checks for, in the form US_XX.",
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

    parser.add_argument(
        "--raw-data-source-instance",
        choices=[instance.value for instance in DirectIngestInstance],
        help="The instance from which we should read raw data.",
        default=DirectIngestInstance.PRIMARY.value,
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
                raw_data_source_instance=DirectIngestInstance(
                    args.raw_data_source_instance
                ),
            )
        )
