# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""This can be used to test that all ingest view results can be properly parsed using
the current mappings.

Note, this only looks at the ingest view run against latest raw data, so if there is
bad raw data that has been overwritten by a newer version of the row, that will be
missed.

Example Usage:
    python -m recidiviz.tools.ingest.development.run_mappings_against_ingest_view_results \
        --project-id recidiviz-staging \
        --state-code US_ND \
        --ingest-view-name elite_alias
"""

import argparse
import logging
import os
import tempfile
import traceback
from datetime import datetime
from typing import Dict, Union

from tqdm import tqdm

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.common.constants import states
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegateImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    to_string_value_converter,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import print_entity_tree
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import get_closest_string


def _get_ingest_view(
    region: direct_ingest_regions.DirectIngestRegion, ingest_view_name: str
) -> DirectIngestViewQueryBuilder:
    view_collector = DirectIngestViewQueryBuilderCollector(region, [])

    ingest_views_by_name = {
        view.ingest_view_name: view for view in view_collector.collect_query_builders()
    }
    if ingest_view_name not in ingest_views_by_name:
        maybe_name = get_closest_string(
            search=ingest_view_name, within=ingest_views_by_name.keys()
        )
        raise ValueError(
            f"No view found with name '{ingest_view_name}'. Did you mean '{maybe_name}'?"
        )

    return ingest_views_by_name[ingest_view_name]


def query_ingest_view(
    region: direct_ingest_regions.DirectIngestRegion, ingest_view_name: str
) -> BigQueryResultsContentsHandle[str]:
    """Queries latest ingest view and returns results"""
    big_query_client = BigQueryClientImpl()
    ingest_view = _get_ingest_view(region, ingest_view_name)

    query = ingest_view.build_query(
        config=DirectIngestViewQueryBuilder.QueryStructureConfig(
            raw_data_datetime_upper_bound=None,
        ),
    )

    logging.info(
        "Running `%s` view query against latest raw data tables...", ingest_view_name
    )
    query_job = big_query_client.run_query_async(query_str=query, use_query_cache=True)

    contents_handle = BigQueryResultsContentsHandle(
        query_job,
        # Convert all values to strings for backwards compatiblity with the data we
        # read from test fixture files.
        value_converter=to_string_value_converter,
        max_expected_rows=None,
    )

    query_job.result()
    logging.info("Query completed.")

    return contents_handle


def parse_results(
    region: direct_ingest_regions.DirectIngestRegion,
    ingest_view_name: str,
    contents_handle: BigQueryResultsContentsHandle,
    write_results: bool,
) -> None:
    """Parses the ingest view results, collecting any errors and writing them to a file."""
    ingest_instance = DirectIngestInstance.PRIMARY
    results_update_datetime = datetime.now()
    manifest_compiler = IngestViewManifestCompiler(
        delegate=IngestViewManifestCompilerDelegateImpl(
            region=region, schema_type=SchemaType.STATE
        )
    )

    log_path = os.path.join(tempfile.gettempdir(), "mappings_errors.txt")
    results_path = os.path.join(tempfile.gettempdir(), "mappings_results.txt")
    logging.info("Parsing results...")
    logging.info("  logs at %s", log_path)
    if write_results:
        logging.info("  results at %s", results_path)

    progress = tqdm(total=contents_handle.query_job.result().total_rows)

    with open(log_path, "w", encoding="utf-8") as logfile, open(
        results_path, "w", encoding="utf-8"
    ) as results_file:
        num_errors = 0

        def result_processor(
            i: int, row: Dict[str, str], result: Union[Entity, Exception]
        ) -> None:
            nonlocal num_errors

            if isinstance(result, Exception):
                logging.info("Error: %s", result)

                num_errors += 1
                print(f"### Row {i}", file=logfile)
                print(f"Ingest View Result: {row}", file=logfile)
                traceback.print_exception(
                    etype=type(result),
                    value=result,
                    tb=result.__traceback__,
                    file=logfile,
                )
            elif isinstance(result, Entity) and write_results:
                print_entity_tree(result, file=results_file)
            progress.update()

        manifest_compiler.compile_manifest(
            ingest_view_name=ingest_view_name
        ).parse_contents(
            contents_iterator=contents_handle.get_contents_iterator(),
            result_callable=result_processor,
            context=IngestViewContentsContextImpl(
                ingest_instance=ingest_instance,
                results_update_datetime=results_update_datetime,
            ),
        )

        progress.close()
        if num_errors:
            logging.info(
                "Parsing completed with %d failures, see full failures at %s",
                num_errors,
                log_path,
            )
        else:
            logging.info("Parsed successfully.")


def main(
    state_code: states.StateCode, ingest_view_name: str, write_results: bool
) -> None:
    region = direct_ingest_regions.get_direct_ingest_region(state_code.value)

    contents_handle = query_ingest_view(region, ingest_view_name)
    parse_results(region, ingest_view_name, contents_handle, write_results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--project-id",
        required=True,
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
    )

    parser.add_argument(
        "--state-code",
        required=True,
        type=states.StateCode,
        choices=list(states.StateCode),
        help="The state to test.",
    )

    parser.add_argument(
        "--ingest-view-name",
        required=True,
        type=str,
        help="The ingest view to run.",
    )

    parser.add_argument(
        "--write-results",
        nargs="?",
        const=True,
        default=False,
        help="Writes parsed results to a file. This can be used to understand the "
        "results that an existing mapping will produce or edge cases to test when "
        "developing a new mapping. Note: This slows down the tool significantly.",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with local_project_id_override(args.project_id):
        if args.write_results:
            logging.warning(
                "Warning: the script will run significantly slower when "
                "--write-results is set due to the significant I/O load."
            )
        main(
            state_code=args.state_code,
            ingest_view_name=args.ingest_view_name,
            write_results=args.write_results,
        )
