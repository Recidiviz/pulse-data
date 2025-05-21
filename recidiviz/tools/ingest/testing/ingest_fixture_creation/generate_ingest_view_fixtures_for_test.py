# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
This script downloads raw data for a given ingest view,
randomizes or encrypts certain data, and then stores that
data in CSV files for fixtures.

Usage:
python -m recidiviz.tools.ingest.testing.ingest_fixture_creation.generate_ingest_view_fixtures_for_test \
    --state_code US_MO \
    --external_id_type US_MO_DOC \
    --external_id_value 12345 \
    --ingest_view_name sentence \
    --test_characteristic revocation \
    [--files_to_make_empty <file tags to skip>]

files_to_make_empty should be a last resort when an existing ingest view
needs new test cases, but the logic connecting raw dependencies isn't
clean enough to connect your specific test case. Please try updating
the ingest view and raw data dependencies first!

If you are adding an ingest view test to a ingest view with existing code files and want
to skip re-generating those code files, pass the skip_code_files flag like

python -m recidiviz.tools.ingest.testing.ingest_fixture_creation.generate_ingest_view_fixtures_for_test \
    --state_code US_MO \
    --external_id_type US_MO_DOC \
    --external_id_value 12345 \
    --ingest_view_name sentence \
    --test_characteristic revocation \
    --skip_code_files


"""
import argparse
import os

from tabulate import tabulate

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.external_id_type_helpers import (
    external_id_types_by_state_code,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    fixture_path_for_raw_data_dependency,
)
from recidiviz.tests.ingest.direct.raw_data_fixture import RawDataFixture
from recidiviz.tools.ingest.testing.ingest_fixture_creation.fixture_pruning import (
    prune_fixture_data,
)
from recidiviz.tools.ingest.testing.ingest_fixture_creation.pull_root_entity_filtered_raw_data_for_ingest_view import (
    build_root_entity_filtered_raw_data_queries,
)
from recidiviz.tools.ingest.testing.ingest_fixture_creation.randomize_fixture_data import (
    randomize_fixture_data,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

_SKIP_CODE_FILE_MESSAGE = "SKIPPING ALREADY DOWNLOADED CODE FILE"
_EMPTY_FIXTURE_MESSAGE = "WRITING EMPTY FIXTURE FILE"


def _parse_args() -> argparse.Namespace:
    """Reads in command line arguments and validates them."""
    parser = argparse.ArgumentParser(
        description="Generate raw data fixtures for a given ingest view."
    )
    parser.add_argument(
        "--project_id",
        type=str,
        default=GCP_PROJECT_STAGING,
        help="The GCP project ID to query raw data from.",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
    )
    parser.add_argument(
        "--state_code",
        type=StateCode,
        choices=list(StateCode),
        required=True,
        help="The state code for the region (e.g., 'US_AZ').",
    )
    parser.add_argument(
        "--ingest_view_name",
        type=str,
        required=True,
        help="The name of the ingest view (e.g., 'state_sentence').",
    )
    parser.add_argument(
        "--external_id_type",
        type=str,
        required=True,
        help="The external ID type to filter on (e.g., 'US_AZ_PERSON_ID').",
    )
    parser.add_argument(
        "--external_id_value",
        type=str,
        required=True,
        help="The external ID value to filter on (e.g., '12345').",
    )
    parser.add_argument(
        "--test_characteristic",
        type=str,
        required=True,
        help=(
            "The thing or concept you are testing. It is a characteristic of the person whose data you're pulling into fixture files. "
            "The test characteristic is used in ingest test names as 'test_<view_name>__for__<test_characteristic>'. "
            "It is also the file name of the generated CSVs. "
            "For example, if you are testing the sentence ingest view and the test characteristic is 'revocation', "
            "the test name will be test_sentence__for__revocation the generated CSVs will be named 'revocation.csv'."
        ),
    )
    # By default, we do not skip code files and will regenerate them each time.
    # Passing this option at the commandline will skip querying and writing code
    # files if they already exist at the expected fixture path.
    parser.add_argument(
        "--skip_code_files",
        action="store_true",
        help="If this flag is included, the script will skip code files if they already exist.",
    )
    parser.add_argument(
        "--files_to_make_empty",
        nargs="+",
        required=False,
        help=(
            "Space separated list of file tags to NOT query, resulting in an empty fixture. "
            "Please only use this as a last resort!"
        ),
    )
    args = parser.parse_args()

    state_id_types = external_id_types_by_state_code()[args.state_code]
    if args.external_id_type not in state_id_types:
        raise ValueError(
            f"Invalid external ID type: {args.external_id_type}. "
            f"StateCode {args.state_code.value.upper()} supports: "
            f"{', '.join(state_id_types)}."
        )
    return args


def _validate_and_preview_external_id(
    project_id: str,
    state_code: StateCode,
    ingest_view_name: str,
    external_id_type: str,
    external_id_value: str,
) -> DirectIngestViewQueryBuilder:
    """
    Checks if the external ID type is valid for the given ingest view and state code.
    Returns the view builder for the ingest view.
    """
    region = direct_ingest_regions.get_direct_ingest_region(state_code.value)
    view_collector = DirectIngestViewQueryBuilderCollector.from_state_code(state_code)
    mapping_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )

    view_builder = view_collector.get_query_builder_by_view_name(ingest_view_name)
    mapping = mapping_collector.ingest_view_to_manifest[ingest_view_name]
    if external_id_type not in mapping.root_entity_external_id_types:
        raise ValueError(
            f"External ID type {external_id_type} not expected for ingest view {ingest_view_name}. "
            f"Expected external ID types: {mapping.root_entity_external_id_types}"
        )

    _grid = tabulate(
        [
            [
                "Project",
                "State Code",
                "Ingest View Name",
                "External ID Type",
                "External ID Value",
            ],
            [
                project_id,
                state_code.value.upper(),
                ingest_view_name,
                external_id_type,
                external_id_value,
            ],
        ],
        headers="firstrow",
        tablefmt="heavy_grid",
    )
    _ = prompt_for_confirmation(
        f"\n\n{_grid}\n\nGenerate raw data fixtures for the following?\n"
    )
    return view_builder


def _validate_and_preview_file_paths(
    state_code: StateCode,
    view_builder: DirectIngestViewQueryBuilder,
    test_characteristic: str,
    skip_code_files: bool,
) -> dict[str, str]:
    """
    Checks if any code files need to be overwritten/skipped and prompts the user to
    confirm fixture file paths.

    Returns any fixtures we will skip generating.
    """
    file_paths_to_write = {}
    files_to_skip = {}
    dependencies = view_builder.raw_table_dependency_configs_by_file_tag
    for file_tag, dependency in dependencies.items():
        file_path = fixture_path_for_raw_data_dependency(
            state_code, dependency, test_characteristic
        )
        if (dependency.is_code_file and skip_code_files) and os.path.exists(file_path):
            files_to_skip[file_tag] = _SKIP_CODE_FILE_MESSAGE
        else:
            file_paths_to_write[file_tag] = file_path
    _grid = tabulate(
        [
            ["Raw Data Table", "File Path"],
            *file_paths_to_write.items(),
            *files_to_skip.items(),
        ],
        headers="firstrow",
        tablefmt="heavy_grid",
        maxcolwidths=[30, 100],
    )
    _ = prompt_for_confirmation(
        f"\n\n{_grid}\n\nWrite fixtures to the following locations?\n"
    )
    return files_to_skip


def _validate_and_preview_queries(
    view_builder: DirectIngestViewQueryBuilder,
    external_id_type: str,
    external_id_value: str,
    dataset: str,
    project_id: str,
    file_tags_to_skip_with_reason: dict[str, str],
) -> dict[str, str]:
    """Builds and previews the queries for the raw data fixtures."""
    queries = build_root_entity_filtered_raw_data_queries(
        view_builder,
        external_id_type,
        external_id_value,
        dataset,
        project_id,
        file_tags_to_skip_with_reason,
    )
    _grid = tabulate(
        [
            ["Raw Data Table", "Query"],
            *[[ft, query] for ft, query in queries.items()],
        ],
        headers="firstrow",
        tablefmt="heavy_grid",
        maxcolwidths=[30, 100],
    )
    _ = prompt_for_confirmation(
        f"\n\n{_grid}\n\nGenerate raw data fixtures from these queries?\n"
    )
    return queries


def main() -> None:
    """Generates raw data fixtures from command line arguments."""
    args = _parse_args()
    if args.files_to_make_empty is not None:
        _ = prompt_for_confirmation(
            f"\n\nThe following files will be made empty: {args.files_to_make_empty}. Are you sure!?!?"
        )
    view_builder = _validate_and_preview_external_id(
        args.project_id,
        args.state_code,
        args.ingest_view_name,
        args.external_id_type,
        args.external_id_value,
    )
    dataset = raw_tables_dataset_for_region(
        args.state_code, DirectIngestInstance.PRIMARY
    )
    fixtures_to_skip_queries = _validate_and_preview_file_paths(
        args.state_code,
        view_builder,
        args.test_characteristic,
        args.skip_code_files,
    )
    if args.files_to_make_empty is not None:
        for file_tag in args.files_to_make_empty:
            fixtures_to_skip_queries[file_tag] = _EMPTY_FIXTURE_MESSAGE
    queries = _validate_and_preview_queries(
        view_builder,
        args.external_id_type,
        args.external_id_value,
        dataset,
        args.project_id,
        fixtures_to_skip_queries,
    )
    # TODO(#39686) No longer prompt when encrypted PII is in configs.
    _ = prompt_for_confirmation(
        "\n\nHave you recorded this information in our tracker here? ---> https://go/fixture-pii"
    )
    bq_client = BigQueryClientImpl(args.project_id)
    for (
        file_tag,
        dependency,
    ) in view_builder.raw_table_dependency_configs_by_file_tag.items():
        query = queries[file_tag]
        if query == _SKIP_CODE_FILE_MESSAGE:
            continue
        fixture = RawDataFixture(dependency)
        if query == _EMPTY_FIXTURE_MESSAGE:
            fixture.write_empty_fixture(args.test_characteristic)
        else:
            query_job = bq_client.run_query_async(
                query_str=query,
                use_query_cache=True,
            )
            df = query_job.result().to_dataframe()
            df = randomize_fixture_data(df, dependency.raw_file_config)
            if not dependency.is_code_file:
                if dependency.raw_file_config.no_valid_primary_keys:
                    prompt_for_confirmation(
                        f"\n\nThe raw data table {dependency.file_tag} does not have a primary key. "
                        "This fixture will use ALL columns in the config to drop duplicate rows across file_ids. "
                        "Are you sure you want to continue?"
                    )
                    pks = [c.name for c in dependency.raw_file_config.current_columns]
                else:
                    pks = dependency.raw_file_config.primary_key_cols
                df = prune_fixture_data(df, pks)
            fixture.write_dataframe_into_fixture_file(
                df,
                args.test_characteristic,
            )


if __name__ == "__main__":
    main()
