# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
Script for loading `*latest_views` to a sandbox dataset.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.development.load_raw_data_latest_views_to_sandbox \
    --project_id recidiviz-staging \
    --state_code US_ND \
    --views_sandbox_dataset_prefix my_prefix \
    [--raw_tables_sandbox_dataset_prefix my_other_prefix]
"""
import argparse
import logging
import sys
from typing import List

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_data_table_latest_view_updater import (
    DirectIngestRawDataTableLatestViewUpdater,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def main(
    state_code: StateCode,
    raw_tables_sandbox_dataset_prefix: str,
    views_sandbox_dataset_prefix: str,
) -> None:
    """Executes the main flow of the script."""
    bq_client = BigQueryClientImpl(project_id=metadata.project_id())
    controller = DirectIngestRawDataTableLatestViewUpdater(
        state_code=state_code.value.lower(),
        bq_client=bq_client,
        raw_tables_sandbox_dataset_prefix=raw_tables_sandbox_dataset_prefix,
        views_sandbox_dataset_prefix=views_sandbox_dataset_prefix,
    )
    controller.update_views_for_state()


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the named arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state_code",
        help="State to which this config belongs in the form US_XX.",
        type=StateCode,
        choices=list(StateCode),
        required=True,
    )

    parser.add_argument(
        "--project_id",
        help="Which project to read raw data from.",
        type=str,
        choices=GCP_PROJECTS,
        required=True,
    )

    example_output_dataset = raw_latest_views_dataset_for_region(
        region_code="us_xx", sandbox_dataset_prefix="my_prefix"
    )
    parser.add_argument(
        "--views_sandbox_dataset_prefix",
        help=f"Prefix of sandbox dataset to write views to. For state US_XX and prefix "
        f"'my_prefix', views will be written to {example_output_dataset}.",
        type=str,
        required=True,
    )

    example_input_dataset = raw_tables_dataset_for_region(
        region_code="us_xx", sandbox_dataset_prefix="my_prefix"
    )
    parser.add_argument(
        "--raw_tables_sandbox_dataset_prefix",
        help=f"Prefix of sandbox dataset to read raw data from to. For state US_XX and "
        f"prefix 'my_prefix', data will be read from {example_input_dataset}.",
        type=str,
        required=False,
        default=None,
    )

    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    args = parse_arguments(sys.argv[1:])
    with local_project_id_override(args.project_id):
        main(
            state_code=args.state_code,
            raw_tables_sandbox_dataset_prefix=args.raw_tables_sandbox_dataset_prefix,
            views_sandbox_dataset_prefix=args.views_sandbox_dataset_prefix,
        )
