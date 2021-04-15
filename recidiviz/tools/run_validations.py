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
This script runs locally-defined validations and determines which ones succeed
and which ones fail.

NOTE: This only picks up changes to the text of the validation queries. If you make
changes to sub-views, this will not be cognizant of any changes made there that
are not yet uploaded.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.run_validations \
    --project-id recidiviz-staging \
    --region-code-filter [region_code] \
    --validation-name-filter [regex]
"""
import argparse
import logging
import re
from typing import Optional

from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.validation_manager import execute_validation


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(description="Run locally-defined valications.")
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--region-code-filter",
        default=None,
        help="Region code filter - when set, will only limit validations to the specified region.",
    )
    parser.add_argument(
        "--validation-name-filter",
        default=None,
        help="Regex name filter - when set, will only run validations with names that match this regex.",
    )
    return parser


def main(
    region_code_filter: Optional[str], validation_name_filter: Optional[str]
) -> None:
    validation_regex = (
        re.compile(validation_name_filter) if validation_name_filter else None
    )
    execute_validation(
        rematerialize_views=False,
        region_code_filter=region_code_filter,
        validation_name_filter=validation_regex,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(args.region_code_filter, args.validation_name_filter)
