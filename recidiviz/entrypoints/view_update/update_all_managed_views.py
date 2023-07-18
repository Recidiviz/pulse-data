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
"""Script for manage view updates to occur - to be called only within the Airflow DAG's
KubernetesPodOperator."""
import argparse
import logging

from recidiviz.big_query.view_update_manager import execute_update_all_managed_views
from recidiviz.utils.metadata import project_id
from recidiviz.utils.params import str_to_list


def parse_arguments() -> argparse.Namespace:
    """Parses arguments for the managed views update process."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--sandbox_prefix",
        help="The sandbox prefix for which the refresh needs to write to",
        type=str,
    )

    parser.add_argument(
        "--dataset_ids_to_load",
        dest="dataset_ids_to_load",
        help="A list of dataset_ids to load separated by commas. If provided, only "
        "loads datasets in this list plus ancestors.",
        type=str_to_list,
        required=False,
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    args = parse_arguments()

    execute_update_all_managed_views(
        project_id(), args.sandbox_prefix, args.dataset_ids_to_load
    )
