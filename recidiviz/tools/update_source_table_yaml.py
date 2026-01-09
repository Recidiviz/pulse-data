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
This script writes BigQuery table schemas to YAML files in our recidiviz/source_tables/schema directory

Example usage:

# Update the YAML definitions for referenced tables in the `static_reference_tables` dataset
uv run python -m recidiviz.tools.update_source_table_yaml \
    --dataset-id static_reference_tables \
    --project-id [project id]

# Update the YAML definitions for 2 tables in the `static_reference_tables` dataset
python -m recidiviz.tools.update_source_table_yaml \
    --dataset-id static_reference_tables \
    --table-ids state_ids,state_county_codes \
    --project-id [project id]
"""
import argparse
import logging
import os.path

import yaml

import recidiviz
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_list
from recidiviz.view_registry.deployed_views import all_deployed_view_builders


def update_source_file_yaml(table_address: BigQueryAddress) -> None:
    dataset_path = os.path.join(
        os.path.abspath(os.path.dirname(recidiviz.__file__)),
        "source_tables/externally_managed",
        table_address.dataset_id,
    )

    if not os.path.exists(dataset_path):
        os.mkdir(dataset_path)

    table_yaml_path = os.path.join(
        dataset_path,
        f"{table_address.table_id}.yaml",
    )
    logging.info(
        "Updating %s for unmanaged source table %s", table_yaml_path, table_address
    )

    client = BigQueryClientImpl()
    source_table_config = SourceTableConfig.from_table(client.get_table(table_address))

    with open(table_yaml_path, "w", encoding="utf-8") as yaml_file:
        yaml_file.write(yaml.dump(source_table_config.to_dict(), sort_keys=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dataset-id",
        required=True,
        help="The '{dataset}' to read source table schemas from",
    )

    parser.add_argument(
        "--table-ids",
        required=False,
        type=str_to_list,
        default=[],
        help="(optional) Source table(s) schema to update, multiple can be specified using a comma delimited list",
    )

    parser.add_argument(
        "--project-id", required=True, help="The project_id for the destination table"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    with local_project_id_override(args.project_id):
        if not args.table_ids:
            # If an explicit list of table addresses to update was not provided, instead
            # infer the list from the BQ view graph, updating all tables that are
            # referenced by any view.
            referenced_source_tables = BigQueryViewDagWalker(
                views=[
                    view_builder.build()
                    for view_builder in all_deployed_view_builders()
                ]
            ).get_referenced_source_tables()
            tables_to_update = {
                source_table_address
                for source_table_address in referenced_source_tables
                if source_table_address.dataset_id == args.dataset_id
            }
        else:
            tables_to_update = {
                BigQueryAddress(dataset_id=args.dataset_id, table_id=table_id)
                for table_id in args.table_ids
            }

        for source_table_address in tables_to_update:
            try:
                update_source_file_yaml(source_table_address)
            except Exception as e:
                logging.info(
                    "Error updating schema for %s; continuing...", source_table_address
                )
                logging.error(e)
