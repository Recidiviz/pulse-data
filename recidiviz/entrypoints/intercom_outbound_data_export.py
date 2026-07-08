# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Entrypoint for Intercom outbound content data export"""

import argparse
import csv
from datetime import datetime

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.dataset_config import INTERCOM_EXPORT_DATASET
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.externally_managed.collect_externally_managed_source_table_configs import (
    build_source_table_repository_for_externally_managed_tables,
)


def verify_headers(
    file_paths: dict[str, str],
) -> None:
    """
    Verify the CSV header matches the schema in the corresponding YAML file

    Args:
        file_paths: temp filepaths for Intercom outbound export data CSVs
    """

    repo = build_source_table_repository_for_externally_managed_tables(project_id=None)

    for base_name, source_path in file_paths.items():
        # TODO(OBT-18103): Change to table_id=base_name after yamls are redefined
        table_id = f"intercom_{base_name}"
        big_query_address = BigQueryAddress(
            dataset_id=INTERCOM_EXPORT_DATASET, table_id=table_id
        )

        if big_query_address not in repo.source_tables:
            # TODO(OBT-18103): Change this error message after the yamls are redefined as GCS backed tables to
            # f"Check that a YAML for this table exists under {os.path.dirname(intercom_export.__file__)}"
            raise KeyError(
                f"No source table config found for [{big_query_address.to_str()}]. "
                f"Check that a YAML for this table exists under "
                f"recidiviz/source_tables/externally_managed/intercom_export/."
            )
        source_table_config = repo.source_tables[big_query_address]
        schema_fields_by_name = [f.name for f in source_table_config.schema_fields]

        with open(file=source_path, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)

        for i, col_name in enumerate(headers):
            if col_name not in schema_fields_by_name:
                raise ValueError(
                    f"Column [{col_name}] not found in source table YAML for "
                    f"[{big_query_address.to_str()}]. "
                    f"Available columns: [{schema_fields_by_name}]"
                )
            if col_name != schema_fields_by_name[i]:
                raise ValueError(
                    f"CSV header does not match the ordering of the YAML schema fields "
                    f"found in [{big_query_address.to_str()}]. "
                    f"CSV columns: [{headers}]. "
                    f"Source table YAML schema fields: [{schema_fields_by_name}]."
                )


class IntercomOutboundDataExport(EntrypointInterface):
    """Entrypoint for Intercom outbound content data export"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--destination-dataset",
            help="The dataset that the exported Intercom data will be added to",
            type=str,
            required=True,
        )
        parser.add_argument(
            "--start-datetime-inclusive",
            help="UTC datetime for start of date range, inclusive",
            type=datetime.fromisoformat,
            required=True,
        )
        parser.add_argument(
            "--end-datetime-inclusive",
            help="UTC datetime for end of date range, inclusive",
            type=datetime.fromisoformat,
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        """Runs Intercom outbound content data export."""
