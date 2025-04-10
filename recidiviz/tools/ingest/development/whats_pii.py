#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
"""
Little script to show PII columns in raw data.

To see all PII columns, run:
  python -m recidiviz.tools.ingest.development.whats_pii

To see all PII columns of a given type, run:
  python -m recidiviz.tools.ingest.development.whats_pii \
    --field-type <type> 
"""

import argparse

from tabulate import tabulate

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    RawTableColumnFieldType,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)


def gather_pii_columns(
    field_type_filter: RawTableColumnFieldType | None = None,
) -> list[dict]:
    """Gathers all raw data columns with is_pii: True in their YAML configs."""
    pii_data = []

    for state in get_existing_direct_ingest_states():
        region_config = get_region_raw_file_config(state.value)
        for file_tag, raw_file_config in region_config.raw_file_configs.items():
            pii_columns = [
                {
                    "State": state.value,
                    "File": file_tag,
                    "Name": column.name,
                    "Field Type": column.field_type.value,
                    "Description": column.description or "NO DOCUMENTATION",
                }
                for column in raw_file_config.current_columns
                if column.is_pii
                and (not field_type_filter or column.field_type == field_type_filter)
            ]
            if pii_columns:
                pii_data.extend(pii_columns)
    return pii_data


def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Show PII columns in raw data.")
    parser.add_argument(
        "--field-type",
        help="Filter columns by their field type (e.g., 'STRING', 'DATETIME').",
        type=RawTableColumnFieldType,
        choices=list(RawTableColumnFieldType),
        default=None,
        required=False,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    pii_data = gather_pii_columns(field_type_filter=args.field_type)
    if not pii_data:
        print("No PII columns found.")
    else:
        print(
            tabulate(pii_data, tablefmt="heavy_grid", maxcolwidths=[20, 30, 20, 20, 60])
        )


if __name__ == "__main__":
    main()
