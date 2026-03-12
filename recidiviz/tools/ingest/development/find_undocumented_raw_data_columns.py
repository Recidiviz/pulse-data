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
"""A script that scans raw data config YAML files and lists columns that are not
documented (i.e. missing a meaningful description).

Can be run for a single state or all states.

Usage:
    # Single state:
    python -m recidiviz.tools.ingest.development.find_undocumented_raw_data_columns \
        --state-code US_AR

    # All states (omit --state-code):
    python -m recidiviz.tools.ingest.development.find_undocumented_raw_data_columns

    # Include summary counts only:
    python -m recidiviz.tools.ingest.development.find_undocumented_raw_data_columns \
        --summary-only
"""
import argparse
import logging
import sys

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.utils.string import is_meaningful_docstring


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find undocumented columns in raw data config files."
    )
    parser.add_argument(
        "--state-code",
        type=StateCode,
        help="State code to check (e.g. US_AR). If omitted, runs for all states.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only print per-state summary counts, not individual columns.",
    )
    return parser.parse_args(argv)


def find_undocumented_columns_for_state(
    state_code: StateCode, summary_only: bool
) -> tuple[int, int]:
    """Prints undocumented columns for the given state. Returns (undocumented, total)."""
    region_config = get_region_raw_file_config(state_code.value.lower())

    total_undocumented = 0
    total_columns = 0

    for file_tag in sorted(region_config.raw_file_configs):
        file_config = region_config.raw_file_configs[file_tag]
        current_cols = file_config.current_columns

        undocumented = [
            col for col in current_cols if not is_meaningful_docstring(col.description)
        ]
        total_columns += len(current_cols)
        total_undocumented += len(undocumented)

        if undocumented and not summary_only:
            logging.info(
                "  %s (%d/%d undocumented):",
                file_tag,
                len(undocumented),
                len(current_cols),
            )
            for col in undocumented:
                desc_preview = (
                    repr(col.description) if col.description else "<no description>"
                )
                logging.info("    - %s  %s", col.name, desc_preview)

    documented = total_columns - total_undocumented
    if total_columns:
        logging.info(
            "%s: %d undocumented / %d total columns (%.1f%% documented)",
            state_code.value,
            total_undocumented,
            total_columns,
            documented / total_columns * 100,
        )
    else:
        logging.info("%s: no columns found", state_code.value)
    return total_undocumented, total_columns


def main(
    state_code: StateCode | None,
    summary_only: bool,
) -> None:
    if state_code is not None:
        state_codes = [state_code]
    else:
        state_codes = get_existing_direct_ingest_states()

    grand_total_undocumented = 0
    grand_total_columns = 0
    for sc in state_codes:
        undocumented, total = find_undocumented_columns_for_state(sc, summary_only)
        grand_total_undocumented += undocumented
        grand_total_columns += total

    if state_code is None and grand_total_columns:
        grand_documented = grand_total_columns - grand_total_undocumented
        logging.info(
            "Total: %d undocumented / %d total columns (%.1f%% documented)",
            grand_total_undocumented,
            grand_total_columns,
            grand_documented / grand_total_columns * 100,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv[1:])
    main(
        state_code=args.state_code,
        summary_only=args.summary_only,
    )
