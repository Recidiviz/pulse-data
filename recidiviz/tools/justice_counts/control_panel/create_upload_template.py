#!/usr/bin/env bash

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
"""Utilities for generating templates for Bulk Upload.

Usage: python -m recidiviz.tools.justice_counts.control_panel.create_upload_template LAW_ENFORCEMENT
"""

import argparse
import os
from typing import Any, Dict

import pandas as pd

from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.persistence.database.schema.justice_counts import schema


def generate_bulk_upload_template(system: schema.System) -> None:
    """Generates a Bulk Upload template for a particular agency."""
    system_enum = schema.System(system)
    metricfiles = SYSTEM_TO_METRICFILES[system_enum]
    filename_to_rows = {}
    for metricfile in metricfiles:
        rows = []
        if (
            metricfile.definition.reporting_frequency
            == schema.ReportingFrequency.ANNUAL
        ):
            for year in [2021, 2022]:
                row = {"year": str(year), "value": ""}
                rows.append(row)
        else:
            for year in [2021, 2022]:
                for month in range(1, 13):
                    row = {"year": str(year), "month": str(month), "value": ""}
                    rows.append(row)

        new_rows = []
        if metricfile.disaggregation:
            for row in rows:
                row.pop("value")  # move value column last
                for (
                    dimension
                ) in metricfile.disaggregation:  # type: ignore[attr-defined]
                    new_row: Dict[str, Any] = dict(
                        **row,
                        **{metricfile.disaggregation_column_name: dimension.value},
                        **{"value": ""},
                    )
                    new_rows.append(new_row)
        else:
            new_rows = rows

        filename_to_rows[metricfile.canonical_filename] = new_rows

    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        f"frontends/justice-counts/control-panel/public/assets/{system_enum.name}.xlsx"
    ) as writer:
        for filename, rows in filename_to_rows.items():
            df = pd.DataFrame.from_dict(rows)
            df.to_excel(writer, sheet_name=filename, index=False)


if __name__ == "__main__":
    if not os.getcwd().endswith("pulse-data"):
        raise ValueError("Script must be run from pulse-data directory.")

    parser = argparse.ArgumentParser()
    parser.add_argument("system")
    args = parser.parse_args()
    generate_bulk_upload_template(system=args.system)
