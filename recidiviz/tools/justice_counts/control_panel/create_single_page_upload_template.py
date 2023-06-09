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
"""Utilities for generating single-page templates for Bulk Upload.

Usage: python -m recidiviz.tools.justice_counts.control_panel.create_single_page_upload_template LAW_ENFORCEMENT
"""

import argparse
import os
from typing import Any, Dict

import numpy as np
import pandas as pd

from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.persistence.database.schema.justice_counts import schema


def generate_single_page_bulk_upload_template(system: schema.System) -> None:
    """Generates a Single Page Bulk Upload template for a particular agency."""
    system_enum = schema.System(system)
    metricfiles = SYSTEM_TO_METRICFILES[system_enum]
    all_rows = []
    for metricfile in metricfiles:  # pylint: disable=too-many-nested-blocks
        rows = []
        current_metric = metricfile.definition
        high_level_metric = [
            metric_file
            for metric_file in metricfiles
            if metric_file.definition == current_metric
            and metric_file.disaggregation is None
        ][0].canonical_filename
        if (
            metricfile.definition.reporting_frequency
            == schema.ReportingFrequency.ANNUAL
        ):
            if metricfile.definition.system == schema.System.SUPERVISION:
                for s in [s.name for s in schema.System.supervision_subsystems()] + [
                    "ALL"
                ]:
                    for year in [2023]:
                        row = {
                            "metric": high_level_metric,
                            "year": str(year),
                            "month": np.nan,
                            "system": s,
                            "breakdown_category": np.nan,
                            "breakdown": np.nan,
                            "value": np.nan,
                        }
                        rows.append(row)
            else:
                for year in [2023]:
                    row = {
                        "metric": high_level_metric,
                        "year": str(year),
                        "month": np.nan,
                        "breakdown_category": np.nan,
                        "breakdown": np.nan,
                        "value": np.nan,
                    }
                    rows.append(row)
        else:
            if metricfile.definition.system == schema.System.SUPERVISION:
                for s in [s.name for s in schema.System.supervision_subsystems()] + [
                    "ALL"
                ]:
                    for year in [2023]:
                        for month in range(1, 13):
                            row = {
                                "metric": high_level_metric,
                                "year": str(year),
                                "month": str(month),
                                "system": s,
                                "breakdown_category": np.nan,
                                "breakdown": np.nan,
                                "value": np.nan,
                            }
                            rows.append(row)
            else:
                for year in [2023]:
                    for month in range(1, 13):
                        row = {
                            "metric": high_level_metric,
                            "year": str(year),
                            "month": str(month),
                            "breakdown_category": np.nan,
                            "breakdown": np.nan,
                            "value": np.nan,
                        }
                        rows.append(row)

        new_rows = []
        if metricfile.disaggregation:
            for row in rows:
                for (
                    dimension
                ) in metricfile.disaggregation:  # type: ignore[attr-defined]
                    new_row: Dict[str, Any] = {
                        **row,
                        "breakdown_category": metricfile.disaggregation_column_name,  # type: ignore
                        "breakdown": dimension.value,
                    }
                    new_rows.append(new_row)
        else:
            new_rows = rows

        all_rows.extend(new_rows)

    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        f"{system_enum.name}.xlsx"
    ) as writer:
        df = pd.DataFrame.from_dict(all_rows)
        df.to_excel(writer, index=False)
        worksheet = writer.sheets["Sheet1"]
        for idx, col in enumerate(df):
            series = df[col]
            # set column length to max string length in column
            # default column length is 8
            max_len = max(series.astype(str).map(len).max(), 8)
            worksheet.set_column(idx, idx, max_len)


if __name__ == "__main__":
    if not os.getcwd().endswith("pulse-data"):
        raise ValueError("Script must be run from pulse-data directory.")

    parser = argparse.ArgumentParser()
    parser.add_argument("system")
    args = parser.parse_args()
    generate_single_page_bulk_upload_template(system=args.system)
