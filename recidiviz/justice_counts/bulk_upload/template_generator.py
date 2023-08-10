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
# but WITHOUT ANY WARRANTY; without even the implied warragit addnty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Utilities for generating templates for Bulk Upload.
"""


import datetime
from typing import Any, Dict

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.persistence.database.schema.justice_counts import schema


def generate_bulk_upload_template(
    system: schema.System,
    file_path: str,
    session: Session,
    agency: schema.Agency,
) -> None:
    """Generates a Bulk Upload template for a particular agency."""

    MetricInterface_objs = DatapointInterface.get_metric_settings_by_agency(
        session,
        agency,
    )
    metric_key_to_enabled_status = {
        metric.key: metric.is_metric_enabled for metric in MetricInterface_objs
    }
    metricfiles = SYSTEM_TO_METRICFILES[system]
    filename_to_rows = {}

    this_year = datetime.date.today().year
    last_year = this_year - 1

    for metricfile in metricfiles:  # pylint: disable=too-many-nested-blocks
        enabled_status = metric_key_to_enabled_status.get(metricfile.definition.key)
        if enabled_status is False:
            continue
        rows = []
        if (
            metricfile.definition.reporting_frequency
            == schema.ReportingFrequency.ANNUAL
        ):
            if metricfile.definition.system == schema.System.SUPERVISION:
                for s in [s.name for s in schema.System.supervision_subsystems()] + [
                    "ALL"
                ]:
                    for year in [last_year, this_year]:
                        row = {"year": str(year), "system": s, "value": ""}
                        rows.append(row)
            else:
                for year in [last_year, this_year]:
                    row = {"year": str(year), "value": ""}
                    rows.append(row)
        else:
            if metricfile.definition.system == schema.System.SUPERVISION:
                for s in [s.name for s in schema.System.supervision_subsystems()] + [
                    "ALL"
                ]:
                    for year in [last_year, this_year]:
                        for month in range(1, 13):
                            row = {
                                "year": str(year),
                                "month": str(month),
                                "system": s,
                                "value": "",
                            }
                            rows.append(row)
            else:
                for year in [last_year, this_year]:
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
                    new_row: Dict[str, Any] = {
                        **row,
                        metricfile.disaggregation_column_name: dimension.value,  # type: ignore
                        "value": "",
                    }
                    new_rows.append(new_row)
        else:
            new_rows = rows

        filename_to_rows[metricfile.canonical_filename] = new_rows

    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        file_path
    ) as writer:
        for filename, rows in filename_to_rows.items():
            df = pd.DataFrame.from_dict(rows)
            df.to_excel(writer, sheet_name=filename, index=False)
            worksheet = writer.sheets[filename]
            for idx, col in enumerate(df):
                series = df[col]
                # set column length to max string length in column
                # default column length is 8
                max_len = max(series.astype(str).map(len).max(), 8)
                worksheet.set_column(idx, idx, max_len)
