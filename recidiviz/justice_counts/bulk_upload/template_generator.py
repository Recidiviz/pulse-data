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
"""


import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.persistence.database.schema.justice_counts import schema

THIS_YEAR = datetime.date.today().year
LAST_YEAR = THIS_YEAR - 1


def generate_bulk_upload_template(
    system: schema.System,
    file_path: str,
    session: Session,
    agency: schema.Agency,
) -> None:
    """Generates a Bulk Upload template for a particular agency."""

    metric_interface_list = DatapointInterface.get_metric_settings_by_agency(
        session,
        agency,
    )
    metric_key_to_interface = {
        metric_interface.key: metric_interface
        for metric_interface in metric_interface_list
    }
    metricfiles = SYSTEM_TO_METRICFILES[system]
    filename_to_rows = {}

    child_agencies = (
        AgencyInterface.get_child_agencies_for_agency(session=session, agency=agency)
        if agency.is_superagency is True
        else []
    )

    for metricfile in metricfiles:  # pylint: disable=too-many-nested-blocks
        rows: List[Dict[str, str]] = []
        metric_interface = metric_key_to_interface.get(metricfile.definition.key)
        enabled_status = (
            metric_interface.is_metric_enabled if metric_interface is not None else True
        )

        if enabled_status is False:
            continue

        if (
            metricfile.definition.reporting_frequency
            == schema.ReportingFrequency.ANNUAL
        ):
            rows = _add_rows_for_annual_metric(rows=rows, metricfile=metricfile)

        else:
            rows = _add_rows_for_monthly_metric(rows=rows, metricfile=metricfile)

        if metricfile.disaggregation is not None:
            rows = _add_rows_for_disaggregated_metric(
                rows=rows,
                metricfile=metricfile,
                metric_interface=metric_interface,
            )

        if agency.is_superagency is True:
            rows = _add_rows_for_super_agency(rows=rows, child_agencies=child_agencies)

        # If no new rows were added, don't add the sheet. This will happen in
        # the case where a metric is all dimensions of an dissaggregation
        # are disabled.
        if len(rows) > 0:
            filename_to_rows[metricfile.canonical_filename] = rows

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


def _add_rows_for_annual_metric(
    rows: List[Dict[str, str]], metricfile: MetricFile
) -> List[Dict[str, str]]:
    if metricfile.definition.system == schema.System.SUPERVISION:
        for s in [s.name for s in schema.System.supervision_subsystems()] + ["ALL"]:
            for year in [LAST_YEAR, THIS_YEAR]:
                row = {"year": str(year), "system": s, "value": ""}
                rows.append(row)
    else:
        for year in [LAST_YEAR, THIS_YEAR]:
            row = {"year": str(year), "value": ""}
            rows.append(row)
    return rows


def _add_rows_for_monthly_metric(
    rows: List[Dict[str, str]], metricfile: MetricFile
) -> List[Dict[str, str]]:
    if metricfile.definition.system == schema.System.SUPERVISION:
        for s in [s.name for s in schema.System.supervision_subsystems()] + ["ALL"]:
            for year in [LAST_YEAR, THIS_YEAR]:
                for month in range(1, 13):
                    row = {
                        "year": str(year),
                        "month": str(month),
                        "system": s,
                        "value": "",
                    }
                    rows.append(row)
    else:
        for year in [LAST_YEAR, THIS_YEAR]:
            for month in range(1, 13):
                row = {"year": str(year), "month": str(month), "value": ""}
                rows.append(row)
    return rows


def _add_rows_for_disaggregated_metric(
    rows: List[Dict[str, str]],
    metricfile: MetricFile,
    metric_interface: Optional[MetricInterface] = None,
) -> List[Dict[str, str]]:
    """Adds rows for a sheet representing a metric's disaggregation"""
    new_rows = []
    dimension_id_to_dimension_metric_interface = (
        {
            aggregated_dim.dimension_identifier(): aggregated_dim
            for aggregated_dim in metric_interface.aggregated_dimensions
        }
        if metric_interface is not None
        else {}
    )
    dimension_metric_interface = dimension_id_to_dimension_metric_interface.get(
        metricfile.disaggregation.dimension_identifier()  # type: ignore[union-attr]
    )
    for row in rows:
        row.pop("value")  # move value column last
        for dimension in metricfile.disaggregation:  # type: ignore[union-attr]
            if (
                dimension_metric_interface is not None
                and dimension_metric_interface.dimension_to_enabled_status is not None
            ):
                if (
                    dimension_metric_interface.dimension_to_enabled_status.get(
                        dimension
                    )
                    is False
                ):
                    # If a dimension is turned off, don't include it in the spreadsheet.
                    continue
            new_row: Dict[str, Any] = {
                **row,
                metricfile.disaggregation_column_name: dimension.value,  # type: ignore
                "value": "",
            }
            new_rows.append(new_row)
    return new_rows


def _add_rows_for_super_agency(
    rows: List[Dict[str, str]], child_agencies: List[schema.Agency]
) -> List[Dict[str, str]]:
    new_rows = []
    for row in rows:
        row.pop("value")  # move value column last
        for agency in child_agencies:
            new_row: Dict[str, Any] = {
                **row,
                "agency": agency.name,
                "value": "",
            }
            new_rows.append(new_row)
    return new_rows
