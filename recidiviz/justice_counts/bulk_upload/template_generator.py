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
"""Utilities for generating templates for Bulk Upload."""


import datetime
from collections import defaultdict
from typing import Dict, List

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.persistence.database.schema.justice_counts import schema

THIS_YEAR = datetime.date.today().year
LAST_YEAR = THIS_YEAR - 1


def get_supervision_subsystems_metric_interfaces(
    metric_key_to_interface: dict[str, MetricInterface],
    metricfile: MetricFile,
) -> List[MetricInterface]:
    """
    Returns the list of supervision subsystem metric interfaces from
    `metric_key_to_interface` which correspond to the `metricfile`.

    As an example, if metricfile is the "funding" metricfile, then when we get to a
    metric_interface that includes the "funding" metricfile
    (e.g. MetricInterface(
            key=PAROLE_FUNDING,
            metric_files = [funding metricfile, funding_by_type metricfile]
        ))
    then we will append this metric_interface to the list.
    """
    metric_interfaces = []
    for metric_interface in metric_key_to_interface.values():
        if (
            metricfile.canonical_filename
            in set(x.canonical_filename for x in metric_interface.metric_files)
            and metric_interface.metric_definition.system
            in schema.System.supervision_subsystems()
        ):
            metric_interfaces.append(metric_interface)
    return metric_interfaces


def generate_bulk_upload_template(
    system: schema.System,
    file_path: str,
    session: Session,
    agency: schema.Agency,
    is_single_page_template: bool = False,
    is_generic_template: bool = False,  # a template with all of the metric sheets
) -> None:
    """Generates a Bulk Upload template for a particular agency."""

    metric_key_to_interface = {
        metric_interface.key: metric_interface
        for metric_interface in MetricSettingInterface.get_agency_metric_interfaces(
            session,
            agency,
        )
    }
    metricfiles = SYSTEM_TO_METRICFILES[system]

    filename_to_rows = defaultdict(list)

    child_agencies = (
        AgencyInterface.get_child_agencies_for_agency(session=session, agency=agency)
        if agency.is_superagency is True
        else []
    )

    for metricfile in metricfiles:  # pylint: disable=too-many-nested-blocks
        high_level_metric = [
            curr_metricfile
            for curr_metricfile in metricfiles
            if curr_metricfile.definition.key == metricfile.definition.key
            and curr_metricfile.disaggregation is None
        ][0].canonical_filename

        metric_interfaces = [metric_key_to_interface[metricfile.definition.key]]
        if system == schema.System.SUPERVISION:
            metric_interfaces.extend(
                get_supervision_subsystems_metric_interfaces(
                    metric_key_to_interface=metric_key_to_interface,
                    metricfile=metricfile,
                )
            )

        for metric_interface in metric_interfaces:
            rows: List[Dict[str, str]] = []

            enabled_status = metric_interface.is_metric_enabled

            if is_generic_template is False and enabled_status is not True:
                continue
            reporting_frequency = (
                metric_interface.custom_reporting_frequency.frequency
                if metric_interface.custom_reporting_frequency.frequency is not None
                else metricfile.definition.reporting_frequency
            )

            if reporting_frequency == schema.ReportingFrequency.ANNUAL:
                rows = _add_rows_for_annual_metric(
                    rows=rows,
                    metricfile=metricfile,
                    is_single_page_template=is_single_page_template,
                    high_level_metric=high_level_metric,
                    is_disaggregated_by_supervision_subsystems=metric_interface.disaggregated_by_supervision_subsystems
                    is True,
                    metric_interface=metric_interface,
                )

            else:
                rows = _add_rows_for_monthly_metric(
                    rows=rows,
                    metricfile=metricfile,
                    is_single_page_template=is_single_page_template,
                    high_level_metric=high_level_metric,
                    is_disaggregated_by_supervision_subsystems=metric_interface.disaggregated_by_supervision_subsystems
                    is True,
                    metric_interface=metric_interface,
                )

            if metricfile.disaggregation is not None:
                rows = _add_rows_for_disaggregated_metric(
                    rows=rows,
                    metricfile=metricfile,
                    metric_interface=metric_interface,
                    is_single_page_template=is_single_page_template,
                )

            if agency.is_superagency is True and system != schema.System.SUPERAGENCY:
                rows = _add_rows_for_super_agency(
                    rows=rows,
                    child_agencies=child_agencies,
                    is_single_page_template=is_single_page_template,
                )

            # If no new rows were added, don't add the sheet. This will happen in
            # the case where a metric is all dimensions of an dissaggregation
            # are disabled.
            if is_single_page_template is False:
                if len(rows) > 0:
                    # This will need to be an extend.
                    filename_to_rows[metricfile.canonical_filename].extend(rows)
            else:
                filename_to_rows["Sheet 1"].extend(rows)

    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        file_path
    ) as writer:
        for filename, rows in filename_to_rows.items():
            df = pd.DataFrame.from_dict(rows)
            if is_single_page_template:
                row = rows[0]
                sorted_by = list(row.keys())
                # Sheet will be sorted by metric, year, month, agency, system,
                # `breakdown_category`, `breakdown`, `value`
                df.sort_values(by=sorted_by)
            df.to_excel(writer, sheet_name=filename, index=False)
            worksheet = writer.sheets[filename]
            for idx, col in enumerate(df):
                series = df[col]
                # set column length to max string length in column
                # default column length is 8
                max_len = max(series.astype(str).map(len).max(), 8)
                worksheet.set_column(idx, idx, max_len)


def _add_rows_for_annual_metric(
    rows: List[Dict[str, str]],
    metricfile: MetricFile,
    is_single_page_template: bool,
    high_level_metric: str,
    is_disaggregated_by_supervision_subsystems: bool,
    metric_interface: MetricInterface,
) -> List[Dict[str, str]]:
    """Creates rows for an annual metric."""
    if is_disaggregated_by_supervision_subsystems is True and (
        metricfile.definition.system == schema.System.SUPERVISION
        or metricfile.definition.system in schema.System.supervision_subsystems()
    ):
        system = metric_interface.metric_definition.system.value
        for year in [LAST_YEAR, THIS_YEAR]:
            row = (
                # Columns will be  `year`, `system`, `value`
                {"year": str(year), "system": system, "value": ""}
                if is_single_page_template is False
                # Columns will be `metric`, `year`, `month`, `system`,
                # `breakdown_category`, `breakdown`, `value`
                else {
                    "metric": high_level_metric,
                    "year": str(year),
                    "month": "",
                    "system": system,
                    "breakdown_category": "",
                    "breakdown": "",
                    "value": "",
                }
            )
            rows.append(row)
    else:
        for year in [LAST_YEAR, THIS_YEAR]:
            row = (
                # Columns will be  `year`, `value`
                {"year": str(year), "value": ""}
                if is_single_page_template is False
                # Columns will be `metric`, `year`, `month`,
                # `breakdown_category`, `breakdown`, `value`
                else {
                    "metric": high_level_metric,
                    "year": str(year),
                    "month": "",
                    "breakdown_category": "",
                    "breakdown": "",
                    "value": "",
                }
            )
            rows.append(row)
    return rows


def _add_rows_for_monthly_metric(
    rows: List[Dict[str, str]],
    metricfile: MetricFile,
    is_single_page_template: bool,
    high_level_metric: str,
    is_disaggregated_by_supervision_subsystems: bool,
    metric_interface: MetricInterface,
) -> List[Dict[str, str]]:
    """Creates rows for an monthly metric."""
    if is_disaggregated_by_supervision_subsystems is True and (
        metricfile.definition.system == schema.System.SUPERVISION
        or metricfile.definition.system in schema.System.supervision_subsystems()
    ):
        system = metric_interface.metric_definition.system.value
        for year in [LAST_YEAR, THIS_YEAR]:
            for month in range(1, 13):
                row = (
                    # Columns will be `year`, `month`, `system, `value`
                    {
                        "year": str(year),
                        "month": str(month),
                        "system": system,
                        "value": "",
                    }
                    if is_single_page_template is False
                    # Columns will be `metric`, `year`, `month`, `system`
                    # `breakdown_category`, `breakdown`, `value`
                    else {
                        "metric": high_level_metric,
                        "year": str(year),
                        "month": str(month),
                        "system": system,
                        "breakdown_category": "",
                        "breakdown": "",
                        "value": "",
                    }
                )
                rows.append(row)
    else:
        for year in [LAST_YEAR, THIS_YEAR]:
            for month in range(1, 13):
                row = (
                    # Columns will be `year`, `month`, `value`
                    {"year": str(year), "month": str(month), "value": ""}
                    if is_single_page_template is False
                    # Columns will be `metric`, `year`, `month`,
                    # `breakdown_category`, `breakdown`, `value`
                    else {
                        "metric": high_level_metric,
                        "year": str(year),
                        "month": str(month),
                        "breakdown_category": "",
                        "breakdown": "",
                        "value": "",
                    }
                )
                rows.append(row)
    return rows


def _add_rows_for_disaggregated_metric(
    rows: List[Dict[str, str]],
    metricfile: MetricFile,
    is_single_page_template: bool,
    metric_interface: MetricInterface,
) -> List[Dict[str, str]]:
    """Adds rows for a sheet representing a metric's disaggregation"""
    new_rows: List[Dict[str, str]] = []
    dimension_id_to_dimension_metric_interface = {
        aggregated_dim.dimension_identifier(): aggregated_dim
        for aggregated_dim in metric_interface.aggregated_dimensions
    }
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
                    is not True
                ):
                    # If a dimension is turned off, don't include it in the spreadsheet.
                    continue
            values_to_add = [dimension.value]

            if dimension_metric_interface is not None:
                # Add sub-dimensions
                for (
                    sub_dimension,
                    enabled_status,
                ) in dimension_metric_interface.dimension_to_other_sub_dimension_to_enabled_status.get(
                    dimension, {}
                ).items():
                    if enabled_status is True:
                        values_to_add.append(
                            f"{dimension.value} - {sub_dimension.title()}"
                        )

            for value in values_to_add:
                if is_single_page_template is False:
                    # Columns will be `year`, `month`, `system`,
                    # `disaggregation col (i.e offense_type)`, `value`
                    new_rows.append(
                        {
                            **row,  # type: ignore[arg-type]
                            metricfile.disaggregation_column_name: value,  # type: ignore
                            "value": "",
                        }
                    )
                else:
                    # Columns will be `metric`, `year`, `month`, `system`
                    # `breakdown_category`, `breakdown`, `value`
                    new_rows.append(
                        {
                            **row,  # type: ignore[arg-type]
                            "breakdown_category": metricfile.disaggregation_column_name,  # type: ignore
                            "breakdown": value,
                            "value": "",
                        }
                    )

    return new_rows


def _add_rows_for_super_agency(
    rows: List[Dict[str, str]],
    child_agencies: List[schema.Agency],
    is_single_page_template: bool,
) -> List[Dict[str, str]]:
    """Updates rows with a `agency` column for super agency templates."""

    new_rows: List[Dict[str, str]] = []
    for row in rows:
        if is_single_page_template is True:
            metric = row.pop("metric")  # move first column
            year = row.pop("year")
            month = row.pop("month")
            # Columns will be `metric`, `year`, `month`, `agency`,
            # `system`, `breakdown_category`, `breakdown`, `value`
            if len(child_agencies) > 0:
                for agency in child_agencies:
                    new_rows.append(
                        {
                            "metric": metric,
                            "year": year,
                            "month": month,
                            "agency": agency.name,
                            **row,
                        }
                    )
            else:
                new_rows.append(
                    {
                        "metric": metric,
                        "year": year,
                        "month": month,
                        "agency": "",
                        **row,
                    }
                )
        else:
            row.pop("value")  # move last column
            # Columns will be `year`, `month`, `agency`, `value`
            if len(child_agencies) > 0:
                for agency in child_agencies:
                    new_rows.append(
                        {
                            **row,
                            "agency": agency.name,
                            "value": "",
                        }
                    )
            else:
                new_rows.append(
                    {
                        **row,
                        "agency": "",
                        "value": "",
                    }
                )
    return new_rows
