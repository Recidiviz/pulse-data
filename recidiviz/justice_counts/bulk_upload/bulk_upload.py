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
"""Functionality for bulk upload of data into the Justice Counts database."""


import calendar
import csv
import datetime
import logging
import os
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Tuple, Type

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
    MetricFile,
    fuzzy_match_against_options,
)
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metrics.metric_interface import (
    MetricAggregatedDimensionData,
    MetricInterface,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
    ReportStatus,
)

MONTH_NAMES = list(calendar.month_name)


class BulkUploader:
    """Functionality for bulk upload of data into the Justice Counts database."""

    def __init__(
        self, infer_aggregate_value: bool = False, catch_errors: bool = True
    ) -> None:
        self.catch_errors = catch_errors
        self.infer_aggregate_value = infer_aggregate_value
        self.text_analyzer = TextAnalyzer(
            configuration=TextMatchingConfiguration(
                # We don't want to treat "other" as a stop word,
                # because it's a valid breakdown category
                stop_words_to_remove={"other"}
            )
        )

    def upload_directory(
        self,
        session: Session,
        directory: str,
        agency_id: int,
        system: schema.System,
        user_account: schema.UserAccount,
    ) -> Dict[str, Exception]:
        """Iterate through all CSV files in the given directory and upload them
        to the Justice Counts database using the `upload_csv` method defined below.
        If an error is encountered on a particular file, log it and continue.
        """
        filename_to_error = {}

        # Sort so that we process e.g. caseloads before caseloads_by_gender.
        # This is important because it allows us to remove the requirement
        # that caseloads_by_gender includes the aggregate metric value too,
        # which would be redundant.
        for filename in sorted(os.listdir(directory)):
            if not filename.endswith(".csv"):
                continue

            filepath = os.path.join(directory, filename)
            logging.info("Uploading %s", filename)
            try:
                self.upload_csv(
                    session=session,
                    filename=filepath,
                    agency_id=agency_id,
                    system=system,
                    user_account=user_account,
                )
            except Exception as e:
                if self.catch_errors:
                    filename_to_error[filename] = e
                else:
                    raise e
        return filename_to_error

    def upload_excel(
        self,
        session: Session,
        xls: pd.ExcelFile,
        agency_id: int,
        system: schema.System,
        user_account: schema.UserAccount,
    ) -> Dict[str, Exception]:
        """Iterate through all tabs in an Excel spreadsheet and upload them
        to the Justice Counts database using the `upload_rows` method defined below.
        If an error is encountered on a particular tab, log it and continue.
        """
        sheet_to_error = {}

        # TODO(#13731): Save raw Excel file in GCS

        # Sort so that we process e.g. caseloads before caseloads_by_gender.
        # This is important because it allows us to remove the requirement
        # that caseloads_by_gender includes the aggregate metric value too,
        # which would be redundant.
        for sheet_name in sorted(xls.sheet_names):
            logging.info("Uploading %s", sheet_name)

            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                # Drop any rows that only contain NaN values
                df = df.dropna(axis=0, how="all")
                # Convert dataframe to a list of dictionaries
                rows = df.to_dict("records")

                self._upload_rows(
                    session=session,
                    system=system,
                    rows=rows,
                    filename=sheet_name,
                    agency_id=agency_id,
                    user_account=user_account,
                )
            except Exception as e:
                if self.catch_errors:
                    sheet_to_error[sheet_name] = e
                else:
                    raise e

        return sheet_to_error

    def upload_csv(
        self,
        session: Session,
        filename: str,
        agency_id: int,
        system: schema.System,
        user_account: schema.UserAccount,
    ) -> None:
        """Uploads a CSV file containing data for a particular metric.
        Core functionality is handled by the `upload_rows` method below.
        """
        with open(filename, "r", encoding="utf-8") as csvfile:
            rows = list(csv.DictReader(csvfile))

        # TODO(#13731): Save raw CSV file in GCS

        self._upload_rows(
            session=session,
            system=system,
            rows=rows,
            filename=filename,
            agency_id=agency_id,
            user_account=user_account,
        )

    def _upload_rows(
        self,
        session: Session,
        system: schema.System,
        rows: List[Dict[str, Any]],
        filename: str,
        agency_id: int,
        user_account: schema.UserAccount,
    ) -> None:
        """Generally, a file will only contain metrics for one system. In the case
        of supervision, the file could contain metrics for supervision, parole, or
        probation. This is indicated by the `system` column. In this case, we break
        up the rows by system, and then ingest one system at a time."""
        system_to_rows = self._get_system_to_rows(system=system, rows=rows)
        for current_system, current_rows in system_to_rows.items():
            # Based on the system and the name of the CSV file, determine which
            # Justice Counts metric this file contains data for
            metricfile = self._get_metricfile(filename=filename, system=current_system)

            self._upload_rows_for_metricfile(
                session=session,
                rows=current_rows,
                metricfile=metricfile,
                agency_id=agency_id,
                user_account=user_account,
            )

    def _get_system_to_rows(
        self, system: schema.System, rows: List[Dict[str, Any]]
    ) -> Dict[schema.System, List[Dict[str, Any]]]:
        """Groups the rows in the file by the value of the `system` column.
        Returns a dictionary mapping each system to its list of rows."""
        system_to_rows = {}
        if system == schema.System.SUPERVISION:
            system_value_to_rows = {
                k: list(v)
                for k, v in groupby(
                    sorted(rows, key=lambda x: x["system"]), lambda x: x["system"]
                )
            }
            normalized_system_value_to_system = {
                "both": schema.System.SUPERVISION,
                "parole": schema.System.PAROLE,
                "probation": schema.System.PROBATION,
            }
            for system_value, system_rows in system_value_to_rows.items():
                normalized_system_value = system_value.lower().strip()
                mapped_system = normalized_system_value_to_system[
                    normalized_system_value
                ]
                system_to_rows[mapped_system] = system_rows
        else:
            system_to_rows[system] = rows
        return system_to_rows

    def _upload_rows_for_metricfile(
        self,
        session: Session,
        rows: List[Dict[str, Any]],
        metricfile: MetricFile,
        agency_id: int,
        user_account: schema.UserAccount,
    ) -> None:
        """Takes as input a set of rows (originating from a CSV or Excel spreadsheet tab)
        in the format of a list of dictionaries, i.e. [{"column_name": <column_value>} ... ].
        The rows should be formatted according to the technical specification, and contain
        data for a particular metric across multiple time periods.

        Uploads this data into the Justice Counts database by breaking it up into Report objects,
        and either updating existing reports or creating new ones.

        A simplified version of the expected format:
        year | month | value | offense_type
        ===================================
        2021 | 1     | 100   | All
        2021 | 1     | 50    | Felony
        2021 | 2     | 110   | All

        This data would be used to either update or create two reports, one for January
        2021 and one for February 2021.

        The filename is assumed to be of the format "metric_name.csv", where metric_name
        corresponds to one of the MetricFile objects in bulk_upload_helpers.py.
        """
        reports = ReportInterface.get_reports_by_agency_id(session, agency_id=agency_id)

        # Step 1: Group any existing reports for this agency by time range.
        reports_sorted_by_time_range = sorted(
            reports, key=lambda x: (x.date_range_start, x.date_range_end)
        )
        reports_by_time_range = {
            k: list(v)
            for k, v in groupby(
                reports_sorted_by_time_range,
                key=lambda x: (x.date_range_start, x.date_range_end),
            )
        }

        metric_definition = metricfile.definition
        if len(metric_definition.reporting_frequencies) > 1:
            raise ValueError("Multiple reporting frequencies are not yet supported.")
        reporting_frequency = metric_definition.reporting_frequencies[0]

        # TODO(#13731): Make sure there are no unexpected columns in the file

        # Step 2: Group the rows in this file by time range.
        (rows_by_time_range, time_range_to_year_month,) = self._get_rows_by_time_range(
            rows=rows, reporting_frequency=reporting_frequency
        )

        # Step 3: For each time range represented in the file, convert the
        # reported data into a ReportMetric object. If a report already
        # exists for this time range, update it with the ReportMetric.
        # Else, create a new report and add the ReportMetric.
        for time_range, rows_for_this_time_range in rows_by_time_range.items():
            existing_report = reports_by_time_range.get(time_range)
            if existing_report is not None:
                if len(existing_report) != 1:
                    raise ValueError(
                        f"Found {len(existing_report)} reports with time range {time_range}."
                    )
                report = existing_report[0]
            else:  # existing report is None, so create the report
                year, month = time_range_to_year_month[time_range]
                report = ReportInterface.create_report(
                    session=session,
                    agency_id=agency_id,
                    user_account_id=user_account.id,
                    year=year,
                    month=month,
                    frequency=reporting_frequency.value,
                )
                reports_by_time_range[time_range] = [report]

            report_metric = self._get_report_metric(
                metricfile=metricfile,
                time_range=time_range,
                rows_for_this_time_range=rows_for_this_time_range,
            )

            ReportInterface.add_or_update_metric(
                session=session,
                report=report,
                report_metric=report_metric,
                user_account=user_account,
                use_existing_aggregate_value=metricfile.supplementary_disaggregation,
            )

            ReportInterface.update_report_metadata(
                session=session,
                report=report,
                editor_id=user_account.id,
                status=ReportStatus.DRAFT.value,
            )

    def _get_metricfile(self, filename: str, system: schema.System) -> MetricFile:
        try:
            # remove leading path and .csv extension and strip whitespace
            stripped_filename = filename.split("/")[-1].split(".")[0].strip()
        except Exception as e:
            raise ValueError(
                "Expected a filename of the format `metric_name.csv`."
            ) from e

        filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]

        if stripped_filename not in filename_to_metricfile:
            raise ValueError(
                f"No metric corresponds to the filename `{stripped_filename}`. "
                f"Options are {filename_to_metricfile.keys()}."
            )

        return filename_to_metricfile[stripped_filename]

    def _get_rows_by_time_range(
        self, rows: List[Dict[str, Any]], reporting_frequency: ReportingFrequency
    ) -> Tuple[
        Dict[Tuple[datetime.date, datetime.date], List[Dict[str, Any]]],
        Dict[Tuple[datetime.date, datetime.date], Tuple[int, int]],
    ]:
        rows_by_time_range = defaultdict(list)
        time_range_to_year_month = {}
        for row in rows:
            # remove whitespace from column headers
            row = {k.strip(): v for k, v in row.items() if k is not None}
            year = self._get_column_value(row=row, column_name="year", column_type=int)
            if reporting_frequency == ReportingFrequency.MONTHLY:
                month = self._get_column_value(
                    row=row, column_name="month", column_type=int
                )
            else:
                # TODO(#13731): Look up whether this agency uses fiscal years
                month = 1

            date_range_start, date_range_end = ReportInterface.get_date_range(
                year=year, month=month, frequency=reporting_frequency.value
            )
            time_range_to_year_month[(date_range_start, date_range_end)] = (year, month)
            rows_by_time_range[(date_range_start, date_range_end)].append(row)
        return rows_by_time_range, time_range_to_year_month

    def _get_report_metric(
        self,
        metricfile: MetricFile,
        time_range: Tuple[datetime.date, datetime.date],
        rows_for_this_time_range: List[Dict[str, Any]],
    ) -> MetricInterface:
        """Given a a set of rows from the CSV that all correspond to a single
        time period, convert the data in these rows to a MetricInterface object.
        If the metric associated with this CSV has no disaggregations, there
        should only be a single row for a single time period, and it contains
        the aggregate metric value. If the metric does have a disaggregation,
        there will be several rows, one with the value for each category.
        """
        aggregate_value = None
        dimension_to_value: Optional[Dict[DimensionBase, Optional[float]]] = (
            {d: None for d in metricfile.disaggregation}  # type: ignore[attr-defined]
            if metricfile.disaggregation is not None
            else None
        )

        # If this file represents a metric without disaggregations,
        # there should only be one row for a given time period.
        if metricfile.disaggregation is None:
            if len(rows_for_this_time_range) != 1:
                raise ValueError(
                    f"Only expected one row for time range {time_range} "
                    f"because {metricfile.filenames[0]} doesn't have any disaggregations, "
                    f"but found {len(rows_for_this_time_range)} rows."
                )

            row = rows_for_this_time_range[0]
            aggregate_value = self._get_column_value(
                row=row, column_name="value", column_type=float
            )

        else:  # metricfile.disaggregation is not None
            if metricfile.disaggregation_column_name is None:
                raise ValueError(
                    "`disaggregation` is not None but `disaggregation_column_name` is None"
                )
            for row in rows_for_this_time_range:
                # If this file represents a metric with a disaggregation,
                # there will likely be more than one row for a given time range;
                # there will be one row for each dimension value. Each will have
                # a value (i.e. the number or count) and a disaggregation value
                # (i.e. the category the count refers to, e.g. Male or Female).
                value = self._get_column_value(
                    row=row, column_name="value", column_type=float
                )

                # disaggregation_value is either "All" or an enum member,
                # e.g. "Male" for Gender, "Asian" for Race, "Felony" for OffenseType, etc
                disaggregation_value = self._get_column_value(
                    row=row,
                    column_name=metricfile.disaggregation_column_name,
                    column_type=str,
                )
                if disaggregation_value.lower() == "all":
                    aggregate_value = value
                else:
                    try:
                        matching_disaggregation_member = metricfile.disaggregation(disaggregation_value)  # type: ignore
                    except ValueError:
                        # A ValueError will be thrown by the line above if the user-entered disaggregation
                        # value is not actually a member of the disaggreation enum. In that case, we fuzzy
                        # match against the enum members and try again.
                        disaggregation_options = [
                            member.value for member in metricfile.disaggregation  # type: ignore[attr-defined]
                        ]
                        disaggregation_value = fuzzy_match_against_options(
                            analyzer=self.text_analyzer,
                            text=disaggregation_value,
                            options=disaggregation_options,
                        )
                        matching_disaggregation_member = metricfile.disaggregation(
                            disaggregation_value
                        )  # type: ignore[call-arg]
                    dimension_to_value[matching_disaggregation_member] = value  # type: ignore[index]

            if aggregate_value is None:
                # If aggregate_value is None, that means the input file doesn't contain a
                # row with the value 'All'. It only contains breakdown values.
                if metricfile.supplementary_disaggregation:
                    # If this file contains a non-primary aggregation, like gender or race,
                    # the aggregate values don't need to be reported, because they've already been
                    # reported on the primary aggregation. If the aggregate value IS reported,
                    # it will later be validated to match that of the primary aggregation (see
                    # `use_existing_aggregate_value` flag).
                    pass
                else:
                    # Otherwise, whether or not we require the aggregate value to be reported
                    # depends on the `infer_aggregate_value` flag. If this is True, we
                    # calculate the aggregate value by summing up all breakdowns. If this is
                    # False, and no aggregate value is reported, we throw an error.
                    if self.infer_aggregate_value:
                        aggregate_value = sum(
                            val  # type: ignore[misc]
                            for val in dimension_to_value.values()  # type: ignore[union-attr]
                            if val is not None
                        )
                    else:
                        raise ValueError(
                            f"No aggregate metric value found for the metric `{metricfile.filenames[0]}` "
                            f"and time period {time_range}. Make sure to include a row labeled `All` "
                            "for every time period."
                        )

        return MetricInterface(
            key=metricfile.definition.key,
            value=aggregate_value,
            contexts=[],
            aggregated_dimensions=[
                MetricAggregatedDimensionData(dimension_to_value=dimension_to_value)
            ]
            if dimension_to_value is not None
            else [],
        )

    def _get_column_value(
        self, row: Dict[str, Any], column_name: str, column_type: Type
    ) -> Any:
        """Given a row, a column name, and a column type, attempts to
        extract a value of the given type from the row."""
        if column_name not in row:
            raise ValueError(
                f"Expected the column {column_name} to be present. Got {list(row.keys())}"
            )

        column_value = row[column_name]

        # Allow numeric values with columns in them (e.g. 1,000)
        if isinstance(column_value, str):
            column_value = column_value.replace(",", "")

        try:
            value = column_type(column_value)
        except Exception as e:
            if column_name != "month":
                raise ValueError(
                    f"Expected the column {column_name} to be of type {column_type}. Got {row.items()}."
                ) from e

            # Allow "month" column to be either numbers or month names
            column_value = self._get_month_column_value(column_value=column_value)
            value = column_type(column_value)

        # Round numbers to two decimal places
        if isinstance(value, float):
            value = round(value, 2)

        return value

    def _get_month_column_value(self, column_value: str) -> int:
        """Takes as input a string and attempts to find the corresponding month
        index using the calendar module's month_names enum. For instance,
        March -> 3. Uses fuzzy matching to handle typos, such as `Febuary`."""
        column_value = column_value.title()
        if column_value not in MONTH_NAMES:
            column_value = fuzzy_match_against_options(
                analyzer=self.text_analyzer, text=column_value, options=MONTH_NAMES
            )
        return MONTH_NAMES.index(column_value)
