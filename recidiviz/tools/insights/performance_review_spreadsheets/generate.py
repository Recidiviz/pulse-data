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
Script for generating Excel spreadsheets with staff metrics for supervisors in ID, for use in their
performance reviews. See https://www.notion.so/recidiviz/Design-Doc-ID-2025-Performance-Reviews-1517889f4d198077b276fc1fc5569165
for details.

Usage: python -m recidiviz.tools.insights.performance_review_spreadsheets.generate
This will generate spreadsheets in the root directory of pulse-data.
"""

import argparse
import logging
import os
import sys
from datetime import date
from enum import Enum, auto
from typing import List, Tuple

from dateutil.relativedelta import relativedelta
from openpyxl import Workbook
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.styles.numbers import FORMAT_NUMBER, FORMAT_PERCENTAGE_00
from openpyxl.worksheet.worksheet import Worksheet

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.tools.insights.performance_review_spreadsheets.impact_funnel_metrics import (
    ImpactFunnelMetrics,
)
from recidiviz.tools.insights.performance_review_spreadsheets.officer_aggregated_metrics import (
    OfficerAggregatedMetrics,
)
from recidiviz.tools.insights.performance_review_spreadsheets.officer_aggregated_metrics_from_sandbox import (
    OfficerAggregatedMetricsFromSandbox,
)
from recidiviz.tools.insights.performance_review_spreadsheets.supervisors_and_officers import (
    SupervisorsAndOfficers,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override


class RowHeadingEnum(Enum):
    USAGE = auto()
    OUTCOMES_METRICS = auto()
    AVG_DAILY_CASELOAD = auto()
    TIMELY_RISK_ASSESSMENTS = auto()
    TIMELY_F2F_CONTACTS = auto()
    SUPERVISION_LEVEL_MISMATCH = auto()
    EARLY_DISCHARGE = auto()
    EARLY_DISCHARGE_GRANTS = auto()
    EARLY_DISCHARGE_MONTHLY_GRANT_RATE = auto()
    EARLY_DISCHARGE_ELIGIBLE = auto()
    LSU = auto()
    LSU_GRANTS = auto()
    LSU_MONTHLY_GRANT_RATE = auto()
    LSU_ELIGIBLE = auto()
    SLD = auto()
    SLD_GRANTS = auto()
    SLD_MONTHLY_GRANT_RATE = auto()
    SLD_ELIGIBLE = auto()
    FT_DISCHARGE = auto()
    FT_DISCHARGE_GRANTS = auto()
    FT_DISCHARGE_MONTHLY_GRANT_RATE = auto()
    FT_DISCHARGE_ELIGIBLE = auto()
    OPERATIONS_METRICS = auto()


class ColumnHeadingEnum(Enum):
    YEARLY = "All of 2024"


_ROW_HEADING_LABELS = {
    RowHeadingEnum.USAGE: "Usage",
    RowHeadingEnum.OUTCOMES_METRICS: "Outcomes Metrics",
    RowHeadingEnum.AVG_DAILY_CASELOAD: "# Average Daily Caseload",
    RowHeadingEnum.EARLY_DISCHARGE: "Early Discharge",
    RowHeadingEnum.EARLY_DISCHARGE_GRANTS: "Grants during time period",
    RowHeadingEnum.EARLY_DISCHARGE_MONTHLY_GRANT_RATE: "Monthly grant rate: (# grants that month / avg daily caseload that month) or average of monthly grant rate ",
    RowHeadingEnum.EARLY_DISCHARGE_ELIGIBLE: "Eligible as of end of time period",
    RowHeadingEnum.LSU: "LSU",
    RowHeadingEnum.LSU_GRANTS: "Grants during time period",
    RowHeadingEnum.LSU_MONTHLY_GRANT_RATE: "Monthly grant rate: (# grants that month / avg daily caseload that month) or average of monthly grant rate ",
    RowHeadingEnum.LSU_ELIGIBLE: "Eligible as of end of time period",
    RowHeadingEnum.SLD: "Supervision Level Downgrade",
    RowHeadingEnum.SLD_GRANTS: "Grants during time period",
    RowHeadingEnum.SLD_MONTHLY_GRANT_RATE: "Monthly grant rate: (# grants that month / avg daily caseload that month) or average of monthly grant rate ",
    RowHeadingEnum.SLD_ELIGIBLE: "Eligible as of end of time period",
    RowHeadingEnum.FT_DISCHARGE: "Past FTRD",
    RowHeadingEnum.FT_DISCHARGE_GRANTS: "Grants during time period",
    RowHeadingEnum.FT_DISCHARGE_MONTHLY_GRANT_RATE: "Monthly grant rate: (# grants that month / avg daily caseload that month) or average of monthly grant rate ",
    RowHeadingEnum.FT_DISCHARGE_ELIGIBLE: "Eligible as of end of time period",
    RowHeadingEnum.OPERATIONS_METRICS: "Operations Metrics",
    RowHeadingEnum.TIMELY_RISK_ASSESSMENTS: "Timely Risk Assessments",
    RowHeadingEnum.TIMELY_F2F_CONTACTS: "Timely F2F Contacts",
    RowHeadingEnum.SUPERVISION_LEVEL_MISMATCH: "Supervision & Risk Level Mismatch",
}

_ROW_HEADINGS: list[RowHeadingEnum | str | None] = [
    RowHeadingEnum.USAGE,
    "# Total Logins each month",
    "# of Months in 2024 where they logged in at least once",
    "Caseload Type",
    None,
    RowHeadingEnum.OUTCOMES_METRICS,
    RowHeadingEnum.AVG_DAILY_CASELOAD,
    "# Absconsions",
    "Months flagged as having a high absconsion rate",
    "# Incarcerations",
    "Months flagged as having a high incarceration rate",
    None,
    RowHeadingEnum.EARLY_DISCHARGE,
    RowHeadingEnum.EARLY_DISCHARGE_ELIGIBLE,
    "Overridden (Marked Ineligible) as of end of time period",
    RowHeadingEnum.EARLY_DISCHARGE_GRANTS,
    RowHeadingEnum.EARLY_DISCHARGE_MONTHLY_GRANT_RATE,
    'Most often used "Ineligible Reason"',
    None,
    RowHeadingEnum.LSU,
    RowHeadingEnum.LSU_ELIGIBLE,
    "Overridden (Marked Ineligible) as of end of time period",
    RowHeadingEnum.LSU_GRANTS,
    RowHeadingEnum.LSU_MONTHLY_GRANT_RATE,
    'Most often used "Ineligible Reason"',
    None,
    RowHeadingEnum.SLD,
    RowHeadingEnum.SLD_ELIGIBLE,
    "Overridden (Marked Ineligible) as of end of time period",
    RowHeadingEnum.SLD_GRANTS,
    RowHeadingEnum.SLD_MONTHLY_GRANT_RATE,
    'Most often used "Ineligible Reason"',
    None,
    RowHeadingEnum.FT_DISCHARGE,
    RowHeadingEnum.FT_DISCHARGE_ELIGIBLE,
    "Overridden (Marked Ineligible) as of end of time period",
    RowHeadingEnum.FT_DISCHARGE_GRANTS,
    RowHeadingEnum.FT_DISCHARGE_MONTHLY_GRANT_RATE,
    'Most often used "Ineligible Reason"',
    None,
    RowHeadingEnum.OPERATIONS_METRICS,
    RowHeadingEnum.TIMELY_RISK_ASSESSMENTS,
    RowHeadingEnum.TIMELY_F2F_CONTACTS,
    RowHeadingEnum.SUPERVISION_LEVEL_MISMATCH,
]

_COLUMN_DATE_FORMAT = "%b %Y"

_COLUMN_HEADINGS = [
    None,
    ColumnHeadingEnum.YEARLY.value,
    *[date(2024, month, 1).strftime(_COLUMN_DATE_FORMAT) for month in range(1, 13)],
]

# Rows that represent a section header and need different formatting
_ROW_SECTION_HEADERS = [
    RowHeadingEnum.USAGE,
    RowHeadingEnum.OUTCOMES_METRICS,
    RowHeadingEnum.EARLY_DISCHARGE,
    RowHeadingEnum.LSU,
    RowHeadingEnum.SLD,
    RowHeadingEnum.FT_DISCHARGE,
    RowHeadingEnum.OPERATIONS_METRICS,
]

_GRAY_BACKGROUND = PatternFill(
    start_color="d9d9d9", end_color="d9d9d9", fill_type="solid"
)
_RED_BACKGROUND = PatternFill(
    start_color="ea9999", end_color="ea9999", fill_type="solid"
)
_BOLD_FONT = Font(bold=True)


CONDITIONAL_FORMAT_THRESHOLDS = {
    RowHeadingEnum.TIMELY_RISK_ASSESSMENTS: 0.9,
    RowHeadingEnum.TIMELY_F2F_CONTACTS: 0.9,
    RowHeadingEnum.SUPERVISION_LEVEL_MISMATCH: 0.98,
}


def create_headers(sheet: Worksheet) -> None:
    """Adds header rows + columns to the spreadsheet. This must be run before other data is added to
    the spreadsheet, because it just appends full rows."""
    data = [
        _COLUMN_HEADINGS,
        *[
            [_ROW_HEADING_LABELS[row] if isinstance(row, RowHeadingEnum) else row]
            for row in _ROW_HEADINGS
        ],
    ]
    for index, row in enumerate(data):
        sheet.append(row)
        sheet[f"A{index+1}"].alignment = Alignment(wrap_text=True)

    sheet.column_dimensions["A"].width = 48
    for header in _ROW_SECTION_HEADERS:
        index = get_row_index(header)
        sheet[f"A{index}"].font = _BOLD_FONT
        sheet[f"A{index}"].fill = _GRAY_BACKGROUND
    sheet["B1"].font = _BOLD_FONT


def get_row_index(heading: RowHeadingEnum) -> int:
    return (
        _ROW_HEADINGS.index(heading) + 2  # 1-indexed + additional 1 for empty first row
    )


def get_column_index(heading: str) -> str:
    return chr(ord("A") + _COLUMN_HEADINGS.index(heading))


def apply_conditional_formatting(sheet: Worksheet) -> None:
    for row_heading, threshold in CONDITIONAL_FORMAT_THRESHOLDS.items():
        row_idx = get_row_index(row_heading)
        col_idx = get_column_index(ColumnHeadingEnum.YEARLY.value)

        cell = f"{col_idx}{row_idx}"

        sheet.conditional_formatting.add(
            cell,
            FormulaRule(
                formula=[f"AND(NOT(ISBLANK({cell})), {cell}<{threshold})"],
                fill=_RED_BACKGROUND,
            ),
        )


def try_set_metric_cell(
    sheet: Worksheet, metric_value: int | float | None, col_idx: str, row_idx: int
) -> None:
    if metric_value is not None:
        cell = sheet[f"{col_idx}{row_idx}"]
        cell.value = metric_value
        cell.number_format = FORMAT_NUMBER


def write_metrics(
    sheet: Worksheet,
    officer_id: str,
    officer_aggregated_metrics: OfficerAggregatedMetrics,
) -> None:
    """Write aggregated metrics to the appropriate cells in the sheet"""
    avg_daily_caseload_row = get_row_index(RowHeadingEnum.AVG_DAILY_CASELOAD)
    for metric in officer_aggregated_metrics.monthly_data[officer_id]:
        parsed_date = (metric.end_date_exclusive - relativedelta(days=1)).strftime(
            _COLUMN_DATE_FORMAT
        )
        col_idx = get_column_index(parsed_date)
        try_set_metric_cell(
            sheet, metric.avg_daily_population, col_idx, avg_daily_caseload_row
        )
    yearly_col_idx = get_column_index(ColumnHeadingEnum.YEARLY.value)
    if officer_id in officer_aggregated_metrics.yearly_data:
        yearly_data = officer_aggregated_metrics.yearly_data[officer_id]
        try_set_metric_cell(
            sheet,
            yearly_data.avg_daily_population,
            yearly_col_idx,
            avg_daily_caseload_row,
        )


def write_metrics_from_sandbox(
    sheet: Worksheet,
    officer_id: str,
    officer_aggregated_metrics: OfficerAggregatedMetricsFromSandbox,
) -> None:
    """Write aggregated metrics to the appropriate cells in the sheet"""
    early_discharge_row = get_row_index(RowHeadingEnum.EARLY_DISCHARGE_GRANTS)
    lsu_row = get_row_index(RowHeadingEnum.LSU_GRANTS)
    sld_row = get_row_index(RowHeadingEnum.SLD_GRANTS)
    ft_discharge_row = get_row_index(RowHeadingEnum.FT_DISCHARGE_GRANTS)

    for metric in officer_aggregated_metrics.monthly_data[officer_id]:
        parsed_date = (metric.end_date_exclusive - relativedelta(days=1)).strftime(
            _COLUMN_DATE_FORMAT
        )
        col_idx = get_column_index(parsed_date)
        try_set_metric_cell(
            sheet, metric.task_completions_early_discharge, col_idx, early_discharge_row
        )
        try_set_metric_cell(
            sheet,
            metric.task_completions_transfer_to_limited_supervision,
            col_idx,
            lsu_row,
        )
        try_set_metric_cell(
            sheet, metric.task_completions_supervision_level_downgrade, col_idx, sld_row
        )
        try_set_metric_cell(
            sheet,
            metric.task_completions_full_term_discharge,
            col_idx,
            ft_discharge_row,
        )

    yearly_col_idx = get_column_index(ColumnHeadingEnum.YEARLY.value)
    if officer_id in officer_aggregated_metrics.yearly_data:
        yearly_data = officer_aggregated_metrics.yearly_data[officer_id]
        try_set_metric_cell(
            sheet,
            yearly_data.task_completions_early_discharge,
            yearly_col_idx,
            early_discharge_row,
        )
        try_set_metric_cell(
            sheet,
            yearly_data.task_completions_transfer_to_limited_supervision,
            yearly_col_idx,
            lsu_row,
        )
        try_set_metric_cell(
            sheet,
            yearly_data.task_completions_supervision_level_downgrade,
            yearly_col_idx,
            sld_row,
        )
        try_set_metric_cell(
            sheet,
            yearly_data.task_completions_full_term_discharge,
            yearly_col_idx,
            ft_discharge_row,
        )


def write_impact_funnel_metrics(
    sheet: Worksheet, officer_id: str, impact_funnel_metrics: ImpactFunnelMetrics
) -> None:
    task_type_to_row_heading = {
        "EARLY_DISCHARGE": RowHeadingEnum.EARLY_DISCHARGE_ELIGIBLE,
        "TRANSFER_TO_LIMITED_SUPERVISION": RowHeadingEnum.LSU_ELIGIBLE,
        "SUPERVISION_LEVEL_DOWNGRADE": RowHeadingEnum.SLD_ELIGIBLE,
        "FULL_TERM_DISCHARGE": RowHeadingEnum.FT_DISCHARGE_ELIGIBLE,
    }

    for metric in impact_funnel_metrics.data[officer_id]:
        parsed_date = (metric.date - relativedelta(days=1)).strftime(
            _COLUMN_DATE_FORMAT
        )
        col_idx = get_column_index(parsed_date)
        eligible_row = task_type_to_row_heading[metric.task_type]
        try_set_metric_cell(
            sheet, metric.eligible, col_idx, get_row_index(eligible_row)
        )
        if metric.date == date(2025, 1, 1):
            try_set_metric_cell(
                sheet,
                metric.eligible,
                get_column_index(ColumnHeadingEnum.YEARLY.value),
                get_row_index(eligible_row),
            )


def write_grant_rate_formulas(sheet: Worksheet) -> None:
    """Write formulas to calculate the monthly grant rate / average monthly grant rate"""
    avg_caseload_row_idx = get_row_index(RowHeadingEnum.AVG_DAILY_CASELOAD)
    yearly_col_idx = get_column_index(ColumnHeadingEnum.YEARLY.value)
    for month in range(1, 13):
        col_idx = chr(ord(yearly_col_idx) + month)
        for grant_rate_row, num_grants_row in [
            (
                RowHeadingEnum.EARLY_DISCHARGE_MONTHLY_GRANT_RATE,
                RowHeadingEnum.EARLY_DISCHARGE_GRANTS,
            ),
            (
                RowHeadingEnum.LSU_MONTHLY_GRANT_RATE,
                RowHeadingEnum.LSU_GRANTS,
            ),
            (
                RowHeadingEnum.SLD_MONTHLY_GRANT_RATE,
                RowHeadingEnum.SLD_GRANTS,
            ),
            (
                RowHeadingEnum.FT_DISCHARGE_MONTHLY_GRANT_RATE,
                RowHeadingEnum.FT_DISCHARGE_GRANTS,
            ),
        ]:
            grant_rate_row_idx = get_row_index(grant_rate_row)
            num_grants_row_idx = get_row_index(num_grants_row)
            cell = sheet[f"{col_idx}{grant_rate_row_idx}"]
            # Grant rate = # grants / avg caseload. If avg caseload is empty, set grant rate to empty.
            # Excel formula example: =IF(C8, C17/C8, "")
            cell.value = f'=IF({col_idx}{avg_caseload_row_idx}, {col_idx}{num_grants_row_idx}/{col_idx}{avg_caseload_row_idx}, "")'
            cell.number_format = FORMAT_PERCENTAGE_00

    first_date_col = chr(ord(yearly_col_idx) + 1)
    last_date_col = chr(ord(yearly_col_idx) + 12)
    for grant_rate_row in [
        RowHeadingEnum.EARLY_DISCHARGE_MONTHLY_GRANT_RATE,
        RowHeadingEnum.LSU_MONTHLY_GRANT_RATE,
        RowHeadingEnum.SLD_MONTHLY_GRANT_RATE,
        RowHeadingEnum.FT_DISCHARGE_MONTHLY_GRANT_RATE,
    ]:
        grant_rate_row_idx = get_row_index(grant_rate_row)
        cell = sheet[f"{yearly_col_idx}{grant_rate_row_idx}"]
        # Yearly column for grant rate is the average grant rate for each month in the year with data
        cell.value = f"=AVERAGE({first_date_col}{grant_rate_row_idx}:{last_date_col}{grant_rate_row_idx})"
        cell.number_format = FORMAT_PERCENTAGE_00


def generate_sheets(sandbox_prefix: str) -> None:
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    bq_client = BigQueryClientImpl()
    supervisors_to_officers = SupervisorsAndOfficers.from_bigquery(bq_client).data
    officer_aggregated_metrics = OfficerAggregatedMetrics.from_bigquery(bq_client)
    officer_aggregated_metrics_from_sandbox = (
        OfficerAggregatedMetricsFromSandbox.from_bigquery(bq_client, sandbox_prefix)
    )
    impact_funnel_metrics = ImpactFunnelMetrics.from_bigquery(bq_client)
    for supervisor, officers in supervisors_to_officers.items():
        wb = Workbook()
        for officer in officers:
            sheet = wb.create_sheet(title=officer.officer_name.formatted())
            create_headers(sheet)
            apply_conditional_formatting(sheet)
            write_metrics(sheet, officer.officer_id, officer_aggregated_metrics)
            write_metrics_from_sandbox(
                sheet, officer.officer_id, officer_aggregated_metrics_from_sandbox
            )
            write_impact_funnel_metrics(
                sheet, officer.officer_id, impact_funnel_metrics
            )
            write_grant_rate_formulas(sheet)

        # Remove the default sheet, since we created new sheets for our data.
        wb.remove(wb.worksheets[0])
        wb.save(f"{output_dir}/{supervisor.formatted()}.xlsx")


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--sandbox_prefix",
        help="""Prefix of data loaded to a sandbox, for use in metric calculations not provided by our standard materialized views.
        See branch danawillow/id-perf-sandbox for the changes required to be loaded.""",
        type=str,
        required=True,
    )
    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args, _ = parse_arguments(sys.argv)
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        generate_sheets(args.sandbox_prefix)
