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
"""Types for PO Monthly Report context."""


from typing import List, Literal, Optional, TypedDict, Union

from recidiviz.reporting.context.po_monthly_report.constants import (
    OfficerHighlightComparison,
    OfficerHighlightType,
)

# 2D array of rows > cells
TableData = List[List[str]]

DecarceralMetricContext = TypedDict(
    "DecarceralMetricContext",
    {
        "icon": str,
        "heading": str,
        # total and main_text should be joined in template via .format()
        "total": int,
        "main_text": str,
        "supplemental_text": Optional[str],
        "action_table": Optional[TableData],
        "action_text": Optional[str],
    },
)


class _AdverseOutcomeRequired(TypedDict):
    label: str
    count: int


class AdverseOutcomeContext(_AdverseOutcomeRequired, total=False):
    zero_streak: int
    amount_above_average: float


class OfficerHighlightMetricsComparison(TypedDict):
    """A highlight that is explicitly compared to a specific benchmark."""

    type: Literal[
        OfficerHighlightType.MOST_DECARCERAL,
        OfficerHighlightType.LONGEST_ADVERSE_ZERO_STREAK,
    ]
    metrics: List[str]
    compared_to: OfficerHighlightComparison


class OfficerHighlightMetrics(TypedDict):
    """A highlight involving one or more metrics. May be a comparison,
    but not one that needs to be specified (i.e. it's implied by the type)."""

    type: Literal[OfficerHighlightType.ABOVE_AVERAGE_DECARCERAL]
    metrics: List[str]


OfficerHighlight = Union[OfficerHighlightMetrics, OfficerHighlightMetricsComparison]


class ComplianceTaskContext(TypedDict):
    pct: Optional[float]
    num_completed: int
    goal: int
    goal_pct: Optional[float]
    show_goal: bool
    goal_met: bool
    metric_label: str
    metric: str
    upcoming_clients: Optional[TableData]
    upcoming_overflow_text: Optional[str]
    overdue_clients: Optional[TableData]
    overdue_overflow_text: Optional[str]
