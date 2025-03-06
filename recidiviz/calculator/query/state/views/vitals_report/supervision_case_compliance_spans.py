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
"""Sessionized view assigning clients to spans based on various compliance metrics."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_NAME = "supervision_case_compliance_spans"

SUPERVISION_CASE_COMPLIANCE_SPANS_DESCRIPTION = """Sessionized view assigning clients to spans based
on various compliance metrics.

*_required metrics should be interpreted as whether the client has
certain properties (like supervision level) that require regular contacts/assessments, not that they
are required to have that done immediately (that comes from the *_overdue fields)."""

compliance_metric_eligible_supervision_levels_by_state = {
    "US_ND": ("MINIMUM", "MEDIUM", "MAXIMUM"),
    "US_IX": (
        "MINIMUM",
        "MEDIUM",
        "HIGH",
        "MAXIMUM",
        "DIVERSION",
        "INTERSTATE_COMPACT",
        "INTERNAL_UNKNOWN",
    ),
}

eligible_supervision_levels_query_fragment = "\n\tUNION ALL\n".join(
    f"\tSELECT '{state_code}' AS state_code, [{list_to_query_string(supervision_levels, quoted=True)}] AS supervision_levels"
    for state_code, supervision_levels in compliance_metric_eligible_supervision_levels_by_state.items()
)

SUPERVISION_CASE_COMPLIANCE_SPANS_TEMPLATE = f"""
WITH eligible_supervision_levels_by_state AS (
{eligible_supervision_levels_query_fragment}
)
, compliance_metrics AS (
    SELECT
        m.state_code,
        m.person_id,
        date_of_supervision AS start_date,
        DATE_ADD(date_of_supervision, INTERVAL 1 DAY) AS end_date_exclusive,
        supervision_level IN UNNEST(c.supervision_levels) AS assessment_required,
        -- If there are multiple rows for a person on a day and any row says they're overdue, consider them overdue
        LOGICAL_OR(IFNULL(next_recommended_assessment_date < date_of_supervision, FALSE)) AS assessment_overdue,
        supervision_level IN UNNEST(c.supervision_levels) AS contact_required,
        LOGICAL_OR(IFNULL(next_recommended_face_to_face_date < date_of_supervision, FALSE)) AS contact_overdue
    FROM
        `{{project_id}}.shared_metric_views.supervision_case_compliance_metrics_materialized` m
    INNER JOIN
        eligible_supervision_levels_by_state c
        USING (state_code)
    LEFT JOIN `{{project_id}}.sessions.location_sessions_materialized` l
        ON m.person_id = l.person_id
        AND date_of_supervision BETWEEN l.start_date AND {nonnull_end_date_exclusive_clause("l.end_date_exclusive")}
    WHERE
        -- We only need data for the last 6 months for Operations, so filter earlier dates out now to
        -- avoid needing to do extra processing for data we aren't actually using
        date_of_supervision >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 6 MONTH)
        AND NOT (m.state_code="US_ND" AND IFNULL(l.supervision_office_name, "") LIKE "%PRETRIAL")
        AND supervision_type != "INVESTIGATION"
    GROUP BY 1, 2, 3, 4, 5, 7
)
, aggregated_compliance_spans AS (
    {aggregate_adjacent_spans(
        table_name='compliance_metrics',
        attribute=['assessment_required', 'assessment_overdue', 'contact_required', 'contact_overdue'],
        end_date_field_name='end_date_exclusive'
    )}
)
SELECT * FROM aggregated_compliance_spans
"""

SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_NAME,
    view_query_template=SUPERVISION_CASE_COMPLIANCE_SPANS_TEMPLATE,
    description=SUPERVISION_CASE_COMPLIANCE_SPANS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_BUILDER.build_and_print()
