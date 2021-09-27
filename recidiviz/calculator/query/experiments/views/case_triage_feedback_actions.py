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
"""Creates the view builder and view for user feedback actions from case triage."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import (
    CASE_TRIAGE_SEGMENT_DATASET,
    EXPERIMENTS_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    PO_REPORT_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_NAME = "case_triage_feedback_actions"

CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_DESCRIPTION = """
All case feedback actions taken by an officer within the Case Triage app for an
individual client on a single day (day boundary defined in the PST time zone).
Case feedback can include updates around a client's data quality (incorrect data),
assessment information, supervision downgrades or discharges, etc.
Case feedback that is removed/reverted within 3 days of the original feedback is
excluded from the results.
"""

CASE_TRIAGE_FEEDBACK_ACTIONS_QUERY_TEMPLATE = """
    SELECT DISTINCT
        recipients.state_code,
        recipients.officer_external_id,
        actions.user_id AS segment_id,
        COALESCE(district.district, 'INTERNAL_UNKNOWN') AS district,
        client.person_id,
        actions.person_external_id,
        EXTRACT(DATE from actions.timestamp AT TIME ZONE 'US/Pacific') AS feedback_date,
        actions.action_taken AS feedback_type,
    FROM `{project_id}.{case_triage_segment_dataset}.frontend_person_action_taken` actions
    INNER JOIN `{project_id}.{static_reference_tables_dataset}.case_triage_users` recipients
        ON actions.user_id = recipients.segment_id
    LEFT JOIN `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` district
        ON district.officer_external_id = recipients.officer_external_id
        AND district.state_code = recipients.state_code
        AND DATE_TRUNC(DATE(actions.timestamp), MONTH) = DATE(district.year, district.month, 1)
    LEFT JOIN `{project_id}.{analyst_dataset}.person_demographics_materialized` client
        ON recipients.state_code = client.state_code
        AND actions.person_external_id = client.prioritized_external_id

    -- Do not include any feedback actions that were reverted within 3 days of the original action
    LEFT JOIN `{project_id}.{case_triage_segment_dataset}.frontend_person_action_removed` action_removed
        ON actions.user_id = action_removed.user_id
        AND actions.person_external_id = action_removed.person_external_id
        AND actions.action_taken = action_removed.action_removed
        AND action_removed.timestamp BETWEEN actions.timestamp
            AND TIMESTAMP_ADD(actions.timestamp, INTERVAL 72 HOUR)
    WHERE action_removed.id IS NULL
"""

CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_NAME,
    view_query_template=CASE_TRIAGE_FEEDBACK_ACTIONS_QUERY_TEMPLATE,
    description=CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    po_report_dataset=PO_REPORT_DATASET,
    case_triage_segment_dataset=CASE_TRIAGE_SEGMENT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_BUILDER.build_and_print()
