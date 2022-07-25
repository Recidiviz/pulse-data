# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Creates a view of current client status for the Compliant Reporting Workflow"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_NAME = (
    "us_tn_compliant_reporting_workflow_status"
)

US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_DESCRIPTION = (
    """Creates a view of current client status for the Compliant Reporting Workflow"""
)

US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_QUERY_TEMPLATE = """
    WITH
    eligible_clients AS (
      SELECT
            person_id,
            ARRAY_AGG(compliant_reporting_eligible ORDER BY date_of_supervision DESC)[OFFSET(0)] AS compliant_reporting_eligible,
            ARRAY_AGG(district ORDER BY date_of_supervision DESC)[OFFSET(0)] AS district,
            ARRAY_AGG(officer_id ORDER BY date_of_supervision DESC)[OFFSET(0)] AS officer_id,
            -- Note: we are not currently accounting for people who become ineligible and then eligible again;
            -- once eligibility data is sessionized we will want to revise this logic to capture those cases.
            -- In the meantime we are effectively collapsing multiple periods for the same person into one,
            -- which should have minimal short-term impact as people should not be cycling in and out of eligibility
            -- with high frequency (the minimum expected turnaround is 3 months)
            MIN(date_of_supervision) AS eligible_start,
            MAX(date_of_supervision) AS eligible_end,
        FROM `{project_id}.{workflows_dataset}.client_record_archive_materialized`
        WHERE compliant_reporting_eligible IS NOT NULL
        GROUP BY 1
    )
    , surfaced AS (
        SELECT
            person_id,
            DATE(MIN(timestamp), "US/Eastern") as first_surfaced,
        FROM `{project_id}.{workflows_dataset}.clients_surfaced`
        WHERE opportunity_type = 'compliantReporting'
        GROUP BY 1
    )
    , viewed AS (
        SELECT
            person_id,
            DATE(MIN(timestamp), "US/Eastern") as first_viewed,
        FROM `{project_id}.{workflows_dataset}.clients_referral_form_viewed`
        WHERE opportunity_type = 'compliantReporting'
        GROUP BY 1
    )

    , level_downgraded AS (
        SELECT
          person_id,
          event_date AS downgraded,
          opportunity_type,
        FROM `{project_id}.{workflows_dataset}.clients_referral_implemented`
        WHERE opportunity_type = 'compliantReporting'
    )

    , latest_status AS (
      SELECT
        person_id,
        form_status.status AS form_status,
        CASE
          WHEN downgraded IS NOT NULL THEN "TRANSFERRED"
          ELSE form_status.status
        END AS status,
        IFNULL(downgraded, DATE(form_status.timestamp, "US/Eastern")) AS last_updated,
      FROM `{project_id}.{workflows_dataset}.clients_latest_referral_status` form_status
      FULL OUTER JOIN level_downgraded USING (person_id, opportunity_type)
      WHERE opportunity_type = 'compliantReporting'

    )

    SELECT
        eligible_clients.*,
        first_surfaced,
        first_viewed,
        COALESCE(latest_status.last_updated, eligible_start) AS last_updated,
        IFNULL(latest_status.form_status, "NOT_STARTED") AS form_status,
        IFNULL(latest_status.status, "NOT_STARTED") AS status,
        downgraded,
    FROM eligible_clients
    LEFT JOIN surfaced USING (person_id)
    LEFT JOIN viewed USING (person_id)
    LEFT JOIN latest_status USING (person_id)
    LEFT JOIN level_downgraded USING (person_id)
"""

US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_QUERY_TEMPLATE,
    should_materialize=True,
    workflows_dataset=WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_BUILDER.build_and_print()
