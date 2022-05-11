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
"""View that defines funnels for the Compliant Reporting Workflow"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_NAME = "us_tn_compliant_reporting_funnel"

US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_DESCRIPTION = (
    """View that defines funnels for the Compliant Reporting Workflow"""
)

US_TN_COMPLIANT_REPORTING_FUNNEL_QUERY_TEMPLATE = """
    SELECT
    person_id,
    compliant_reporting_eligible,
    status,
    "Eligible" AS step,
    eligible_start AS start,
    district,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_workflow_status_materialized`

    UNION ALL

    SELECT
    person_id,
    compliant_reporting_eligible,
    status,
    "Surfaced" AS step,
    first_surfaced AS start,
    district,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_workflow_status_materialized`
    WHERE first_surfaced IS NOT NULL

    UNION ALL

    SELECT
    person_id,
    compliant_reporting_eligible,
    status,
    "Form viewed" AS step,
    first_viewed AS start,
    district,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_workflow_status_materialized`
    WHERE first_viewed IS NOT NULL

    UNION ALL

    SELECT
    person_id,
    compliant_reporting_eligible,
    status,
    "Form completed" AS step,
    last_updated AS start,
    district,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_workflow_status_materialized`
    WHERE form_status = "COMPLETED"

    UNION ALL

    SELECT
    person_id,
    compliant_reporting_eligible,
    status,
    "Transferred" AS step,
    downgraded AS start,
    district,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_workflow_status_materialized`
    WHERE status = "TRANSFERRED"

    UNION ALL

    SELECT
    person_id,
    compliant_reporting_eligible,
    status,
    "Marked ineligible" AS step,
    last_updated AS start,
    district,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_workflow_status_materialized`
    WHERE form_status = "DENIED"
"""

US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_FUNNEL_QUERY_TEMPLATE,
    should_materialize=True,
    analyst_dataset=ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER.build_and_print()
