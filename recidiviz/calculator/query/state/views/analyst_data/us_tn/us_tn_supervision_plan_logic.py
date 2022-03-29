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
"""Creates a view to identify individuals current and previous supervision levels"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUPERVISION_PLAN_LOGIC_VIEW_NAME = "us_tn_supervision_plan_logic"

US_TN_SUPERVISION_PLAN_LOGIC_VIEW_DESCRIPTION = (
    """Creates a view to identify individuals eligible for Compliant Reporting"""
)

US_TN_SUPERVISION_PLAN_LOGIC_QUERY_TEMPLATE = """
    SELECT * , 
            LAG(SupervisionLevelClean) OVER(partition by Offender_ID ORDER BY PlanStartDate ASC) AS prev_level,
            LAG(PlanStartDate) OVER(partition by Offender_ID ORDER BY PlanStartDate ASC) AS prev_start,
            DATE_DIFF(current_date('US/Eastern'),PlanStartDate, MONTH) AS time_since_current_start,
            DATE_DIFF(current_date('US/Eastern'),LAG(PlanStartDate) OVER(partition by Offender_ID ORDER BY PlanStartDate ASC), MONTH) AS time_since_prev_start
        FROM (
            SELECT OffenderID as Offender_ID, 
            CAST(CAST(PlanStartDate AS datetime) AS DATE) AS PlanStartDate,
            CAST(CAST(PlanEndDate AS datetime) AS DATE) AS PlanEndDate,
            SupervisionLevel,
            PlanType,
            -- Recategorize raw text into MINIMUM and MEDIUM
            -- TODO(#11799): Confirm full list of codes for minimum and medium supervision level in supervision plan
            CASE WHEN SupervisionLevel IN ('4MI', 'MIN', 'Z1A', 'Z1M','ZMI' ) THEN 'MINIMUM' 
                WHEN SupervisionLevel IN ('4ME','KG2','KN2','MED','QG2','QN2','VG2','VN2','XG2','XMD','XN2','Z2A','Z2M','ZME') THEN 'MEDIUM'
                END AS SupervisionLevelClean
            FROM `{project_id}.us_tn_raw_data_up_to_date_views.SupervisionPlan_latest`
            )
"""

US_TN_SUPERVISION_PLAN_LOGIC_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_SUPERVISION_PLAN_LOGIC_VIEW_NAME,
    description=US_TN_SUPERVISION_PLAN_LOGIC_VIEW_DESCRIPTION,
    view_query_template=US_TN_SUPERVISION_PLAN_LOGIC_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_PLAN_LOGIC_VIEW_BUILDER.build_and_print()
