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
Queries information needed to surface eligible folks to be early released on a Drug Transition Program in AZ.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_NAME = (
    "us_az_approaching_acis_or_recidiviz_dtp_request_record"
)

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_DESCRIPTION = """
    Queries information needed to surface eligible folks to be early released on a Drug Transition Program in AZ.
    """


US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_QUERY_TEMPLATE = f"""
# TODO(#33958) - Add all the relevant components of this opportunity record
WITH recidiviz_dtp_date_approaching AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code="'US_AZ'",
    tes_task_query_view='overdue_for_recidiviz_dtp_request_materialized',
    id_type="'US_AZ_ADC_NUMBER'",
    almost_eligible_only=True)}
),

acis_dtp_date_approaching AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code="'US_AZ'",
    tes_task_query_view='overdue_for_acis_dtp_request_materialized',
    id_type="'US_AZ_ADC_NUMBER'",
    almost_eligible_only=True)}
),

combine_acis_and_recidiviz_dtp_dates AS (
    -- Fast track: ACIS DTP date within 1 days and 30 days
    SELECT
        * EXCEPT(criteria_reason),
        "FAST_TRACK" AS metadata_tab_description,
    FROM acis_dtp_date_approaching,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    WHERE "US_AZ_INCARCERATION_PAST_ACIS_DTP_DATE" IN UNNEST(ineligible_criteria)
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.criteria_name') AS STRING) = "US_AZ_INCARCERATION_PAST_ACIS_DTP_DATE"
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_dtp_date') AS DATE) BETWEEN 
            DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY) AND DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY)

    UNION ALL

    -- Approved by Time Comp: ACIS DTP date within 31 days and 180 days
    SELECT
        * EXCEPT(criteria_reason),
        "APPROVED_BY_TIME_COMP" AS metadata_tab_description 
    FROM acis_dtp_date_approaching,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    WHERE "US_AZ_INCARCERATION_PAST_ACIS_DTP_DATE" IN UNNEST(ineligible_criteria)
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.criteria_name') AS STRING) = "US_AZ_INCARCERATION_PAST_ACIS_DTP_DATE"
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_dtp_date') AS DATE) BETWEEN 
            DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 31 DAY) AND DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 180 DAY)

    UNION ALL

    -- Almost eligible section 1: Projected DTP date within 7 days and 180 days
    # TODO(#33958) - recidiviz_tpr_date_approaching needs to be split into section 1 and 2
    SELECT 
        *,
        "ALMOST_ELIGIBLE_SECTION_1" AS metadata_tab_description 
    FROM recidiviz_dtp_date_approaching

    -- Almost eligible section 2: Projected DTP date within 181 days and 365 days
)

SELECT 
    external_id,
    state_code,
    reasons,
    ineligible_criteria,
    metadata_tab_description,
FROM 
    combine_acis_and_recidiviz_dtp_dates
"""

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_AZ
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
