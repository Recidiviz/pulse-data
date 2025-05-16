# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View that logs earned credit activity in US_MA"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_NAME = (
    "us_ma_earned_credit_activity_preprocessed"
)

US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_DESCRIPTION = """View that logs earned credit activity in US_MA and is
built off of raw data. This view is added into a state agnostic version `analyst_data.earned_credit_activity` which
stores the activity columns as a json 'activity_attributes'"""

US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_QUERY_TEMPLATE = """
SELECT
    pei.state_code,
    pei.person_id,
    CAST(CAST(period_ending AS DATETIME) AS DATE) AS credit_date,
    --TODO(#42452): Write validation to ensure that we are not dropping any credit data which could happen if the
    --activity string is not formatted as expected
    /*
    Credit types that are not "EARNED_GOOD_TIME" are identified based on the ACTIVITY string which is suffixed with
    "(COMPLETION)" or "(BOOST)" to indicate credits of that type.
    */
    CASE WHEN ACTIVITY LIKE '%(COMPLETION)' THEN 'COMPLETION'
        WHEN ACTIVITY LIKE '%(BOOST)' THEN 'BOOST'
        ELSE 'EARNED_GOOD_TIME' END AS credit_type,
    /*
    Completion credits are stored in a different column than boost or egt credits. When a person has a row with
    completion credits they have a value of 0 in the other credits field. Therefore, all of the credit values can be
    combined into one column. That is what this case statement is doing.
    */
    CASE WHEN ACTIVITY LIKE '%(COMPLETION)' THEN CAST(COMPLETION_CREDIT AS NUMERIC)
        ELSE CAST(STATE_DAYS_GRANTED AS NUMERIC) END AS credits_earned,
    activity_code,
    activity,
    activity_type,
    rating,
    activity_seq,
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.egt_report_latest` egt
LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON egt.COMMIT_NO = pei.external_id
    AND pei.state_code='US_MA'
"""

US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    view_id=US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_NAME,
    description=US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_QUERY_TEMPLATE,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_BUILDER.build_and_print()
