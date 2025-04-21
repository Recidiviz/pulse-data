# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query for relevant case notes needed to determine eligibility
for early discharge from supervision in Michigan
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_NAME = (
    "us_mi_complete_discharge_early_from_supervision_request_record"
)

US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_DESCRIPTION = """
    View of relevant case notes for determining eligibility 
    for early discharge from supervision in Michigan 
    """
US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE = f"""
SELECT
    tes.person_id,
    pei.external_id,
    tes.state_code,
    IF(tes.task_name = "COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_SUPERVISION_REQUEST", "Probation", "Parole") 
                AS metadata_supervision_type,
    CASE
    -- 1719: Interstate Compact Parole, 1720: Interstate Compact Probation
    -- Currently clients can meet both criteria, and in these cases the clients mistakenly have `OTHER_STATE` periods
    -- when in reality they are `IC-IN` so we default here to `IC-IN`.
        WHEN ('1719' IN UNNEST(SPLIT((SPLIT(ssp.supervision_type_raw_text, "-"))[SAFE_OFFSET(1)], ","))
                OR '1720' IN UNNEST(SPLIT((SPLIT(ssp.supervision_type_raw_text, "-"))[SAFE_OFFSET(1)],","))) THEN 'IC-IN'
        WHEN ssp.custodial_authority = 'OTHER_STATE' THEN 'IC-OUT'
        ELSE NULL 
        END AS metadata_interstate_flag,
    tes_all.start_date AS metadata_eligible_date,
    reasons,
    tes.is_eligible,
    tes.is_almost_eligible,
FROM (
    SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_discharge_early_from_probation_supervision_request_materialized` 
    UNION ALL
    SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_discharge_early_from_parole_dual_supervision_request_materialized` 
) tes
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON tes.state_code = pei.state_code 
        AND tes.person_id = pei.person_id
        AND pei.id_type = "US_MI_DOC"
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` ssp
    ON ssp.state_code = tes.state_code
    AND ssp.person_id = tes.person_id 
    AND   CURRENT_DATE('US/Pacific') BETWEEN ssp.start_date 
            AND {nonnull_end_date_exclusive_clause('ssp.termination_date')}    
--join views that sessionize spans based on eligibility to get eligible start date
INNER JOIN (
    SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_discharge_early_from_probation_supervision_request__collapsed_materialized` 
    UNION ALL
    SELECT * FROM `{{project_id}}.{{task_eligibility_dataset}}.complete_discharge_early_from_parole_dual_supervision_request__collapsed_materialized` 
) tes_all
ON tes_all.state_code = tes.state_code
    AND tes_all.person_id = tes.person_id 
    AND CURRENT_DATE('US/Pacific') BETWEEN tes_all.start_date AND {nonnull_end_date_exclusive_clause('tes_all.end_date')}
WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
    AND tes.is_eligible
    AND tes.state_code = 'US_MI'
    AND ssp.supervision_type_raw_text LIKE 'ORDER_TYPE_ID_LIST%'
--in the rare case where there are two open supervision periods, dedup so that there is only one record per client
QUALIFY ROW_NUMBER() OVER(PARTITION BY pei.external_id, tes.state_code ORDER BY metadata_interstate_flag DESC)=1
"""

US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MI
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
