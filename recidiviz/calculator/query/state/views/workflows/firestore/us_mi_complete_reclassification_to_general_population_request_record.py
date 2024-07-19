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
"""Query for clients eligible or almost eligible for reclassification to general population from
solitary confinement in Michigan"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.almost_eligible_query_fragments import (
    clients_eligible,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ID_TYPE_BOOK = "US_MI_DOC_BOOK"
ID_TYPE = "US_MI_DOC"

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_NAME = (
    "us_mi_complete_reclassification_to_general_population_request_record"
)

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_DESCRIPTION = """
    Query for clients eligible or almost eligible for reclassification to general population from 
    solitary confinement in Michigan
"""

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_QUERY_TEMPLATE = f"""

WITH current_population AS (
/* Queries all current eligible spans, and all ineligible spans followed by an eligible span. 
It also associates the current span with the reasons for the next eligible span */
   SELECT
        pei.external_id,
        tes.person_id,
        tes.state_code,
        tes.reasons,
        --pull reasons blob from the upcoming eligible span 
        tes.next_reasons,
        tes.ineligible_criteria,
        tes.is_eligible,
        tes.is_almost_eligible,
    FROM (
        SELECT 
            c.*,
            LEAD(is_eligible) OVER (PARTITION BY person_id ORDER BY start_date) AS next_eligibility,
            LEAD(reasons) OVER (PARTITION BY person_id ORDER BY start_date) AS next_reasons,
        FROM 
            `{{project_id}}.{{task_eligibility_dataset}}.complete_reclassification_to_general_population_request_materialized` c
        ) AS tes
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON pei.person_id = tes.person_id 
            AND pei.id_type = 'US_MI_DOC' 
        WHERE 
           CURRENT_DATE('US/Eastern') BETWEEN tes.start_date AND 
                                         {nonnull_end_date_exclusive_clause('tes.end_date')}
            --select spans of time where someone is either eligible, or their next span is eligible
            AND (next_eligibility OR is_eligible)
),
reasons_for_eligibility AS (
/* Queries the reasons for eligibility from the upcoming eligible span */
    SELECT 
        * EXCEPT(array_next_reasons),
        CAST(
                ARRAY(
                SELECT JSON_VALUE(x.reason.overdue_in_temporary)
                FROM UNNEST(array_next_reasons) AS x
                WHERE STRING(x.criteria_name) = 'US_MI_ELIGIBLE_FOR_RECLASSIFICATION_FROM_SOLITARY_TO_GENERAL'
                )[OFFSET(0)]
        AS BOOL)  AS overdue_in_temporary,
        CAST(
                ARRAY(
                SELECT JSON_VALUE(x.reason.overdue_in_temporary_date)
                FROM UNNEST(array_next_reasons) AS x
                WHERE STRING(x.criteria_name) = 'US_MI_ELIGIBLE_FOR_RECLASSIFICATION_FROM_SOLITARY_TO_GENERAL'
                )[OFFSET(0)]
        AS DATE)  AS overdue_in_temporary_date,
    FROM 
    ( SELECT 
        *,
        JSON_QUERY_ARRAY(next_reasons) AS array_next_reasons
        FROM current_population)
    WHERE ARRAY_LENGTH(array_next_reasons) > 0
),
eligible_and_almost_eligible AS (
    -- ELIGIBLE
    {clients_eligible(from_cte = 'reasons_for_eligibility')}
    UNION ALL
    -- ALMOST ELIGIBLE (7 days in temporary segregation)
    SELECT
        * EXCEPT (is_almost_eligible)
    FROM reasons_for_eligibility
    --add ineligible spans where the next span is eligible because the resident is overdue in temporary
    --and their overdue_in_temporary_date is < 23 days away 
    WHERE NOT is_eligible 
        AND overdue_in_temporary
        AND DATE_DIFF(overdue_in_temporary_date, CURRENT_DATE('US/Eastern'), DAY) < 23
)
SELECT 
    a.external_id,
    a.state_code,
    a.reasons,
    a.is_eligible,
    a.ineligible_criteria,
    h.housing_unit_type AS metadata_solitary_confinement_type,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), h.start_date, DAY) AS metadata_days_in_solitary
FROM eligible_and_almost_eligible a
LEFT JOIN
    `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized` h
        ON h.person_id = a.person_id 
        AND CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_exclusive_clause('h.end_date_exclusive')}
"""

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MI
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
