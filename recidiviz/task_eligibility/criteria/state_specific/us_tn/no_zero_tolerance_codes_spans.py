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
# ============================================================================
"""Describes the spans of time when a TN client on Probation has no zero-tolerance codes recorded
within their sentence span. Probation clients with zero-tolerance codes recorded have a high
likelihood of missing ingested sentencing data in TN. Parole clients are excluded as they do not
have missing sentence data issues.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_ZERO_TOLERANCE_CODES_SPANS"

_DESCRIPTION = """Describes the spans of time when a TN client on Probation has no zero-tolerance codes recorded
within their sentence span. Probation clients with zero-tolerance codes recorded have a high
likelihood of missing ingested sentencing data in TN. Parole clients are excluded as they do not
have missing sentence data issues.
"""

_QUERY_TEMPLATE = f"""
    WITH zt_codes AS (
      SELECT pei.person_id, 
              CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
              ContactNoteType AS contact_type,
      FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ContactNoteType_latest`
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
              ON pei.external_id = OffenderID
              AND pei.state_code = 'US_TN'
      INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
            ON pei.person_id = sp.person_id
            AND CAST(CAST(ContactNoteDateTime AS datetime) AS DATE)
                BETWEEN sp.start_date AND {nonnull_end_date_exclusive_clause('sp.termination_date')}
      WHERE ContactNoteType IN ('VWAR','PWAR','ZTVR','COHC')
            /* We only want to consider zero tolerance contact codes that occur when someone is serving something
            other than parole, since we expect sentencing data to be more complete (and discharge dates to be less
            adjustable) for people on parole */ 
            AND sp.supervision_type_raw_text != 'PAO'
    ),
    critical_date_spans AS (
        /* This CTE uses sentence spans to get date imposed for each unique sentence being served during each span,
        and brings in zero tolerance codes that occurred after each date imposed. This allows us to create spans
        of time when the critical date (zero tolerance contact code date) has passed */
        SELECT
            s.person_id,
            s.state_code,
            s.start_date AS start_datetime,
            /* Setting latest sentence span end date to NULL so that if the zero tolerance code occurred after the 
                latest sentence start, we're still flagging that as critical date has passed since it signals 
                potentially missing sentencing data */
            IF(
                MAX({nonnull_end_date_exclusive_clause('s.end_date')}) OVER(PARTITION BY s.person_id) = {nonnull_end_date_exclusive_clause('s.end_date')},
                NULL,
                s.end_date
            ) end_datetime,
            zt_codes.contact_date AS critical_date, 
        FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` s,
        UNNEST(sentences_preprocessed_id_array) as sentences_preprocessed_id
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sentences
          USING(person_id, state_code, sentences_preprocessed_id)
        LEFT JOIN zt_codes
          ON zt_codes.person_id = s.person_id
          AND zt_codes.contact_date > sentences.date_imposed
    ),
    /* The critical_date_has_passed_spans_cte() method creates spans of time when the critical date 
    (zero tolerance contact code date) has passed. The output has overlapping and non-collapsed adjacent spans.
    The create_sub_sessions_with_attributes() helper takes that input and outputs non-overlapping spans (i.e there
    may be multiple rows for a given start/end date which preserve input attributes. The final step is to collapse
    those */
    {critical_date_has_passed_spans_cte()},
    {create_sub_sessions_with_attributes('critical_date_has_passed_spans')},
    grouped_dates AS (
         SELECT
            person_id, 
            state_code,
            start_date,
            end_date,
            -- Someone meets the criteria if there are no zero tolerance contact codes in a given span
            NOT LOGICAL_OR(critical_date_has_passed) AS meets_criteria,
            TO_JSON(STRUCT(ARRAY_AGG(critical_date IGNORE NULLS))) AS reason,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        IF(
            MAX({nonnull_end_date_exclusive_clause('end_date')}) OVER(PARTITION BY person_id) = {nonnull_end_date_exclusive_clause('end_date')},
            NULL, 
            end_date
        ) AS end_date,
        meets_criteria,
        reason
    FROM grouped_dates
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN,
            instance=DirectIngestInstance.PRIMARY,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
