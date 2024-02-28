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
"""Query for clients past their security committee classification review date in Michigan"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ID_TYPE_BOOK = "US_MI_DOC_BOOK"
ID_TYPE = "US_MI_DOC"

US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_NAME = (
    "us_mi_complete_security_committee_classification_review_form_record"
)

US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_DESCRIPTION = """
    Query for clients past their security committee classification review date in Michigan
"""

US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_QUERY_TEMPLATE = f"""

WITH eligible_population AS (
{join_current_task_eligibility_spans_with_external_id(state_code= "'US_MI'", 
    tes_task_query_view = 'complete_security_classification_committee_review_form_materialized',
    id_type = "'US_MI_DOC'")}
),
release_dates AS (
/* queries raw data for the min and max release dates */ 
    SELECT 
        offender_number,
        MAX(pmi_sgt_min_date) AS pmi_sgt_min_date, 
        MAX(pmx_sgt_max_date) AS pmx_sgt_max_date,
    FROM `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.ADH_OFFENDER_SENTENCE_latest`
    INNER JOIN `{{project_id}}.{{us_mi_raw_data_up_to_date_dataset}}.ADH_OFFENDER_latest` 
        USING(offender_id)
    WHERE sentence_type_id = '430' -- sentence is a prison sentence
        AND closing_date is NULL --sentence is open 
    GROUP BY 1
),
bondable_codes AS (
/* queries all bondable codes for misconduct reports in the last 6 months */
    SELECT
        person_id,
        incident_date,
         CASE 
          WHEN JSON_EXTRACT_SCALAR(incident_metadata, '$.BONDABLE_OFFENSES') = '' THEN NULL 
          ELSE SPLIT(JSON_EXTRACT_SCALAR(incident_metadata, '$.BONDABLE_OFFENSES'), ',') 
        END AS bondable_offenses
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident`
    WHERE 
        state_code = "US_MI"
        AND DATE_ADD(incident_date, INTERVAL 6 MONTH) >= CURRENT_DATE('US/Eastern')
    ),
nonbondable_codes AS (
/* queries all nonbondable codes for misconduct reports in the last year */
    SELECT
        person_id,
        incident_date,
         CASE 
          WHEN JSON_EXTRACT_SCALAR(incident_metadata, '$.NONBONDABLE_OFFENSES') = '' THEN NULL 
          ELSE SPLIT(JSON_EXTRACT_SCALAR(incident_metadata, '$.NONBONDABLE_OFFENSES'), ',') 
        END AS nonbondable_offenses
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident`
    WHERE 
        state_code = "US_MI"
        AND DATE_ADD(incident_date, INTERVAL 1 YEAR) >= CURRENT_DATE('US/Eastern')
),
misconduct_codes AS (
/* For bondable codes, the resident can remain in their current cell. 
For nonbondable code, the resident must be moved to segregation until ticket is heard */
SELECT 
    b.person_id,
    CONCAT('(', STRING_AGG(DISTINCT CONCAT(bondable_offense, ', ', STRING(b.incident_date)), '), ('), ')') AS bondable_offenses_within_6_months,
    CONCAT('(', STRING_AGG(DISTINCT CONCAT(nonbondable_offense, ', ', STRING(n.incident_date)), '), ('), ')') AS nonbondable_offenses_within_1_year
FROM bondable_codes b
CROSS JOIN 
  UNNEST(bondable_offenses) AS bondable_offense
LEFT JOIN nonbondable_codes n
  ON b.person_id = n.person_id 
CROSS JOIN 
  UNNEST(n.nonbondable_offenses) AS nonbondable_offense
GROUP BY 1
),
previous_ad_seg_stays AS (
SELECT 
  person_id,
  ARRAY_AGG(CONCAT('(', STRING(start_date), ', ', nonbondable_offenses, ')')) AS ad_seg_stays_and_reasons_within_3_yrs
FROM (
  SELECT 
    h.person_id,
    h.start_date,
    COALESCE(JSON_EXTRACT_SCALAR(i.incident_metadata, '$.NONBONDABLE_OFFENSES'), "") AS nonbondable_offenses
  FROM 
    `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized` h
  LEFT JOIN 
    `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` i
  ON 
    h.state_code = i.state_code
    AND h.person_id = i.person_id 
    AND i.incident_date <= h.start_date
  WHERE housing_unit_type = 'ADMINISTRATIVE_SOLITARY_CONFINEMENT'
    AND DATE_ADD(h.start_date, INTERVAL 3 YEAR) >= CURRENT_DATE('US/Eastern')
    --choose the most recent incident report that happens before the administrative solitary start 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY h.person_id, h.start_date ORDER BY incident_date DESC) = 1
)
GROUP BY person_id
)
SELECT 
tes.state_code,
tes.external_id AS external_id,
tes.reasons,
tes.external_id AS form_information_prisoner_number,
INITCAP(JSON_VALUE(PARSE_JSON(sp.full_name), '$.given_names'))
        || " " 
        || INITCAP(JSON_VALUE(PARSE_JSON(sp.full_name), '$.surname')) AS form_information_prisoner_name,
DATE(pmx_sgt_max_date) AS form_information_max_release_date,
DATE(pmi_sgt_min_date) AS form_information_min_release_date,
si.housing_unit AS form_information_lock,
m.bondable_offenses_within_6_months AS form_information_bondable_offenses_within_6_months,
m.nonbondable_offenses_within_1_year AS form_information_nonbondable_offenses_within_1_year,
p.ad_seg_stays_and_reasons_within_3_yrs AS form_information_ad_seg_stays_and_reasons_within_3_yrs,
FROM eligible_population tes
LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` sp
    ON tes.state_code = sp.state_code
    AND tes.person_id = sp.person_id
LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` si
    ON tes.state_code = si.state_code
    AND tes.person_id = si.person_id
    AND CURRENT_DATE('US/Eastern') BETWEEN si.admission_date AND {nonnull_end_date_clause('si.release_date')}
LEFT JOIN release_dates sgt
    ON sgt.offender_number = tes.external_id 
LEFT JOIN misconduct_codes m
    ON tes.person_id = m.person_id
LEFT JOIN previous_ad_seg_stays p
    ON tes.person_id = p.person_id
"""

US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_NAME,
    view_query_template=US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_QUERY_TEMPLATE,
    description=US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MI
    ),
    us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_COMPLETE_SECURITY_COMMITTEE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.build_and_print()
