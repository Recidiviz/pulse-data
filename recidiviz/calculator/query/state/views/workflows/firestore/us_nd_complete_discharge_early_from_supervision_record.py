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
"""Query for information needed to fill out the early termination form for North Dakota
for individuals that are currently eligible
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.criteria.state_specific.us_nd.implied_valid_early_termination_supervision_level import (
    _CRITERIA_NAME as supervision_level_criteria,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_NAME = (
    "us_nd_complete_discharge_early_from_supervision_record"
)

US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_DESCRIPTION = """
    View of early termination record for form completion for individuals that are
    currently eligible for early termination 
    """

US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_QUERY_TEMPLATE = """
WITH probation_officer AS(
  /* This CTE creates a view unique on external_id
   for non null officer names */
  SELECT
    full_name_json AS full_name,
    legacy_supervising_officer_external_id AS supervising_officer_external_id
  FROM `{project_id}.reference_views.state_staff_with_names` ss
  WHERE state_code = 'US_ND' AND full_name_json IS NOT NULL
),

dataflow_metrics AS (
    /* This CTE creates a view for the current supervision population
    unique on person_id by selecting non null judicial and supervising_officer_external_id fields*/
  SELECT
      dataflow.state_code,
      person_id,
      staff.external_id AS supervising_officer_external_id,
      full_name
  FROM `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_population_span_metrics_materialized` dataflow
    LEFT JOIN
        `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
    ON
        dataflow.supervising_officer_staff_id = staff.staff_id
  LEFT JOIN
    probation_officer
  ON
    staff.external_id = probation_officer.supervising_officer_external_id
  WHERE dataflow.state_code = 'US_ND' AND dataflow.included_in_state_population
    AND dataflow.end_date_exclusive IS NULL
  --choose only one judicial district per individual since individuals with overlapping supervision sentences
  --will have multiple rows in dataflow
  QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY person_id ORDER BY supervising_officer_external_id DESC)=1
),

individual_sentence_charges AS (
  /* This CTE pulls data unique on individual, sentence and charge */
  SELECT 
  sent.person_id,
  pei.external_id AS person_external_id,
  sent.state_code,
  sent.date_imposed AS prior_court_date, --need to validate
  sent.completion_date,
  DATE_DIFF(sent.projected_completion_date, sent.date_imposed, YEAR) AS sentence_years,
  DATE_DIFF(sent.projected_completion_date, sent.date_imposed, MONTH) AS sentence_months,
  sent.projected_completion_date AS probation_expiration_date,
  REGEXP_SUBSTR(charge.county_code, "_ND_(.*)") AS conviction_county,
  offense.court_number AS criminal_number,
  sent.supervision_sentence_id,
  sent.external_id AS supervision_external_id,
  sent.supervision_type_raw_text AS supervision_type,
  charge.external_id AS charge_external_id,
  charge.classification_type AS crime_classification,
  charge.classification_subtype AS crime_subclassification,
  TRIM(
    COALESCE(
      charge.judicial_district_code,
      scc.judicial_district_code
    )
  ) as judicial_district_code,
  INITCAP(JSON_VALUE(PARSE_JSON(charge.judge_full_name), '$.full_name')) AS judge,
  CONCAT(
      COALESCE(charge.description, charge.description, "<to fill>"), 
      IFNULL(CONCAT(" a Class (", charge.classification_subtype, ") "), " a Class (<to fill>) "),
      IFNULL(INITCAP(charge.classification_type), " <to fill>")
    ) AS crime_name,
  FROM `{project_id}.{normalized_state_dataset}.state_supervision_sentence`  sent
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON sent.state_code = pei.state_code 
    AND sent.person_id = pei.person_id
    AND pei.id_type = "US_ND_SID"
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_charge_supervision_sentence_association` ssa
    ON sent.state_code = ssa.state_code
    AND sent.supervision_sentence_id = ssa.supervision_sentence_id
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_charge` charge
    ON ssa.state_code = charge.state_code
    AND ssa.charge_id = charge.charge_id
  LEFT JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offensestable_latest` offense
    ON offense.sid = pei.external_id
    AND offense.case_number = sent.external_id
    AND offense.RecID = charge.external_id
  LEFT JOIN `{project_id}.{static_reference_tables_dataset}.state_county_codes` scc
    ON charge.state_code = scc.state_code
    AND charge.county_code = scc.county_code
  WHERE sent.state_code = "US_ND"
  ),
individual_sentence AS (
  /* This CTE combines charges for each sentence id to create a table
  unique on individual and sentence, and stores those crime names and charge_ids in an array for each sentence */
  SELECT 
  person_id, 
  person_external_id,
  state_code,
  prior_court_date,
  completion_date,
  sentence_years,
  sentence_months,
  probation_expiration_date,
  conviction_county, 
  criminal_number, 
  supervision_sentence_id,
  supervision_external_id,
  supervision_type,
  judicial_district_code,
  CONCAT(
    -- Grab first name (everything before the comma)
    REGEXP_EXTRACT(judge, r',\\s*(\\w+)$'),
    ' ',
    -- Grab last name (everything after the comma)
    REGEXP_EXTRACT(judge, r'^(\\w+),')
  ) AS judge,
  --create a ranking for supervision type to enable sentence deduplication
  IF(supervision_type = "IC PROBATION", 2, 1) AS supervision_type_rank,
  --order crime names and classifcations by severity 
  ARRAY_AGG(crime_subclassification IGNORE NULLS 
                    ORDER BY crime_classification, crime_subclassification, charge_external_id ) AS crime_subtypes,
  ARRAY_AGG(crime_classification IGNORE NULLS 
                    ORDER BY crime_classification, crime_subclassification, charge_external_id) AS crime_classifications,
  ARRAY_AGG(crime_name ORDER BY crime_classification, crime_subclassification, charge_external_id) AS crime_names,
  FROM individual_sentence_charges
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
),
eligible_population AS (
   -- Pull the individuals that are currently eligible for early discharge
   SELECT
     state_code,
     person_id,
     start_date,
     reasons,
     is_eligible,
     is_almost_eligible,
   FROM `{project_id}.{task_eligibility_dataset}.complete_discharge_early_from_supervision_form_materialized`
   WHERE is_eligible
    AND CURRENT_DATE('US/Pacific') BETWEEN start_date AND DATE_SUB(end_date, INTERVAL 1 DAY)
),
supervision_levels AS (
   -- Extract supervision level info from the reasons JSON blob
   SELECT
     state_code,
     person_id,
     JSON_VALUE(reason, '$.reason.supervision_level') AS supervision_level,
   FROM eligible_population,
   UNNEST(JSON_QUERY_ARRAY(reasons)) AS reason
   WHERE JSON_VALUE(reason, '$.criteria_name') = "{supervision_level_criteria}"
),
individual_sentence_ranks AS (
  /* This CTE ranks sentences first by supervision type (Not IC Probation), longest duration,
   and severity of crime. It also keeps record of the # of sentences for an individual 
  
  Sentence info is joined to task eligibility sessions and only supervision sentences that are eligible for ET
  are retained (ie. not IC PAROLE or PAROLE) */
  SELECT
    inds.person_external_id,
    inds.state_code,
    INITCAP(JSON_VALUE(PARSE_JSON(info.full_name), '$.given_names'))
        || " " 
        || INITCAP(JSON_VALUE(PARSE_JSON(info.full_name), '$.surname')) AS client_name,
    inds.conviction_county,
    inds.judicial_district_code,
    inds.criminal_number,
    inds.judge,
    inds.prior_court_date,
    inds.sentence_months,
    inds.sentence_years,
    inds.crime_names,
    inds.crime_subtypes,
    inds.crime_classifications,
    inds.supervision_type,
    sl.supervision_level,
    inds.probation_expiration_date,
    --create sentence ranking by supervision type (not IC probation), length, and charge severity 
    ROW_NUMBER() OVER (PARTITION BY person_external_id ORDER BY supervision_type_rank,
                                    probation_expiration_date DESC, crime_classifications[OFFSET(0)], 
                                    crime_subtypes[OFFSET(0)], supervision_external_id) AS sentence_rank,
    INITCAP(JSON_VALUE(PARSE_JSON(dm.full_name), '$.given_names'))
        || " " 
        || INITCAP(JSON_VALUE(PARSE_JSON(dm.full_name), '$.surname')) AS probation_officer_full_name,
    te.reasons, 
    te.is_eligible,
    te.is_almost_eligible,
   COUNT(*) OVER(PARTITION BY person_external_id) AS number_of_sentences
  FROM dataflow_metrics dm
  INNER JOIN eligible_population te
    USING (state_code, person_id)
  INNER JOIN supervision_levels sl
    USING (state_code, person_id)
  INNER JOIN individual_sentence inds
    ON te.state_code = inds.state_code
    AND te.person_id = inds.person_id
    AND te.start_date BETWEEN inds.prior_court_date AND COALESCE(inds.completion_date, "9999-12-31")
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person` info
    ON inds.state_code = info.state_code
    AND inds.person_id = info.person_id
  --only individuals on a supervision_type that is eligible for early discharge
  WHERE supervision_type NOT IN ("IC PAROLE", "PAROLE")
)

SELECT 
    person_external_id AS external_id,
    state_code,
    client_name AS form_information_client_name,
    conviction_county AS form_information_conviction_county,
    judicial_district_code AS form_information_judicial_district_code,
    criminal_number AS form_information_criminal_number,
    judge AS form_information_judge_name,
    prior_court_date AS form_information_prior_court_date,
    sentence_months AS form_information_sentence_length_months,
    sentence_years AS form_information_sentence_length_years,
    crime_names AS form_information_crime_names,
    probation_expiration_date AS form_information_probation_expiration_date,
    probation_officer_full_name AS form_information_probation_officer_full_name,
    sa.full_name AS form_information_states_attorney_name,
    CONCAT(sa.address_line_1, " ", COALESCE(sa.address_line_2, ""), "\\n", sa.city, ", ", sa.state, " ", sa.zip) AS form_information_states_attorney_mailing_address,
    sa.phone_office as form_information_states_attorney_phone_number,
    sa.email_1 as form_information_states_attorney_email_address,
    reasons,
    is_eligible,
    is_almost_eligible,
    number_of_sentences > 1 AS metadata_multiple_sentences,
    supervision_type = "IC PROBATION" AS metadata_out_of_state,
    supervision_level = "INTERSTATE_COMPACT" AS metadata_IC_OUT,
FROM individual_sentence_ranks isr
LEFT JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_state_attorney_latest` sa
  ON isr.conviction_county = sa.county
WHERE sentence_rank = 1 --only choose one sentence 
--only select one state attorney per county, so that there is only one record per client 
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_external_id ORDER BY sa.full_name)=1
ORDER BY person_external_id
"""

US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_NAME,
    view_query_template=US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_QUERY_TEMPLATE,
    description=US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    dataflow_metrics_materialized_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ND
    ),
    should_materialize=True,
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    supervision_level_criteria=supervision_level_criteria,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.build_and_print()
