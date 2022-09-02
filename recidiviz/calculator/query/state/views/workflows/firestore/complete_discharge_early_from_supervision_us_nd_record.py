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
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    NORMALIZED_STATE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.task_eligibility.task_eligiblity_spans import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_VIEW_NAME = (
    "us_nd_complete_discharge_early_from_supervision_record"
)

COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_DESCRIPTION = """
    View of early termination record for form completion for individuals that are
    currently eligible for early termination 
    """


COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_QUERY_TEMPLATE = """
    /*{description}*/
    
WITH probation_officer AS(
  /* This CTE creates a view unique on external_id
   for non null officer names */
  SELECT
    full_name,
    external_id AS supervising_officer_external_id
  FROM `{project_id}.{normalized_state_dataset}.state_agent` sa
  WHERE state_code = 'US_ND' AND full_name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY full_name) = 1 --choose only one name per id 
),

dataflow_metrics AS (
    /* This CTE creates a view for the current supervision population
    unique on person_id by selecting non null judicial and supervising_officer_external_id fields*/
  SELECT
  state_code,
  person_id,
  judicial_district_code, 
  supervising_officer_external_id,
  full_name
  FROM `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_single_day_supervision_population_metrics_materialized` dataflow
  LEFT JOIN probation_officer
    USING (supervising_officer_external_id)
  WHERE state_code = 'US_ND'
  --choose only one judicial district per individual since individuals with overlapping supervision sentences
  --will have multiple rows in dataflow
  QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY person_id ORDER BY judicial_district_code DESC, supervising_officer_external_id DESC)=1
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
  sent.projected_completion_date AS probation_expiration_date,
  REGEXP_SUBSTR(charge.county_code, "_ND_(.*)") AS conviction_county,
  offense.court_number AS criminal_number,
  sent.supervision_sentence_id,
  sent.external_id AS supervision_external_id,
  sent.supervision_type_raw_text AS supervision_type,
  charge.external_id AS charge_external_id,
  charge.classification_type AS crime_classification,
  charge.classification_subtype AS crime_subclassification,
  CONCAT(
      COALESCE(ncic.description, charge.description, "<to fill>"), 
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
  LEFT JOIN `{project_id}.{static_reference_tables_dataset}.ncic_codes`ncic
    ON COALESCE(offense.common_statute_ncic_code, offense.code) = ncic.ncic_code
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
  probation_expiration_date,
  conviction_county, 
  criminal_number, 
  supervision_sentence_id,
  supervision_external_id,
  supervision_type,
  --create a ranking for supervision type to enable sentence deduplication
  IF(supervision_type = "IC PROBATION", 2, 1) AS supervision_type_rank,
  --order crime names and classifcations by severity 
  ARRAY_AGG(crime_subclassification IGNORE NULLS 
                    ORDER BY crime_classification, crime_subclassification, charge_external_id ) AS crime_subtypes,
  ARRAY_AGG(crime_classification IGNORE NULLS 
                    ORDER BY crime_classification, crime_subclassification, charge_external_id) AS crime_classifications,
  ARRAY_AGG(crime_name ORDER BY crime_classification, crime_subclassification, charge_external_id) AS crime_names,
  FROM individual_sentence_charges
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12),

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
    dm.judicial_district_code,
    inds.criminal_number,
    doc.judge,
    inds.prior_court_date,
    inds.sentence_years,
    inds.crime_names,
    inds.crime_subtypes,
    inds.crime_classifications,
    inds.supervision_type,
    JSON_VALUE(te.reasons[2].reason, '$.supervision_level') AS supervision_level,
    inds.probation_expiration_date,
    --create sentence ranking by supervision type (not IC probation), length, and charge severity 
    ROW_NUMBER() OVER (PARTITION BY person_external_id ORDER BY supervision_type_rank,
                                    probation_expiration_date DESC, crime_classifications[OFFSET(0)], 
                                    crime_subtypes[OFFSET(0)], supervision_external_id) AS sentence_rank,
    INITCAP(JSON_VALUE(PARSE_JSON(dm.full_name), '$.given_names'))
        || " " 
        || INITCAP(JSON_VALUE(PARSE_JSON(dm.full_name), '$.surname')) AS probation_officer_full_name,
    te.reasons, 
   COUNT(*) OVER(PARTITION BY person_external_id) AS number_of_sentences
  FROM dataflow_metrics dm
  INNER JOIN `{project_id}.{task_eligibility_dataset}.complete_discharge_early_from_supervision_form_materialized` te
    ON te.state_code = dm.state_code
    AND te.person_id = dm.person_id
    AND te.start_date <= CURRENT_DATE('US/Pacific')
    AND te.end_date IS NULL
    --only individuals that are currently eligible for early discharge
    AND te.is_eligible
   --AND te.task_name = "COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_FORM"
 INNER JOIN individual_sentence inds
    ON te.state_code = inds.state_code
    AND te.person_id = inds.person_id
    AND te.start_date BETWEEN inds.prior_court_date AND COALESCE(inds.completion_date, "9999-12-31")
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person` info
    ON inds.state_code = info.state_code
    AND inds.person_id = info.person_id
  LEFT JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offendercasestable_latest` doc
    ON doc.sid = inds.person_external_id
    AND doc.case_number = inds.supervision_external_id
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
sentence_years AS form_information_sentence_length_years,
crime_names AS form_information_crime_names,
probation_expiration_date AS form_information_probation_expiration_date,
probation_officer_full_name AS form_information_probation_officer_full_name,
reasons,
number_of_sentences > 1 AS metadata_multiple_sentences,
supervision_type = "IC PROBATION" AS metadata_out_of_state,
supervision_level = "INTERSTATE_COMPACT" AS metadata_IC_OUT,
FROM individual_sentence_ranks
WHERE sentence_rank = 1 --only choose one sentence 
ORDER BY person_external_id
"""

COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_VIEW_NAME,
    view_query_template=COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_QUERY_TEMPLATE,
    description=COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    dataflow_metrics_materialized_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset("us_nd"),
    should_materialize=True,
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_nd"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_US_ND_RECORD_VIEW_BUILDER.build_and_print()
