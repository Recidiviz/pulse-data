# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""LSU Eligibility Criteria
    - Low supervision for at least 1 year
    - Currently employed
    - No positive UA in past 6 months
    - Must have had residence check in past year
    - Must have had face-to-face contact in past 6 months
    - Completed treatment (if required)
    - Case note exclusions (none in past year) (https://github.com/Recidiviz/recidiviz-research/pull/527)
        - violation = [('.*violat.*', 'regex'), 'pv', 'rov', 'report of violation']
        - sanction = [('sanction', 'partial_ratio')]
        - extend = [('extend', 'partial_ratio')]
        - abscond = [('abscond', 'partial_ratio'), 'absconsion']
        - custody = ['in custody', ('arrest', 'partial_ratio')]
        - agents_warning = ['aw', 'agents warrant', 'cw', 'bw', 'commission warrant', 'bench warrant', 'warrant']
        - revoke = [('.*revoke.*', 'regex'), ('.*revoc.*', 'regex'), 'rx']
        - new_investigation = ['psi', 'file_review', 'activation']
        - other = ['critical', 'detainer', 'positive', 'admission', ('ilet.*nco | nco.*ilet.*', 'regex')]
    - Not serving murder/manslaughter,sex-offense, or DUI sentence
        - Actually check the sentence/offense-type, don't just rely on caseload
    - No Police Contact in the last 12 months that include terms related to ARRESTED, FELONY, MISDEMEANOR, CHARGED, CITED, JAIL

Additional Criteria for Early Discharge Eligibility
    - (naive approach) Served at least 50% of maximum possible sentence
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.case_triage.views.dataset_config import CASE_TRIAGE_DATASET
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_DAY_0_LSU_VIEW_NAME = "us_id_day_0_lsu"

US_ID_DAY_0_LSU_VIEW_DESCRIPTION = """LSU Eligibility Criteria for Idaho"""

US_ID_DAY_0_LSU_QUERY_TEMPLATE = """
WITH preliminary_eligibility AS (
    -- Low supervision for 1 year
    SELECT
        person_external_id,
        CONCAT(
            JSON_EXTRACT_SCALAR(full_name, '$.given_names'),
            ' ',
            JSON_EXTRACT_SCALAR(full_name, '$.surname')
        ) AS client_name,
        c.supervising_officer_external_id,
        supervision_type,
        case_type,
    # TODO(#14214) - remove the dependency on case triage 
    FROM `{project_id}.{case_triage_dataset}.etl_clients_materialized` c
    INNER JOIN `{project_id}.{case_triage_dataset}.etl_opportunities_materialized`
        USING (state_code, person_external_id)
    WHERE state_code = 'US_ID'
        AND opportunity_type = 'LIMITED_SUPERVISION_UNIT'
        AND JSON_EXTRACT_SCALAR(opportunity_metadata, '$.hasEmployment') = 'true'
        AND (
            SAFE_CAST(JSON_EXTRACT_SCALAR(opportunity_metadata, '$.countPositiveUAOneYear') AS INT64) = 0
            OR SAFE_CAST(JSON_EXTRACT_SCALAR(opportunity_metadata, '$.daysSincePositiveUA') AS INT64) >= 180
        )
        AND most_recent_face_to_face_date >= DATE_SUB(CURRENT_DATE("US/Mountain"), INTERVAL 6 MONTH)
        AND most_recent_home_visit_date >= DATE_SUB(CURRENT_DATE("US/Mountain"), INTERVAL 1 YEAR)
),
case_notes AS (
    SELECT
        CAST(person_external_id AS STRING) AS person_external_id,
        CAST(MAX(GREATEST(
            violation,
            sanction,
            extend,
            absconsion,
            in_custody,
            agents_warning,
            revocation,
            new_investigation,
            other
        )) AS INT64) AS case_notes_flag
    FROM `{project_id}.{supplemental_dataset}.us_id_case_note_matched_entities`
    WHERE SAFE_CAST(create_dt AS date) >= DATE_SUB(CURRENT_DATE("US/Mountain"), INTERVAL 1 YEAR)
    GROUP BY 1
),
treatment AS (
    SELECT
        person_external_id,
        1 AS incomplete_treatment,
    FROM (
        SELECT
            CAST(person_external_id AS STRING) AS person_external_id,
            CAST(MAX(any_treatment) AS INT64) AS any_treatment,
            CAST(MAX(treatment_complete) AS INT64) AS treatment_complete,
        FROM `{project_id}.{supplemental_dataset}.us_id_case_note_matched_entities`
        WHERE SAFE_CAST(create_dt AS date) >= DATE_SUB(CURRENT_DATE("US/Mountain"), INTERVAL 2 YEAR)
        GROUP BY 1
    )
    WHERE any_treatment = 1 AND treatment_complete = 0
),
sentences AS (
    SELECT
        pei.external_id AS person_external_id,
        s.sentence_start_date,
        s.sentence_completion_date,
        CASE
            WHEN CURRENT_DATE("US/Mountain") >= s.projected_completion_date_max THEN NULL
            WHEN COALESCE(s.max_projected_sentence_length, 0) = 0 THEN NULL
            WHEN CURRENT_DATE("US/Mountain") >= s.sentence_completion_date THEN NULL
            ELSE DATE_DIFF(CURRENT_DATE("US/Mountain"), s.sentence_start_date, DAY) / s.max_projected_sentence_length
        END AS pct_sentence_served,
        offense_type,
        CASE WHEN offense_type = 'MURDER & MAN' THEN 1 ELSE 0 END AS offense_type_murder,
        CASE WHEN offense_type = 'SEX' THEN 1 ELSE 0 END AS offense_type_sex,
    FROM `{project_id}.{sessions_dataset}.compartment_sentences_materialized` s,
        UNNEST(offense_type) AS offense_type
    INNER JOIN `{project_id}.{state_dataset}.state_person_external_id` pei
        USING (state_code, person_id)
    WHERE state_code = 'US_ID'
),
pct_served_current_sentences AS (
    SELECT
        person_external_id,
        MIN(pct_sentence_served) AS min_pct_sentence_served,
    FROM sentences
    WHERE pct_sentence_served IS NOT NULL
    GROUP BY 1
),
sentences_dedup AS (
    SELECT
        person_external_id,
        sentence_start_date,
        sentence_completion_date,
        MAX(offense_type_murder) AS offense_type_murder,
        MAX(offense_type_sex) AS offense_type_sex,
    FROM sentences
    GROUP BY 1, 2, 3
),
murder_or_sex_offense AS (
    SELECT DISTINCT
        person_external_id,
        sentence_start_date,
        sentence_completion_date,
    FROM sentences_dedup
    WHERE GREATEST(offense_type_murder, offense_type_sex) = 1
),
current_offense_type_flag AS (
    SELECT
        person_external_id,
        1 AS sentence_type_flag,
    FROM murder_or_sex_offense
    WHERE CURRENT_DATE("US/Mountain") BETWEEN sentence_start_date AND IFNULL(sentence_completion_date, '9999-01-01')
),
dui AS (
    SELECT DISTINCT
        external_id AS person_external_id,
        1 AS dui_flag,
    FROM `{project_id}.{sessions_dataset}.compartment_sentences_materialized`,
    UNNEST(description) AS description
    INNER JOIN `{project_id}.{state_dataset}.state_person_external_id`
        USING (state_code, person_id)
    WHERE state_code = 'US_ID'
        AND description IN (
            'DRIVING UNDER THE INFLUENCE',
            'AGGRAVATED DRIVING UNDER THE INFLUENCE',
            'DRIVING WHILE INTOXICATED',
            'AGGRAVATED DRIVING UNDER INFLUENCE OF INTOXICATING SUBSTANCE'
        )
        AND CURRENT_DATE("US/Mountain") BETWEEN sentence_start_date AND IFNULL(sentence_completion_date, '9999-01-01')
),
us_id_le_contact AS (
    SELECT
        o.offendernumber AS person_external_id,
        UPPER(lct.codedescription) AS contact_type,
        CAST(CAST(lc.contactdate AS DATETIME) AS DATE) AS contact_date,
        FROM `{project_id}.{us_id_raw_data_up_to_date_dataset}.cis_lawenforcementcontact_recidiviz_latest` lc
        INNER JOIN `{project_id}.{us_id_raw_data_up_to_date_dataset}.cis_codelawenforcementcontacttype_recidiviz_latest` lct
        ON lc.codelawenforcementcontacttypeid = lct.id
        INNER JOIN `{project_id}.{us_id_raw_data_up_to_date_dataset}.cis_offender_latest` o
        ON lc.offenderid = o.id
        WHERE CAST(CAST(lc.contactdate AS DATETIME) AS DATE) > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
),
le_contact AS (
    SELECT DISTINCT
        CAST(person_external_id AS STRING) AS person_external_id,
        1 AS le_contact_flag,
    FROM us_id_le_contact
    WHERE contact_type IN (
            'ARRESTED',
            'FELONY',
            'MISDEMEANOR',
            'CHARGED',
            'CITED',
            'JAIL'
        )
)

SELECT
    pe.*,
    CASE WHEN COALESCE(min_pct_sentence_served, 0) >= 0.5 THEN 1 ELSE 0 END AS ed_flag,
FROM preliminary_eligibility pe
LEFT JOIN case_notes
    USING (person_external_id)
LEFT JOIN treatment
    USING (person_external_id)
LEFT JOIN current_offense_type_flag
    USING (person_external_id)
LEFT JOIN dui
    USING (person_external_id)
LEFT JOIN le_contact
    USING (person_external_id)
LEFT JOIN pct_served_current_sentences
    USING (person_external_id)
WHERE IFNULL(case_notes_flag, 0) = 0
    AND IFNULL(incomplete_treatment, 0) = 0
    AND IFNULL(sentence_type_flag, 0) = 0
    AND IFNULL(dui_flag, 0) = 0
    AND IFNULL(le_contact_flag, 0) = 0
     """

US_ID_DAY_0_LSU_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ID_DAY_0_LSU_VIEW_NAME,
    view_query_template=US_ID_DAY_0_LSU_QUERY_TEMPLATE,
    description=US_ID_DAY_0_LSU_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    state_dataset=STATE_BASE_DATASET,
    case_triage_dataset=CASE_TRIAGE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    us_id_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_id"),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_DAY_0_LSU_VIEW_BUILDER.build_and_print()
