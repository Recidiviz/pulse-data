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
"""Supervision Downgrade Eligibility Criteria
    - Currently supervised at a level higher than most recent assessment
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
    - No Police Contact in the last 12 months that include terms related to ARRESTED, FELONY, MISDEMEANOR, CHARGED, CITED, JAIL
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.case_triage.views.dataset_config import CASE_TRIAGE_DATASET
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_NAME = "us_id_day_0_supervision_downgrade"

US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_DESCRIPTION = """  This view was created to simplify logic identified during the Day 0 trip to Idaho in September 2021.  """
US_ID_DAY_0_SUPERVISION_DOWNGRADE_QUERY_TEMPLATE = """
WITH preliminary_eligibility AS (
    SELECT DISTINCT
        c.person_external_id,
        CONCAT(
            JSON_EXTRACT_SCALAR(full_name, '$.given_names'),
            ' ',
            JSON_EXTRACT_SCALAR(full_name, '$.surname')
        ) AS client_name,
        c.supervising_officer_external_id,
        c.supervision_type,
        c.case_type,
        JSON_EXTRACT_SCALAR(o.opportunity_metadata, '$.latestAssessmentDate') AS latest_assessment_date,
        c.supervision_level,
        JSON_EXTRACT_SCALAR(o.opportunity_metadata, '$.recommendedSupervisionLevel') AS recommended_supervision_level,
        JSON_EXTRACT_SCALAR(o.opportunity_metadata, '$.assessmentScore') AS assessment_score,
    # TODO(#14214) - remove the dependency on case triage 
    FROM `{project_id}.{case_triage_dataset}.etl_clients_materialized` c
    INNER JOIN `{project_id}.{case_triage_dataset}.etl_opportunities_materialized` o
        USING (state_code, person_external_id)
    WHERE state_code = 'US_ID'
        AND opportunity_type = 'OVERDUE_DOWNGRADE'
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

SELECT pe.*
FROM preliminary_eligibility pe
LEFT JOIN case_notes
    USING (person_external_id)
LEFT JOIN le_contact
    USING (person_external_id)
WHERE IFNULL(case_notes_flag, 0) = 0
    AND IFNULL(le_contact_flag, 0) = 0
    AND pe.latest_assessment_date IS NOT NULL    """

US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_NAME,
    view_query_template=US_ID_DAY_0_SUPERVISION_DOWNGRADE_QUERY_TEMPLATE,
    description=US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_DESCRIPTION,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    case_triage_dataset=CASE_TRIAGE_DATASET,
    us_id_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_id"),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER.build_and_print()
