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
"""Powers report that shows US_ND residents who may be missing a housing referral to be eligible to transfer to minimum security unit"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    get_recent_denials_query,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_VIEW_NAME = (
    "us_nd_transfer_to_min_eligible_without_referral"
)

US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_QUERY_TEMPLATE = f"""

{get_recent_denials_query()}

SELECT 
    rr.state_code,
    rr.person_external_id AS elite_no,
    CONCAT(
        JSON_EXTRACT_SCALAR(rr.person_name, '$.given_names'), ' ',
        JSON_EXTRACT_SCALAR(rr.person_name, '$.surname')
    ) AS person_name,
    rr.facility_id,
    rr.unit_id AS living_unit,
    rr.release_date,
    rr.custody_level,
    CONCAT(
        sr.given_names, ' ', sr.surname
    ) AS officer_name,
    CASE 
        WHEN ineligible_criteria_unnested = 'US_ND_NOT_ENROLLED_IN_RELEVANT_PROGRAM' THEN 'Enrolled in a program'
        WHEN ineligible_criteria_unnested = 'US_ND_NOT_IN_AN_ORIENTATION_UNIT' THEN 'Has only spent 2 weeks in ORU'
        WHEN ineligible_criteria_unnested = 'INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE' THEN 'One month from 42 months pre-release'
        ELSE ''
    END AS missing_criteria,
FROM `{{project_id}}.workflows_views.us_nd_transfer_to_minimum_facility_form_record_materialized` tes
LEFT JOIN UNNEST(tes.ineligible_criteria) AS ineligible_criteria_unnested
INNER JOIN `{{project_id}}.workflows_views.resident_record_materialized` rr
  ON tes.external_id = rr.person_external_id
    AND tes.state_code = rr.state_code
LEFT JOIN `{{project_id}}.workflows_views.incarceration_staff_record_materialized` sr
    ON sr.id = rr.officer_id
        AND sr.state_code = 'US_ND'
LEFT JOIN recent_denials rd
    ON tes.state_code = rd.state_code
        AND rr.person_external_id = rd.person_external_id
-- This person is not marked ineligible
WHERE rd.person_external_id IS NULL
ORDER BY facility_id, officer_name, release_date DESC
"""

US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.USER_METRICS_DATASET_ID,
    view_id=US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_VIEW_NAME,
    view_query_template=US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_VIEW_BUILDER.build_and_print()
