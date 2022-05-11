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
"""Creates a view for collapsing raw ID employment data into contiguous periods of employment or unemployment overlapping with a supervision super session"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_NAME = (
    "us_id_employment_periods_preprocessed"
)

US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_DESCRIPTION = (
    """View of employment information in Idaho"""
)

US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_QUERY_TEMPLATE = """
/* {description} */
    #TODO(#12548): Deprecate state preprocessing views once employment data exists in state schema
    SELECT DISTINCT
        person.person_id,
        "US_ID" AS state_code,
        SAFE_CAST(SPLIT(employment.startdate, ' ')[OFFSET(0)] AS DATE) AS employment_start_date,
        SAFE_CAST(SPLIT(employment.enddate, ' ')[OFFSET(0)] AS DATE) AS employment_end_date,
        SAFE_CAST(SPLIT(employment.verifydate, ' ')[OFFSET(0)] AS DATE) AS last_verified_date,
        COALESCE(UPPER(employment.jobtitle), "UNKNOWN") AS job_title,
        COALESCE(UPPER(employer.name), "UNKNOWN") AS employer_name,
        SAFE_CAST(employment.hoursperweek AS FLOAT64) as hours_per_week,
        UPPER(leftreason.codedescription) as employment_end_reason,
        -- Infer if an employment entry indicates unemployment
        REGEXP_CONTAINS(UPPER(employer.name), r".*UNEMPLOY.*") OR REGEXP_CONTAINS(UPPER(jobtitle), r".*UNEMPLOY.*") AS is_unemployed,
    FROM 
        `{project_id}.{raw_dataset}.cis_employment_latest` employment
    INNER JOIN
        `{project_id}.{raw_dataset}.cis_offender_latest` person_external
    ON 
        employment.personemploymentid = person_external.id
    LEFT JOIN 
        `{project_id}.{raw_dataset}.cis_employer_latest` AS employer
    ON 
        employment.employerid = employer.id
    LEFT JOIN 
        `{project_id}.{raw_dataset}.cis_codeemploymentreasonleft_latest` AS leftreason
    ON 
        employment.codeemploymentreasonleftid = leftreason.id
    LEFT JOIN 
        `{project_id}.{state_base_dataset}.state_person_external_id` person
    ON 
        person_external.offendernumber = person.external_id 
        AND person.state_code = "US_ID"
"""

US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_NAME,
    description=US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_QUERY_TEMPLATE,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=False,
    raw_dataset=raw_latest_views_dataset_for_region("us_id"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_BUILDER.build_and_print()
