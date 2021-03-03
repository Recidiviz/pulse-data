# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Event Based Revocations and New Admissions."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_VIEW_NAME = (
    "event_based_revocations_and_admissions"
)

EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_DESCRIPTION = """
 New admissions and supervision revocations for the COVID report
 """

EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT DISTINCT * FROM (
      -- Take all new admissions, except those to CPP in US_ND
      SELECT
        state_code, person_id,
        admission_date,
        'NEW_ADMISSION' AS incarceration_type
      FROM `{project_id}.{reference_views_dataset}.event_based_admissions`
      WHERE admission_reason = 'NEW_ADMISSION'
      AND {state_specific_facility_exclusion}

      UNION ALL

      -- Append the incarceration admissions identified as revocations (which have multiple possible admission reasons)
      SELECT
        state_code, person_id,
        revocation_admission_date AS admission_date,
        'REVOCATION' AS incarceration_type
      FROM `{project_id}.{reference_views_dataset}.event_based_revocations`
    )
    ORDER BY state_code, admission_date, incarceration_type
    """

EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    view_id=EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_VIEW_NAME,
    view_query_template=EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_QUERY_TEMPLATE,
    description=EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_specific_facility_exclusion=state_specific_query_strings.state_specific_facility_exclusion(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_REVOCATIONS_AND_ADMISSIONS_VIEW_BUILDER.build_and_print()
