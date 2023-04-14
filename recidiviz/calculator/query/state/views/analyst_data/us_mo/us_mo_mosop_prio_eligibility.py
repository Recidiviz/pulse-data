# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Uses information on prioritized release dates, classes, and statutory requirements to 
generate a list of people believed to be eligible for prioritization for MOSOP (MO Sex 
Offender Program)"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_MOSOP_PRIO_ELIGIBILITY_VIEW_NAME = "us_mo_mosop_prio_eligibility"

US_MO_MOSOP_PRIO_ELIGIBILITY_VIEW_DESCRIPTION = """Uses the program tracks determined by
the us_mo_program_tracks view to pull a list of people believed to be eligible for 
MOSOP prioritization."""

US_MO_MOSOP_PRIO_ELIGIBILITY_QUERY_TEMPLATE = """
SELECT * FROM (
    SELECT 
        person_id,
        external_id,
        CASE WHEN ongoing_flag = TRUE THEN "ongoing" ELSE "no_ongoing" END AS eligibility_category
    FROM `{project_id}.{analyst_dataset}.us_mo_program_tracks_materialized`
    WHERE
        mosop_indicator = TRUE 
        AND
        completed_flag = FALSE 
        AND (
            DATE_DIFF(prioritized_date, CURRENT_DATE, MONTH) BETWEEN 12 AND 18
            OR (
                board_determined_release_date = max_discharge AND
                conditional_release IS NOT NULL AND
                DATE_DIFF(conditional_release, CURRENT_DATE, MONTH) <= 18
            )
        )
    )
ORDER BY eligibility_category
"""

PRIORITIZED_ELIGIBILITY = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_MO_MOSOP_PRIO_ELIGIBILITY_VIEW_NAME,
    view_query_template=US_MO_MOSOP_PRIO_ELIGIBILITY_QUERY_TEMPLATE,
    description=US_MO_MOSOP_PRIO_ELIGIBILITY_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRIORITIZED_ELIGIBILITY.build_and_print()
