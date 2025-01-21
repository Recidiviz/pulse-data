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
"""Computes Q6 (proximity to release points) of Idaho's Reclassification of Security Level form at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_SLS_Q6_VIEW_NAME = "us_ix_sls_q6"

US_IX_SLS_Q6_VIEW_DESCRIPTION = """Computes Q6 (proximity to release points) of Idaho's Reclassification of
Security Level form at any point in time. See details of the Reclassification Form here:
https://drive.google.com/file/d/1-Y3-RAqPEUrAKoeSdkNTDmB-V1YAEp1l/view
    """

US_IX_SLS_Q6_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        IF(meets_criteria, -9, 0) AS q6_score
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.eligible_for_proximity_to_release_points_materialized`
"""

US_IX_SLS_Q6_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_SLS_Q6_VIEW_NAME,
    description=US_IX_SLS_Q6_VIEW_DESCRIPTION,
    view_query_template=US_IX_SLS_Q6_QUERY_TEMPLATE,
    task_eligibility_criteria_us_ix=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_SLS_Q6_VIEW_BUILDER.build_and_print()
