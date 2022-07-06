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
"""Processed Sentencing Data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCES_PREPROCESSED_VIEW_NAME = "sentences_preprocessed"

SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = """Processed Sentencing Data"""

SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    --TODO(#13746): Investigate whether completion_date in state agnostic sentences preprocessed should allow for a date in the future
    SELECT  
        * 
        EXCEPT(
            total_program_credits,
            total_behavior_credits,
            total_ppsc_credits,
            total_ged_credits,
            total_literary_credits,
            total_drug_alcohol_credits,
            total_education_attendance_credits,
            total_treatment_credits)
    FROM `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized`
"""

SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
