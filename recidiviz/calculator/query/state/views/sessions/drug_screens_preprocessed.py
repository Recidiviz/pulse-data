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
"""Preprocessed view of drug screen tests and results, unique on person, date, and sample type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DRUG_SCREENS_PREPROCESSED_VIEW_NAME = "drug_screens_preprocessed"

DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of drug screen tests and results, unique on person, date, and sample type"""

DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE = """
    /* {description} */
    SELECT * FROM `{project_id}.{sessions_dataset}.us_id_drug_screens_preprocessed`
    UNION ALL
    SELECT * FROM `{project_id}.{sessions_dataset}.us_tn_drug_screens_preprocessed`
"""

DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=DRUG_SCREENS_PREPROCESSED_VIEW_NAME,
    sessions_dataset=SESSIONS_DATASET,
    description=DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.build_and_print()
