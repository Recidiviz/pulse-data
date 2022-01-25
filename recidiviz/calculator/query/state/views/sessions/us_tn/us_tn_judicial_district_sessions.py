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
"""Sessionized view of judicial district off of pre-processed raw TN sentencing data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_NAME = "us_tn_judicial_district_sessions"

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of judicial district off of pre-processed raw TN sentencing data"""

US_TN_JUDICIAL_DISTRICT_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/ 
    --TODO(#10747): Remove judicial district preprocessing once hydrated in population metrics  
    SELECT 
        person_id,
        state_code,
        sentence_id,
        CAST(judicial_district AS STRING) AS judicial_district_code,
        sentence_effective_date AS judicial_district_start_date,
        LEAD(DATE_SUB(sentence_effective_date, INTERVAL 1 DAY)) OVER(PARTITION BY person_id ORDER BY sentence_effective_date) AS judicial_district_end_date,
    FROM `{project_id}.{sessions_dataset}.us_tn_sentences_preprocessed_materialized`
"""

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_NAME,
    view_query_template=US_TN_JUDICIAL_DISTRICT_SESSIONS_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    description=US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_BUILDER.build_and_print()
