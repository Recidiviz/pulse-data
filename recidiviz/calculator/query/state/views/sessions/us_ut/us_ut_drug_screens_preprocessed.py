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
"""Preprocessed view of drug screens in Utah, unique on person, date, and sample type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_UT_DRUG_SCREENS_PREPROCESSED_VIEW_NAME = "us_ut_drug_screens_preprocessed"

US_UT_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE = """
SELECT
    person_id,
    state_code,
    drug_screen_date,
    sample_type,
    LOGICAL_OR(drug_screen_result IN ("ADMITTED_POSITIVE", "POSITIVE")) OVER w AS is_positive_result,
    FIRST_VALUE(drug_screen_result_raw_text IGNORE NULLS) OVER w AS result_raw_text_primary,
    ARRAY_AGG(IFNULL(drug_screen_result_raw_text, 'NULL')) OVER w AS result_raw_text,
    FIRST_VALUE(
         JSON_VALUE(drug_screen_metadata, '$.substance') IGNORE NULLS
         ) OVER w AS substance_detected,
    CAST(NULL AS STRING) AS med_invalidate_flg,
    FALSE AS is_inferred,
    drug_screen_date AS earliest_drug_screen_date,
FROM
    `{project_id}.{normalized_state_dataset}.state_drug_screen`
WHERE
    state_code = "US_UT"
# We prioritize positive results over negtive ones if they happened on the same day
QUALIFY
    ROW_NUMBER() OVER w = 1
WINDOW w AS
    (
        PARTITION BY person_id, state_code, drug_screen_date, sample_type
        ORDER BY CASE drug_screen_result_raw_text 
            # Positive
            WHEN "Y" THEN 1
            # Negative
            WHEN "N" THEN 2
            ELSE 3 END,
        drug_screen_result_raw_text
    )
"""

US_UT_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_UT_DRUG_SCREENS_PREPROCESSED_VIEW_NAME,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    description=__doc__,
    view_query_template=US_UT_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_UT_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.build_and_print()
