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
"""Preprocessed view of drug screens in Idaho (ATLAS) over the last 20 years, unique on person, date, and sample type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_NAME = "us_ix_drug_screens_preprocessed"

US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of drug screens in Idaho (ATLAS) over the last 20 years, unique on person, date, and sample type"""

US_IX_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE = """
SELECT
    person_id,
    state_code,
    drug_screen_date,
    sample_type,
    LOGICAL_OR(drug_screen_result IN ("ADMITTED_POSITIVE", "POSITIVE")) OVER w AS is_positive_result,
    FIRST_VALUE(drug_screen_result_raw_text IGNORE NULLS) OVER w AS result_raw_text_primary,
    ARRAY_AGG(drug_screen_result_raw_text) OVER w AS result_raw_text,
    CAST(NULL AS STRING) AS substance_detected,
    CAST(NULL AS STRING) AS med_invalidate_flg,
    FALSE AS is_inferred,
FROM
    `{project_id}.{normalized_state_dataset}.state_drug_screen`
WHERE
    state_code = "US_IX"
QUALIFY
    ROW_NUMBER() OVER w = 1
WINDOW w AS
    (
        PARTITION BY person_id, state_code, drug_screen_date, sample_type
        ORDER BY CASE drug_screen_result_raw_text 
            # Indicates admitted use
            WHEN "ADMITTED USE" THEN 1
            # Indicates at least one substance found
            WHEN "NOT ALL NEGATIVE RESULTS" THEN 2
            # Indicates all results were negative
            WHEN "ALL NEGATIVE RESULTS" THEN 3 
            ELSE 4 END,
        drug_screen_result_raw_text
    )
"""

US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_NAME,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    description=US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_IX_DRUG_SCREENS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.build_and_print()
