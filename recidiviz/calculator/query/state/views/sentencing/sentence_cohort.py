#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View of cohorts with PROBATION, RIDER, and TERM sentences."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.sentencing.us_ix.sentencing_sentence_cohort_template import (
    US_IX_SENTENCING_SENTENCE_COHORT_TEMPLATE,
)
from recidiviz.calculator.query.state.views.sentencing.us_nd.sentencing_sentence_cohort_template import (
    US_ND_SENTENCING_SENTENCE_COHORT_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Only consider recent data, starting from this year
RECENT_DATA_START_YEAR = "2010"

SENTENCE_COHORT_VIEW_NAME = "sentence_cohort"

SENTENCE_COHORT_DESCRIPTION = """
This view identifies all "cohort starts" for various sentence dispositions. This allows for recidivism and sentence
disposition calculations for the purpose of PSI Case Insights.

Each row represents an initial sentence for a particular offense, and the start date is when they were subsequently in
the community; for probation, this is when they start their probation sentence and for incarceration, this is when
they're next out on supervision or at liberty.

The cohort_group column combines sentence type and (optionally) length. The pattern is:
* "<sentence_type>|<start>-<end> years" if sentence length is included. <end> can be "?" to indicate no upper bound.
Examples are "INCARCERATION|0-1 years" or "INCARCERATION|6-? years" (representing sentences of length 6 years or more).
* "<sentence_type>" if sentence length is not included.
"""

SENTENCE_COHORT_QUERY_TEMPLATE = f"""
SELECT {{columns}}
FROM
(
    (SELECT *
    FROM ({US_IX_SENTENCING_SENTENCE_COHORT_TEMPLATE})
    )
    UNION ALL
    (SELECT *
    FROM ({US_ND_SENTENCING_SENTENCE_COHORT_TEMPLATE})
    )
)
WHERE EXTRACT(YEAR FROM cohort_start_date) >= {RECENT_DATA_START_YEAR}
"""

SENTENCE_COHORT_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    view_id=SENTENCE_COHORT_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCE_COHORT_QUERY_TEMPLATE,
    description=SENTENCE_COHORT_DESCRIPTION,
    columns=[
        "state_code",
        "person_id",
        "gender",
        "assessment_score",
        "cohort_group",
        "cohort_start_date",
        "most_severe_description",
        "most_severe_ncic_category_uniform",
        "most_severe_ncic_category_external",
        "any_is_violent",
        "any_is_drug",
        "any_is_sex_offense",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_COHORT_VIEW_BUILDER.build_and_print()
