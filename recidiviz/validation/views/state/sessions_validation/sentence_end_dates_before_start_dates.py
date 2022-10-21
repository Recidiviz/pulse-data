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
"""A validation view that contains all of the sentences with end dates that occur before
or on the same day as the sentence start dates."""

from itertools import product

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SENTENCE_END_DATES_BEFORE_START_DATES_VIEW_NAME = (
    "sentence_end_dates_before_start_dates"
)

SENTENCE_END_DATES_BEFORE_START_DATES_DESCRIPTION = """
Return all sentences that have end dates (`completion_date`,
`projected_completion_date_min`, and `projected_completion_date_max`) that come before
or on the same day as the sentence start dates (`effective_date`, and `date_imposed`)
from within the `sentences_preprocessed` view.
"""

SENTENCE_START_DATES = [
    "date_imposed",
    "effective_date",
]

SENTENCE_END_DATES = [
    "completion_date",
    "projected_completion_date_min",
    "projected_completion_date_max",
    "parole_eligibility_date",
]

# Create a list of all the different start date/end date comparison sub-queries
SENTENCE_END_DATES_BEFORE_START_DATES_QUERY_FRAGMENTS = [
    f"""
SELECT
    state_code AS region_code,
    person_id,
    {start_date} AS start_date,
    "{start_date}" AS start_date_type,
    {end_date} AS end_date,
    "{end_date}" AS end_date_type,
FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
WHERE {nonnull_end_date_clause(end_date)} <= {nonnull_start_date_clause(start_date)}
"""
    for start_date, end_date in product(SENTENCE_START_DATES, SENTENCE_END_DATES)
]

# Union all of the different start date/end date comparison queries together
SENTENCE_END_DATES_BEFORE_START_DATES_QUERY_TEMPLATE = "\nUNION ALL\n".join(
    SENTENCE_END_DATES_BEFORE_START_DATES_QUERY_FRAGMENTS
)


SENTENCE_END_DATES_BEFORE_START_DATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SENTENCE_END_DATES_BEFORE_START_DATES_VIEW_NAME,
    view_query_template=SENTENCE_END_DATES_BEFORE_START_DATES_QUERY_TEMPLATE,
    description=SENTENCE_END_DATES_BEFORE_START_DATES_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_END_DATES_BEFORE_START_DATES_VIEW_BUILDER.build_and_print()
