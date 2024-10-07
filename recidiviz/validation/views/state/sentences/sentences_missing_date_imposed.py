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
"""A validation view that contains all of the sentences with no date imposed."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SENTENCES_MISSING_DATE_IMPOSED_VIEW_NAME = "sentences_missing_date_imposed"

SENTENCES_MISSING_DATE_IMPOSED_DESCRIPTION = """
Return all sentences that have no `date_imposed` value, indicating they cannot be linked
to any corresponding sessions.
"""

SENTENCES_MISSING_DATE_IMPOSED_QUERY_TEMPLATE = """
SELECT
  state_code,
  state_code AS region_code,
  sentence_type,
  person_id,
  sentence_id,
  external_id,
FROM `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized`
WHERE date_imposed IS NULL
"""


SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SENTENCES_MISSING_DATE_IMPOSED_VIEW_NAME,
    view_query_template=SENTENCES_MISSING_DATE_IMPOSED_QUERY_TEMPLATE,
    description=SENTENCES_MISSING_DATE_IMPOSED_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER.build_and_print()
