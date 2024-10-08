# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query mapping state sentence_id to opportunity_id and opportunity_pseudonymized_id"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import get_pseudonymized_id_query_str
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCE_ID_TO_OPPORTUNITY_ID_VIEW_NAME = "sentence_id_to_opportunity_id"

SENTENCE_ID_TO_OPPORTUNITY_ID_QUERY_TEMPLATE = f"""
SELECT DISTINCT
    state_code,
    person_id,
    sentence_id,
    sentence_external_id AS opportunity_id,
    {get_pseudonymized_id_query_str(
        "IF(state_code = 'US_IX', 'US_ID', state_code) || sentence_external_id"
    )} AS opportunity_pseudonymized_id,
FROM `{{project_id}}.sentence_sessions.sentences_and_charges_materialized`
"""

SENTENCE_ID_TO_OPPORTUNITY_ID_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=SENTENCE_ID_TO_OPPORTUNITY_ID_VIEW_NAME,
    view_query_template=SENTENCE_ID_TO_OPPORTUNITY_ID_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_ID_TO_OPPORTUNITY_ID_VIEW_BUILDER.build_and_print()
