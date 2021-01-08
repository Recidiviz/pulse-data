# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""A view comparing various values from internal person-level recidivism metrics to the person-level
values from external metrics provided by the state where we both agree that the person was in a release cohort.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.state.recidivism_person_level_template import recidivism_person_level_query

RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_NAME = \
    'recidivism_person_level_external_comparison_matching_people'

RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_DESCRIPTION = """
Comparison of recidivism values between internal and external lists of person-level recidivism among rows where we both
agree the person was included in a release cohort.
"""

RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_QUERY_TEMPLATE = \
    f"""
/*{{description}}*/
{recidivism_person_level_query(include_unmatched_people=False)}
"""

RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_NAME,
    view_query_template=RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_QUERY_TEMPLATE,
    description=RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER.build_and_print()
