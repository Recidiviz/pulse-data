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
"""
Flag all sentence inferred groups created in sentence sessions that have
mismatched sentence level and group level state provided projected dates.

This occurs when the aggregated sentence level projected dates do not match
the state provided group level projected dates for an inferred sentence group.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config


def are_different(col_name: str) -> str:
    return f"""
    (
       sentence.{col_name} IS NOT NULL
       AND
       sentence_group.{col_name} IS NOT NULL
       AND
       sentence.{col_name} != sentence_group.{col_name}
    )
    """


# Will return all inferred groups at a point in time where the
# aggregated sentence level projected dates are not the same as
# the group level projected dates. There should be no returned rows.
QUERY = f"""
SELECT DISTINCT
    sentence.state_code,
    sentence.state_code AS region_code,
    sentence.sentence_inferred_group_id,
    sentence.inferred_group_update_datetime
FROM
    `{{project_id}}.{INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER.table_for_query.to_str()}`
AS
    sentence_group
JOIN
    `{{project_id}}.{INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER.table_for_query.to_str()}`
AS
    sentence
ON
    sentence.state_code = sentence_group.state_code
AND
    sentence.sentence_inferred_group_id = sentence_group.sentence_inferred_group_id
AND
    sentence.inferred_group_update_datetime = sentence_group.inferred_group_update_datetime
AND (
{
    'OR'.join(are_different(col_name) for col_name in [
        'parole_eligibility_date',
        'projected_parole_release_date',
        'projected_full_term_release_date_min',
        'projected_full_term_release_date_max',
    ])
}
)
"""

SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VALIDATION_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.VIEWS_DATASET,
        view_id="inferred_group_aggregated_projected_dates_validation",
        view_query_template=QUERY,
        description=__doc__.strip(),
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VALIDATION_VIEW_BUILDER.build_and_print()
