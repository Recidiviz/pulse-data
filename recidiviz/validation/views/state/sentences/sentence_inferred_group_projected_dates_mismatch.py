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
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

# Will return all inferred groups at a point in time where the
# aggregated sentence level projected dates are not the same as
# the group level projected dates. There should be no returned rows.
QUERY = f"""
WITH _unioned_cte AS
(
SELECT
    state_code,
    state_code AS region_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    parole_eligibility_date,
    projected_parole_release_date,
    projected_full_term_release_date_min,
    projected_full_term_release_date_max,
    "SENTENCE_GROUP" AS source_view,
FROM `{{project_id}}.{INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER.table_for_query.to_str()}`

UNION ALL 

SELECT
    state_code,
    state_code AS region_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    parole_eligibility_date,
    projected_parole_release_date,
    projected_full_term_release_date_min,
    projected_full_term_release_date_max,
    "SENTENCE" AS source_view,
FROM `{{project_id}}.{INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER.table_for_query.to_str()}`
)
,
{create_sub_sessions_with_attributes(
    table_name = '_unioned_cte',
    index_columns=['state_code','region_code','person_id','sentence_inferred_group_id'],
    end_date_field_name='end_date_exclusive')
}
, 
_deduped_cte AS
(
SELECT 
    state_code,
    region_code,
    person_id,
    sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    ANY_VALUE(IF(source_view = 'SENTENCE',parole_eligibility_date, NULL)) AS parole_eligibility_date__sentence,
    ANY_VALUE(IF(source_view = 'SENTENCE_GROUP',parole_eligibility_date, NULL)) AS parole_eligibility_date__sentence_group,
    ANY_VALUE(IF(source_view = 'SENTENCE',projected_parole_release_date, NULL)) AS projected_parole_release_date__sentence,
    ANY_VALUE(IF(source_view = 'SENTENCE_GROUP',projected_parole_release_date, NULL)) AS projected_parole_release_date__sentence_group, 
    ANY_VALUE(IF(source_view = 'SENTENCE',projected_full_term_release_date_min, NULL)) AS projected_full_term_release_date_min__sentence,
    ANY_VALUE(IF(source_view = 'SENTENCE_GROUP',projected_full_term_release_date_min, NULL)) AS projected_full_term_release_date_min__sentence_group, 
    ANY_VALUE(IF(source_view = 'SENTENCE',projected_full_term_release_date_max, NULL)) AS projected_full_term_release_date_max__sentence,
    ANY_VALUE(IF(source_view = 'SENTENCE_GROUP',projected_full_term_release_date_max, NULL)) AS projected_full_term_release_date_max__sentence_group,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5,6
)
SELECT 
    * EXCEPT(session_id, date_gap_id)
FROM
    (
    {aggregate_adjacent_spans(table_name = '_deduped_cte',
                              index_columns=['state_code','region_code','person_id','sentence_inferred_group_id'],
                              attribute = ['parole_eligibility_date__sentence',
                                           'parole_eligibility_date__sentence_group',
                                           'projected_parole_release_date__sentence',
                                           'projected_parole_release_date__sentence_group',
                                           'projected_full_term_release_date_min__sentence',
                                           'projected_full_term_release_date_min__sentence_group',
                                           'projected_full_term_release_date_max__sentence',
                                           'projected_full_term_release_date_max__sentence_group'],
                              end_date_field_name = "end_date_exclusive"
                              )
    
    }
    )
    WHERE parole_eligibility_date__sentence != parole_eligibility_date__sentence_group
        OR projected_parole_release_date__sentence!=parole_eligibility_date__sentence_group
        OR projected_full_term_release_date_min__sentence!=projected_full_term_release_date_min__sentence_group 
        OR projected_full_term_release_date_max__sentence!=projected_full_term_release_date_max__sentence_group
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
