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
"""View that logs earned credit activity in US_MA"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import sessionize_ledger_data
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_ma_person_projected_date_sessions_preprocessed"
)

US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = """"""

US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
WITH cte AS
(
--TODO(#42451): Deprecate this view if sentence-level data is ingested from US_MA
/*
This first query stores the adjusted_max_release_date as the projected_full_term_release_date_max with an update date
that is the US_MA report run date. Each time we get an updated report, we will get a new record with a new update_date
that captures any changes to the date fields or earned credit fields
*/
SELECT
    state_code,
    person_id,
    update_date,
    -- Rounding the credit fields and casting to an integer as this is how these values are represented elsewhere in our
    -- schema
    --TODO(#42454): Schema change to capture multiple types of earned time
    CAST(ROUND(total_state_credit) AS INT64) AS earned_time_days,
    CAST(ROUND(total_completion_credit) AS INT64) AS good_time_days,
    -- rts (release-to-supervision) date is technically not a parole eligibility date and this is a separate concept in
    -- US_MA. However, we do not receive parole_eligibility_date and this is probably the best spot for it in our schema
    CAST(rts_date AS DATE) AS parole_eligibility_date,
    CAST(adjusted_max_release_date AS DATE) AS projected_full_term_release_date_max,
FROM `{{project_id}}.{{analyst_views_dataset}}.us_ma_projected_dates_materialized`

UNION ALL

/*
This second query exists to capture the original max release date. This is a static date that does not change. Therefore,
it best fits in our schema as distinct row that represents our best estimate of the projected_full_term_release_date_max
up until the first report run date, at which point we start using the adjusted_max_release_date as our 
projected_full_term_release_date_max value. Ideally, the update_date here would be the date the sentence starts being
served. However, we do not have this date so we use the egt_initial_date (the first date that a person starts earning
good time) instead. Realistically for our calculations, we just need this date to be any date that comes before the
first report run date.
*/
SELECT DISTINCT
  state_code,
  person_id,
  egt_initial_date AS update_date,
  CAST(NULL AS INT64) AS earned_time_days,
  CAST(NULL AS INT64) AS good_time_days,
  CAST(NULL AS DATE) AS parole_eligibiity_date,
  CAST(original_max_release_date AS DATE) AS projected_full_term_release_date_max
FROM `{{project_id}}.{{analyst_views_dataset}}.us_ma_projected_dates_materialized`
)
,
_sessionized AS
(
{sessionize_ledger_data(
    table_name='cte',
    index_columns=['state_code','person_id'],
    update_column_name='update_date',
    attribute_columns=['earned_time_days','good_time_days','parole_eligibility_date','projected_full_term_release_date_max'],
)}
)
/*
Structure the data so that it can be unioned in with `person_projected_date_sessions`. This includes creating an array
of structs that captures sentence-level information. Since we don't have "sentences" in US_MA, we just pull in the 
person-level information as a single sentence. 
*/
SELECT 
    state_code,
    person_id,
    person_id AS sentence_inferred_group_id,
    start_date,
    end_date_exclusive,
    parole_eligibility_date AS group_parole_eligibility_date, 
    CAST(NULL AS DATE) AS group_projected_parole_release_date,
    CAST(NULL AS DATE) AS group_projected_full_term_release_date_min,
    projected_full_term_release_date_max AS group_projected_full_term_release_date_max,
    good_time_days AS group_good_time_days,
    earned_time_days AS group_earned_time_days,
    CAST(NULL AS BOOL) AS has_any_out_of_state_sentences,
    CAST(NULL AS BOOL) AS has_any_in_state_sentences,
    ARRAY_AGG(
        STRUCT(
            person_id AS sentence_id,
            parole_eligibility_date AS sentence_parole_eligibility_date,
            CAST(NULL AS DATE) AS sentence_projected_parole_release_date,
            CAST(NULL AS DATE) AS sentence_projected_full_term_release_date_min,
            projected_full_term_release_date_max AS sentence_projected_full_term_release_date_max,
            CAST(NULL AS INT64) AS sentence_length_days_min,
            CAST(NULL AS INT64) AS sentence_length_days_max,
            good_time_days AS sentence_good_time_days,
            earned_time_days AS sentence_earned_time_days,
            CAST(NULL AS STRING) AS sentencing_authority
            )
            ORDER BY person_id
        ) AS sentence_array 
FROM _sessionized
WHERE person_id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
"""

US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
