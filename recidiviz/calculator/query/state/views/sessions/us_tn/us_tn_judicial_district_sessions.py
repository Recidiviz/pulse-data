# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Sessionized view of judicial district off of pre-processed raw TN sentencing data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_NAME = "us_tn_judicial_district_sessions"

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of judicial district off of pre-processed raw TN sentencing data"""

US_TN_JUDICIAL_DISTRICT_SESSIONS_QUERY_TEMPLATE = f"""
    --TODO(#10747): Remove judicial district preprocessing once hydrated in population metrics   
    WITH cte AS
    (
    SELECT
        person_id,
        state_code,
        judicial_district_code,
        sentence_effective_date AS start_date,
        LEAD(sentence_effective_date) OVER(PARTITION BY person_id ORDER BY sentence_effective_date) AS end_date_exclusive,
    FROM
        (
        SELECT 
            ex.person_id,
            'US_TN' AS state_code,
            CAST(jd.JudicialDistrict AS STRING) AS judicial_district_code,
            CAST(CAST(s.SentenceEffectiveDate AS datetime) AS DATE) AS sentence_effective_date,
        FROM `{{project_id}}.{{raw_dataset}}.Sentence_latest` s
        JOIN `{{project_id}}.{{raw_dataset}}.JOIdentification_latest` jd
            USING(OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber) 
        JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` ex
            ON ex.external_id = s.OffenderID
            AND ex.state_code = 'US_TN'
        --This needs to be unique on person id and sentence effective date in order to work properly. It is very
        --rare (0.2% of cases) that a person has more than one judicial district on the same start date and when that
        --occurs deterministically take the first code
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, sentence_effective_date ORDER BY judicial_district_code)=1
        )
    )
    ,
    sessionized_cte AS 
    (
    {aggregate_adjacent_spans(table_name='cte',
                       attribute='judicial_district_code',
                       session_id_output_name='judicial_district_session_id',
                       end_date_field_name='end_date_exclusive')}
    )
    ,
    last_day_of_data_cte AS
    (
    SELECT 
        MAX(GREATEST(start_date_inclusive, end_date_exclusive)) AS last_day_of_data
    FROM 
        (
        SELECT 
            start_date_inclusive, 
            end_date_exclusive 
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized`
        WHERE state_code = 'US_TN'
        UNION ALL
        SELECT 
            start_date_inclusive, 
            end_date_exclusive 
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized`
        WHERE state_code = 'US_TN'
        )
    )
    SELECT 
        person_id,
        state_code,
        judicial_district_session_id,
        judicial_district_code,
        start_date AS judicial_district_start_date,
        CASE WHEN DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) <= last_day_of_data THEN end_date_exclusive END AS judicial_district_end_date_exclusive,
        DATE_SUB(CASE WHEN DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) <= last_day_of_data THEN end_date_exclusive END, INTERVAL 1 DAY) AS judicial_district_end_date,
    FROM sessionized_cte
    JOIN last_day_of_data_cte ON TRUE
    WHERE start_date<=last_day_of_data
"""

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_NAME,
    view_query_template=US_TN_JUDICIAL_DISTRICT_SESSIONS_QUERY_TEMPLATE,
    description=US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_DESCRIPTION,
    raw_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_BUILDER.build_and_print()
