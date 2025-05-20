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
"""Utah client metadata, including Interstate Compact aka ICOTS status and the
case court number for the sentence surfaced in the client record"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_UT_CLIENT_METADATA_VIEW_NAME = "us_ut_client_metadata"

US_UT_CLIENT_METADATA_VIEW_DESCRIPTION = """
Utah client metadata
"""

US_UT_CLIENT_METADATA_VIEW_QUERY_TEMPLATE = """
-- The supervision session we use for the client record; there should be one row per person
WITH session_start_dates AS (
    SELECT
        person_id,
        start_date
    FROM `{project_id}.{sessions_dataset}.prioritized_supervision_super_sessions_materialized`
    WHERE state_code = "US_UT"
    AND end_date_exclusive IS NULL
),
current_sentences AS (
    SELECT 
        sd.person_id,
        ssp.sentence_id
    FROM session_start_dates sd
    LEFT JOIN `{project_id}.sentence_sessions_v2_all.sentence_serving_period_materialized` ssp
        USING (person_id)
    WHERE 
        -- Only include sentences whose serving dates overlap with the session
        ((ssp.end_date_exclusive IS NULL) OR (ssp.end_date_exclusive > sd.start_date))
        -- Check that the serving period began before the supervision period, 
        -- instead of on the same day, to include people who transitioned from
        -- incarceration to supervision, for example
        AND ssp.start_date <= sd.start_date
),
court_case_nums AS (
    SELECT 
        sd.person_id,
        c.crt_case_num AS court_case_number
    FROM session_start_dates sd
    LEFT JOIN current_sentences cs
        USING (person_id)
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_sentence` ss
        ON sd.person_id = ss.person_id 
        AND cs.sentence_id = ss.sentence_id
    LEFT JOIN `{project_id}.{us_ut_raw_data_dataset}.crt_case_latest` c
        ON c.intr_case_num = ss.external_id
),

icots AS (
    SELECT
        sd.person_id,
        IF(REGEXP_CONTAINS(slr.supervision_level_raw_text, 'COMPACT IN'), True, False) AS interstate_compact_in,
    FROM session_start_dates sd
    LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_raw_text_sessions_materialized` slr
        ON sd.person_id = slr.person_id
        AND slr.end_date_exclusive IS NULL
)

SELECT
    sd.person_id,
    court_case_number,
    interstate_compact_in
FROM session_start_dates sd
LEFT JOIN court_case_nums USING (person_id)
LEFT JOIN icots USING (person_id)
"""

US_UT_CLIENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_UT_CLIENT_METADATA_VIEW_NAME,
    view_query_template=US_UT_CLIENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_UT_CLIENT_METADATA_VIEW_DESCRIPTION,
    should_materialize=True,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_ut_raw_data_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_UT, instance=DirectIngestInstance.PRIMARY
    ),
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_UT_CLIENT_METADATA_VIEW_BUILDER.build_and_print()
