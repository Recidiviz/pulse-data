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
"""Query for relevant metadata needed to support upcoming restrictive housing hearing opportunity in Missouri
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_mo_query_fragments import (
    current_bed_stay_cte,
    hearings_dedup_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_NAME = (
    "us_mo_upcoming_restrictive_housing_hearing_record"
)

US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support upcoming restrictive housing hearing opportunity in Missouri 
    """
US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_QUERY_TEMPLATE = f"""
    WITH base_query AS (
        SELECT
           tes.person_id,
           pei.external_id, 
           tes.state_code,
           tes.reasons AS reasons,
        FROM `{{project_id}}.{{task_eligibility_dataset}}.upcoming_restrictive_housing_hearing_materialized`  tes
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
          USING(person_id)
        WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
            AND tes.is_eligible
            AND tes.state_code = 'US_MO'
    )
    ,
    {hearings_dedup_cte()}
    ,
    most_recent_hearings AS (
        SELECT DISTINCT
            state_code,
            person_id,
            FIRST_VALUE(hearing_date) OVER person_window AS most_recent_hearing_date,
            FIRST_VALUE(hearing_type) OVER person_window AS most_recent_hearing_type,
            FIRST_VALUE(hearing_facility) OVER person_window AS most_recent_hearing_facility,
        FROM hearings
        WINDOW person_window AS (
            PARTITION by person_id, state_code
            ORDER BY hearing_date DESC
        )
    )
    ,
    current_confinement_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(start_date) OVER w as start_date,
        FROM `{{project_id}}.{{sessions_dataset}}.us_mo_confinement_type_sessions_materialized`
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY confinement_type_session_id DESC
        )
    )
    ,
    current_housing_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(facility_code) OVER w as facility_code,
        FROM `{{project_id}}.{{sessions_dataset}}.us_mo_housing_stay_sessions_materialized`
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY housing_stay_session_id DESC
        )
    )
    ,
    {current_bed_stay_cte()}
    ,
    final AS (
        SELECT
            base.external_id,
            base.state_code,
            base.reasons,
            hearings.most_recent_hearing_date AS metadata_most_recent_hearing_date,
            hearings.most_recent_hearing_type AS metadata_most_recent_hearing_type,
            hearings.most_recent_hearing_facility AS metadata_most_recent_hearing_facility,
            housing.facility_code AS metadata_current_facility,
            confinement.start_date AS metadata_restrictive_housing_start_date,
            bed.bed_number AS metadata_bed_number,
            bed.room_number AS metadata_room_number,
            bed.complex_number AS metadata_complex_number,
            bed.building_number AS metadata_building_number,
            bed.housing_use_code AS metadata_housing_use_code,
        FROM base_query base
        LEFT JOIN most_recent_hearings hearings
        USING (person_id)
        LEFT JOIN current_housing_stay housing
        USING (person_id)
        LEFT JOIN current_confinement_stay confinement
        USING (person_id)
        LEFT JOIN current_bed_stay bed
        USING (person_id)
    )
    SELECT * FROM final
"""

US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_NAME,
    view_query_template=US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_QUERY_TEMPLATE,
    description=US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MO
    ),
    should_materialize=True,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_UPCOMING_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER.build_and_print()
