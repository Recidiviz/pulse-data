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
"""Query for relevant metadata needed to support institutional worker status (known as "309") opportunity in Arkansas"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_NAME = "us_ar_institutional_worker_status_record"

US_AR_INSTITUTIONAL_WORKER_STATUS_QUERY_TEMPLATE = f"""
    WITH base AS (
        {join_current_task_eligibility_spans_with_external_id(
            state_code= "'US_AR'", 
            tes_task_query_view = 'institutional_worker_status_materialized',
            id_type = "'US_AR_OFFENDERID'",
            eligible_only=True,
        )}
    )
    ,
    approved_visitors AS (
        SELECT
            state_code,
            person_id,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(
                        ra_party_id AS party_id,
                        ra_seq_num AS seq_num,
                        ra_relationship_type AS relationship_type,
                        ra_first_name AS first_name,
                        ra_last_name AS last_name,
                        ra_middle_name AS middle_name,
                        ra_suffix AS suffix,
                        ra_dob AS date_of_birth,
                        ra_dob_is_approx AS date_of_birth_is_approximate,
                        ra_race AS race,
                        ra_sex AS sex,
                        relationship_status,
                        relationship_status_date,
                        ra_checklist AS checklist,
                        physical_address,
                        mailing_address,
                        visitation_review_date,
                        visitation_status_reason,
                        visitation_dur_days,
                        visitation_special_condition_1,
                        visitation_special_condition_2,
                        relationship_comments
                    )
                    ORDER BY ra_last_name, ra_first_name, ra_party_id
                )
            ) AS approved_visitors,
            COUNT(*) AS num_approved_visitors,
        FROM `{{project_id}}.analyst_data.us_ar_resident_approved_visitors_preprocessed_materialized`
        GROUP BY 1,2
    )
    SELECT
        base.person_id,
        base.external_id,
        base.state_code,
        base.reasons,
        base.is_eligible,
        base.is_almost_eligible,
        IFNULL(approved_visitors.approved_visitors, TO_JSON([])) AS approved_visitors,
        IFNULL(approved_visitors.num_approved_visitors, 0) AS num_approved_visitors,
        resident_metadata.* EXCEPT (person_id),
    FROM base
    LEFT JOIN `{{project_id}}.workflows_views.us_ar_resident_metadata_materialized` resident_metadata
    USING(person_id)
    LEFT JOIN approved_visitors
    USING(state_code, person_id)
"""

US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_NAME,
    view_query_template=US_AR_INSTITUTIONAL_WORKER_STATUS_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_AR
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_BUILDER.build_and_print()
