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
"""Query for relevant metadata needed to support initial classification opportunity in Tennessee
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    us_tn_classification_forms,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_tn.initial_classification_review import (
    VIEW_BUILDER as US_TN_INITIAL_CLASSIFICATION_REVIEW_TES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_COLLAPSED_TES_SPANS_ADDRESS = build_collapsed_tes_spans_view_materialized_address(
    US_TN_INITIAL_CLASSIFICATION_REVIEW_TES_VIEW_BUILDER
)


US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_NAME = (
    "us_tn_initial_classification_review_record"
)

US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support initial classification opportunity in Tennessee
    """

US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_QUERY_TEMPLATE = f"""
    -- These are the questions we fill from the previous available CAF form. For initial classification, this will 
    -- almost certainly be erroneous information since many of these speak to criminal history which likely changes
    -- between stints in prison
    SELECT * EXCEPT(form_information_q3_score,
                    form_information_q4_score,
                    form_information_q5_score,
                    form_information_q9_score),
            CAST(NULL AS INT64) AS form_information_q3_score,
            CAST(NULL AS INT64) AS form_information_q4_score,
            CAST(NULL AS INT64) AS form_information_q5_score,
            CAST(NULL AS INT64) AS form_information_q9_score,
    FROM ({us_tn_classification_forms(tes_view="initial_classification_review",
                                      where_clause="WHERE "
                                                    "CURRENT_DATE('US/Eastern') BETWEEN tes.start_date "
                                                    f"AND {nonnull_end_date_exclusive_clause('tes.end_date')} "
                                                    "AND tes.is_eligible",
                                      collapsed_tes_spans_address=_COLLAPSED_TES_SPANS_ADDRESS,
                                      )
        })
    
"""

US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_NAME,
    view_query_template=US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_QUERY_TEMPLATE,
    description=US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.build_and_print()
