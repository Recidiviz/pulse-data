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
"""Query for clients eligible for transfer to minimum telephone reporting in Michigan
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.eligibility_spans.us_mi.complete_transfer_to_telephone_reporting_request import (
    VIEW_BUILDER as US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_TES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_VIEW_NAME = (
    "us_mi_complete_transfer_to_telephone_reporting_request_record"
)

US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_DESCRIPTION = """
    Query for clients eligible for transfer to minimum telephone reporting in Michigan
    """

_COLLAPSED_TES_SPANS_ADDRESS = build_collapsed_tes_spans_view_materialized_address(
    US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_TES_VIEW_BUILDER
)


US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_QUERY_TEMPLATE = f"""
SELECT
    tes.person_id,
    pei.external_id,
    tes.state_code,
    tes_collapsed.start_date AS metadata_eligible_date,
    reasons,
    tes.is_eligible,
    tes.is_almost_eligible,
FROM `{{project_id}}.{US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_TES_VIEW_BUILDER.table_for_query.to_str()}` tes
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON tes.state_code = pei.state_code 
    AND tes.person_id = pei.person_id
    AND pei.id_type = "US_MI_DOC"
--join view that sessionizes spans based on eligibility to get eligible start date
INNER JOIN `{{project_id}}.{_COLLAPSED_TES_SPANS_ADDRESS.to_str()}` tes_collapsed
    ON tes_collapsed.state_code = tes.state_code
    AND tes_collapsed.person_id = tes.person_id 
    AND CURRENT_DATE('US/Pacific') BETWEEN tes_collapsed.start_date AND {nonnull_end_date_exclusive_clause('tes_collapsed.end_date')}
WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
    AND tes.is_eligible
    AND tes.state_code = 'US_MI'
"""

US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
