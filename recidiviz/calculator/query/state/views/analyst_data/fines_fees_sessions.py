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
"""View showing running fines/fees balances over time, accounting for different levels of legal status such as
compartment level 0 and system session)"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FINES_FEES_SESSIONS_VIEW_NAME = "fines_fees_sessions"

FINES_FEES_SESSIONS_VIEW_DESCRIPTION = """View showing running fines/fees balances over time, accounting for
different levels of legal status such as compartment level 0 and system session)"""


FINES_FEES_SESSIONS_QUERY_TEMPLATE = """
SELECT 
    state_code,
    person_id,
    NULL AS external_id,
    supervision_legal_authority_session_id,
    fee_type,
    transaction_type,
    start_date,
    end_date,
    unpaid_balance,
    unpaid_balance_within_supervision_session    
FROM
    `{project_id}.{analyst_dataset}.us_tn_fines_fees_sessions_preprocessed`
    
UNION ALL

SELECT 
    state_code,
    person_id,
    external_id,
    NULL AS supervision_legal_authority_session_id,
    fee_type,
    transaction_type,
    start_date,
    end_date,
    unpaid_balance,
    NULL AS unpaid_balance_within_supervision_session    
FROM
    `{project_id}.{analyst_dataset}.us_me_fines_fees_sessions_preprocessed`

"""
FINES_FEES_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=FINES_FEES_SESSIONS_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=FINES_FEES_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=FINES_FEES_SESSIONS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        FINES_FEES_SESSIONS_VIEW_BUILDER.build_and_print()
