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
"""Preprocessed view of fines/fees invoices, unique on person, date, and state code"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INVOICES_PREPROCESSED_VIEW_NAME = "invoices_preprocessed"

INVOICES_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of fines/fees invoices, unique on person, date, and
state code"""

INVOICES_PREPROCESSED_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    external_id,
    fee_type,
    invoice_date,
    invoice_amount,
    invoice_amount_adjusted,
FROM
    `{project_id}.{analyst_dataset}.us_tn_invoices_preprocessed`
    
UNION ALL

SELECT
    state_code,
    person_id,
    external_id,
    fee_type,
    invoice_date,
    invoice_amount,
    invoice_amount_adjusted,
FROM
    `{project_id}.{analyst_dataset}.us_me_invoices_preprocessed`

"""

INVOICES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=INVOICES_PREPROCESSED_VIEW_NAME,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    description=INVOICES_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=INVOICES_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVOICES_PREPROCESSED_VIEW_BUILDER.build_and_print()
