# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""A view revealing when the number of clients for a certain category does not match the expected number for
that category."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PO_REPORT_CLIENTS_VIEW_NAME = 'po_report_clients'

PO_REPORT_CLIENTS_DESCRIPTION = """
  A list of officers that have a mismatch between the number of clients listed for a category and the number for that
  category.
"""

PO_REPORT_CLIENTS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT DISTINCT
      state_code as region_code, review_month, email_address, 'pos_discharges_clients-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized`
    WHERE ARRAY_LENGTH(pos_discharges_clients) != pos_discharges
    
    UNION ALL
    
    SELECT DISTINCT
      state_code as region_code, review_month, email_address, 'earned_discharges_clients-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized`
    WHERE ARRAY_LENGTH(earned_discharges_clients) != earned_discharges
    
    UNION ALL
    
    SELECT DISTINCT
      state_code as region_code, review_month, email_address, 'revocations_clients-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized`
    WHERE ARRAY_LENGTH(revocations_clients) != (crime_revocations + technical_revocations)
    
    UNION ALL
    
    SELECT DISTINCT
      state_code as region_code, review_month, email_address, 'absconsions_clients-mismatch'
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized`
    WHERE ARRAY_LENGTH(absconsions_clients) != absconsions
    """

PO_REPORT_CLIENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PO_REPORT_CLIENTS_VIEW_NAME,
    view_query_template=PO_REPORT_CLIENTS_QUERY_TEMPLATE,
    description=PO_REPORT_CLIENTS_DESCRIPTION,
    po_report_dataset=state_dataset_config.PO_REPORT_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_REPORT_CLIENTS_VIEW_BUILDER.build_and_print()
