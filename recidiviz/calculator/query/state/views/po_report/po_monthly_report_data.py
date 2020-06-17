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
"""Data to populate the monthly PO report email."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import PO_REPORT_DATASET

PO_MONTHLY_REPORT_DATA_VIEW_NAME = 'po_monthly_report_data'

PO_MONTHLY_REPORT_DATA_DESCRIPTION = """
 Monthly data regarding an officer's success in discharging people from supervision, recommending early discharge
 from supervision, and keeping cases in compliance with state standards.
 """

# TODO(3364): Fill this view in once all of the views it depends on are implemented
PO_MONTHLY_REPORT_DATA_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT 'US_ID' as state_code, 'NOT_YET_IMPLEMENTED' AS po_monthly_report
    """

PO_MONTHLY_REPORT_DATA_VIEW = BigQueryView(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=PO_MONTHLY_REPORT_DATA_VIEW_NAME,
    view_query_template=PO_MONTHLY_REPORT_DATA_QUERY_TEMPLATE,
    description=PO_MONTHLY_REPORT_DATA_DESCRIPTION,
    po_report_dataset=PO_REPORT_DATASET
)

if __name__ == '__main__':
    print(PO_MONTHLY_REPORT_DATA_VIEW.view_id)
    print(PO_MONTHLY_REPORT_DATA_VIEW.view_query)
