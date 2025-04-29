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
"""Logic to retrieve data for the SupervisionOfficerSupervisor report."""

from typing import Dict

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import OfficerSupervisorReportData
from recidiviz.reporting.constants import Batch


def retrieve_data_for_outliers_supervision_officer_supervisor(
    batch: Batch,
) -> Dict[str, OfficerSupervisorReportData]:
    """Retrieves the data for Outliers' supervision officer supervisor reports.
    Excludes any reports where there are no metrics that have outliers to highlight."""
    batch_date = utils.get_date_from_batch_id(batch)
    all_report_data = OutliersQuerier(
        batch.state_code,
        # This querier is only used for emails, so there are no user feature variants to check.
        # Limit our metrics to ones that a user with default FV values would see.
        [],
    ).get_officer_level_report_data_for_all_officer_supervisors(end_date=batch_date)
    return {key: report for key, report in all_report_data.items() if report.metrics}
