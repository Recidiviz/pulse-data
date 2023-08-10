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

"""Utilities for selecting from available report contexts."""
from recidiviz.reporting.constants import ReportType
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.context import (
    OutliersSupervisionOfficerSupervisorContext,
)
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.email_reporting_utils import Batch
from recidiviz.reporting.recipient import Recipient


def get_report_context(batch: Batch, recipient: Recipient) -> ReportContext:
    """Returns the appropriate report context for the given parameters, choosing the correct ReportContext
    implementation.

    Args:
        batch: Batch object containing information representing this report
        recipient: The retrieved data for this recipient
    """
    if batch.report_type == ReportType.OutliersSupervisionOfficerSupervisor:
        return OutliersSupervisionOfficerSupervisorContext(batch, recipient)

    raise KeyError(f"Unrecognized report type: {batch.report_type}")
