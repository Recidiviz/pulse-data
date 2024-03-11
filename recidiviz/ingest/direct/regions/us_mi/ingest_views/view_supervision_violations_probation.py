# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing MDOC probation supervision violations information from OMNI."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT
        book_report.offender_booking_id,
        report_id,
        (DATE(start_date)) as start_date,
        (DATE(closing_date)) as closing_date
    FROM {ADH_OFFENDER_BOOKING_REPORT} book_report
    LEFT JOIN {ADH_REPORT}  report ON book_report.related_report_id = report.report_id
    INNER JOIN {ADH_OFFENDER_BOOKING} book ON book_report.offender_booking_id = book.offender_booking_id
    WHERE report_template_id = '301' -- Probation Violation Report
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="supervision_violations_probation",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="report_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
