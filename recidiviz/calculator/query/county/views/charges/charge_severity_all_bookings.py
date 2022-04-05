# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Assigns a 'most_severe_charge' to each Booking."""

from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.county import view_config
from recidiviz.calculator.query.county.views.charges.charges_and_severity import CHARGES_AND_SEVERITY_VIEW

from recidiviz.common.constants.enum_canonical_strings import external_unknown

from recidiviz.persistence.database.schema.county.schema import Booking

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.COUNTY_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.VIEWS_DATASET

CHARGE_SEVERITY_ALL_BOOKINGS_VIEW_NAME = 'charge_severity_all_bookings'

CHARGE_SEVERITY_ALL_BOOKINGS_DESCRIPTION = \
"""
For each booking_id, create a column called 'most_severe_charge'
which defines the severity of its most severe charge.
See `{views_dataset}.{charges_and_severity_view}` for details.
Bookings without charges have most_severe_charge listed as 'EXTERNAL_UNKNOWN'.
""".format(
    views_dataset=VIEWS_DATASET,
    charges_and_severity_view=CHARGES_AND_SEVERITY_VIEW.view_id
)

CHARGE_SEVERITY_ALL_BOOKINGS_QUERY = \
"""
/*{description}*/
SELECT Booking.booking_id, COALESCE(most_severe_charge, '{external_unknown}') AS most_severe_charge
FROM (
  SELECT booking_id, most_severe_charge FROM (
    SELECT
      booking_id,
      -- Select the first value of this group. Since we are sorting by charge severity,
      -- it will be the most severe charge. TODO: Use ARRAY_AGG instead?
      FIRST_VALUE(class) OVER (PARTITION BY booking_id ORDER BY severity ASC) AS most_severe_charge
    FROM `{project_id}.{views_dataset}.{charges_and_severity_view}`
  )
  GROUP BY booking_id, most_severe_charge
) BookingsWithCharges
-- Join the above table with the entire Booking table because
-- the above only considers bookings with charges, and we want
-- to consider bookings without charges as "{external_unknown} for visualization.
RIGHT JOIN
  `{project_id}.{base_dataset}.{booking_table}` Booking
ON BookingsWithCharges.booking_id = Booking.booking_id
""".format(
    description=CHARGE_SEVERITY_ALL_BOOKINGS_DESCRIPTION,
    external_unknown=external_unknown,
    project_id=PROJECT_ID,
    base_dataset=BASE_DATASET,
    views_dataset=VIEWS_DATASET,
    charges_and_severity_view=CHARGES_AND_SEVERITY_VIEW.view_id,
    booking_table=Booking.__tablename__
)

CHARGE_SEVERITY_ALL_BOOKINGS_VIEW = bqview.BigQueryView(
    view_id=CHARGE_SEVERITY_ALL_BOOKINGS_VIEW_NAME,
    view_query=CHARGE_SEVERITY_ALL_BOOKINGS_QUERY
)

if __name__ == '__main__':
    print(CHARGE_SEVERITY_ALL_BOOKINGS_VIEW.view_id)
    print(CHARGE_SEVERITY_ALL_BOOKINGS_VIEW.view_query)
