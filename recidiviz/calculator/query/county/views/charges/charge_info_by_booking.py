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
"""A complete table of charge level and charge class, for every Booking."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.charges.charge_severity_all_bookings import (
    CHARGE_SEVERITY_ALL_BOOKINGS_VIEW_BUILDER,
)
from recidiviz.persistence.database.schema.county.schema import Charge
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGE_INFO_BY_BOOKING_VIEW_NAME = "charge_info_by_booking"

CHARGE_INFO_BY_BOOKING_DESCRIPTION = """
A complete table of charge level and charge class, for every Booking.
"""

CHARGE_INFO_BY_BOOKING_QUERY_TEMPLATE = """
WITH ChargesWithMostSevere as (
  SELECT c.booking_id, c.class, s.most_severe_charge, level from `{project_id}.{base_dataset}.{charge_table}` c JOIN `{project_id}.{views_dataset}.{charge_severity_all_bookings_view}` s ON c.booking_id = s.booking_id
)

SELECT MIN(level) as charge_level, MIN(class) as charge_class, booking_id FROM ChargesWithMostSevere
WHERE class = most_severe_charge
GROUP BY booking_id
"""

CHARGE_INFO_BY_BOOKING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CHARGE_INFO_BY_BOOKING_VIEW_NAME,
    view_query_template=CHARGE_INFO_BY_BOOKING_QUERY_TEMPLATE,
    description=CHARGE_INFO_BY_BOOKING_DESCRIPTION,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
    views_dataset=dataset_config.VIEWS_DATASET,
    charge_severity_all_bookings_view=CHARGE_SEVERITY_ALL_BOOKINGS_VIEW_BUILDER.view_id,
    charge_table=Charge.__tablename__,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CHARGE_INFO_BY_BOOKING_VIEW_BUILDER.build_and_print()
