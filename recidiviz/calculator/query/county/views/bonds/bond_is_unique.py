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
"""Lists each bond_id and decides if it is unique, i.e. it belongs to only one charge."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.persistence.database.schema.county.schema import Bond, Booking, Charge
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BOND_IS_UNIQUE_VIEW_NAME = "bond_is_unique"

BOND_IS_UNIQUE_DESCRIPTION = """
A convenience view that lists each bond_id.
is_unique is true if the bond is only attached to one charge and false otherwise,
i.e. if it is shared between multiple charges.
"""

BOND_IS_UNIQUE_QUERY_TEMPLATE = """
/*{description}*/
SELECT Bond.bond_id, COUNT(*) = 1 AS is_unique
FROM `{project_id}.{base_dataset}.{booking_table}` Booking
JOIN `{project_id}.{base_dataset}.{charge_table}` Charge ON Booking.booking_id = Charge.booking_id
JOIN `{project_id}.{base_dataset}.{bond_table}` Bond ON Charge.bond_id = Bond.bond_id
GROUP BY Bond.bond_id
"""

BOND_IS_UNIQUE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=BOND_IS_UNIQUE_VIEW_NAME,
    view_query_template=BOND_IS_UNIQUE_QUERY_TEMPLATE,
    description=BOND_IS_UNIQUE_DESCRIPTION,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
    booking_table=Booking.__tablename__,
    charge_table=Charge.__tablename__,
    bond_table=Bond.__tablename__,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        BOND_IS_UNIQUE_VIEW_BUILDER.build_and_print()
