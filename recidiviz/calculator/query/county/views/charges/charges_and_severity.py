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
"""Assigns a severity to each Charge record."""
# pylint: disable=line-too-long

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import export_config
from recidiviz.calculator.query.county import view_config
from recidiviz.calculator.query.county.views.charges.charge_class_severity_ranks import CHARGE_CLASS_SEVERITY_RANKS_VIEW

from recidiviz.common.constants.enum_canonical_strings import external_unknown

from recidiviz.persistence.database.schema.county.schema import Charge

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.COUNTY_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.VIEWS_DATASET

CHARGES_AND_SEVERITY_VIEW_NAME = 'charges_and_severity'

CHARGES_AND_SEVERITY_DESCRIPTION = \
"""
Assigns a numeric column "severity" to each charge.
Charge class severity is defined in `{views_dataset}.charge_class_severity_ranks`.
The lower the number, the more severe the charge class (1 is most severe, 8 is least).
""".format(
    views_dataset=VIEWS_DATASET
)

CHARGES_AND_SEVERITY_QUERY = \
"""
/*{description}*/
SELECT charge_id, booking_id, class, severity
FROM (
  -- Assign '{external_unknown}' if charge class is NULL.
  -- These will all be lumped into an UNKNOWN category for visualization.
  SELECT charge_id, booking_id, COALESCE(class, '{external_unknown}') AS class
  FROM
  `{project_id}.{base_dataset}.{charge_table}`
) Charge
LEFT JOIN
  `{project_id}.{views_dataset}.{charge_class_severity_ranks_view}` ChargeClassSeverity
ON
  Charge.class = ChargeClassSeverity.charge_class
""".format(
    description=CHARGES_AND_SEVERITY_DESCRIPTION,
    external_unknown=external_unknown,
    project_id=PROJECT_ID,
    base_dataset=BASE_DATASET,
    views_dataset=VIEWS_DATASET,
    charge_table=Charge.__tablename__,
    charge_class_severity_ranks_view=CHARGE_CLASS_SEVERITY_RANKS_VIEW.view_id
)

CHARGES_AND_SEVERITY_VIEW = BigQueryView(
    view_id=CHARGES_AND_SEVERITY_VIEW_NAME,
    view_query=CHARGES_AND_SEVERITY_QUERY
)

if __name__ == '__main__':
    print(CHARGES_AND_SEVERITY_VIEW.view_id)
    print(CHARGES_AND_SEVERITY_VIEW.view_query)
