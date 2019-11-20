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
"""View that creates a Bond table with amounts and UNKNOWN or DENIED."""
# pylint: disable=line-too-long

from recidiviz.calculator.query import export_config, bqview

from recidiviz.common.constants.enum_canonical_strings import bond_status_posted
from recidiviz.common.constants.enum_canonical_strings import bond_status_revoked
from recidiviz.common.constants.enum_canonical_strings import bond_type_denied
from recidiviz.common.constants.enum_canonical_strings import bond_type_not_required
from recidiviz.common.constants.enum_canonical_strings import bond_type_secured
from recidiviz.common.constants.enum_canonical_strings import present_without_info

from recidiviz.persistence.database.schema.county.schema import Bond

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.COUNTY_BASE_TABLES_BQ_DATASET

BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_NAME = 'bond_amounts_unknown_denied'

BOND_AMOUNTS_UNKNOWN_DENIED_DESCRIPTION = \
"""
Create a Bond Amount table where:
If Bond.amount_dollars is present, keep it.
If Bond.bond_type = '{bond_type_not_required}' OR Bond.status = '{bond_status_posted}',
then set Bond.amount_dollars to 0.

If after that, Bond.amount_dollars is NULL, we must set one of
the `unknown` or `denied` columns to True,
according to the following rules:

`unknown` = True in the following cases:
1) Bond.amount_dollars IS NULL AND Bond.bond_type = '{bond_type_secured}'
2) Bond.amount_dollars IS NULL
     AND Bond.bond_type IS NULL
       AND Bond.status = '{present_without_info}'

`denied` = True in the following cases:
1) Bond.amount_dollars IS NULL AND Bond.bond_type = '{bond_type_denied}'
2) Bond.amount_dollars IS NULL
     AND Bond.bond_type IS NULL
       AND Bond.status = '{bond_status_revoked}'

Constraints:
Only one of `unknown` and `denied` can be True.
If `amount_dollars` is not NULL,  `unknown` and `denied` must both be False.

TODO(rasmi): This may be easier or more readable if unknown/denied are written
as an enum unknown_or_denied column, then broken out into BOOL columns.
""".format(
    bond_type_not_required=bond_type_not_required,
    bond_status_posted=bond_status_posted,
    bond_type_secured=bond_type_secured,
    present_without_info=present_without_info,
    bond_type_denied=bond_type_denied,
    bond_status_revoked=bond_status_revoked
)

BOND_AMOUNTS_UNKNOWN_DENIED_QUERY = \
"""
/*{description}*/
SELECT NewBond.booking_id, NewBond.bond_type, NewBond.status, NewBond.amount_dollars,
  CASE
  WHEN NewBond.amount_dollars IS NOT NULL THEN False
  -- Everything after this implies Bond.amount_dollars IS NULL.
  WHEN NewBond.bond_type = '{bond_type_secured}' THEN True
  WHEN NewBond.bond_type IS NULL
       AND NewBond.status = '{present_without_info}' THEN True
  ELSE False END AS unknown,
  CASE
  WHEN NewBond.amount_dollars IS NOT NULL THEN False
  -- Everything after this implies Bond.amount_dollars IS NULL.
  WHEN NewBond.bond_type = '{bond_type_denied}' THEN True
  WHEN NewBond.bond_type IS NULL
       AND NewBond.status = '{bond_status_revoked}' THEN True
  ELSE False END AS denied
FROM (
  SELECT
    Bond.booking_id, Bond.bond_type, Bond.status,
    -- Set records with Bond.bond_type = '{bond_type_not_required}' OR Bond.status = '{bond_status_posted}' to Bond.amount_dollars = 0.
    IF((Bond.bond_type = '{bond_type_not_required}' OR Bond.status = '{bond_status_posted}'), 0, Bond.amount_dollars) AS amount_dollars
    FROM `{project_id}.{base_dataset}.{bond_table}` Bond
) NewBond
""".format(
    description=BOND_AMOUNTS_UNKNOWN_DENIED_DESCRIPTION,
    bond_type_secured=bond_type_secured,
    bond_type_not_required=bond_type_not_required,
    present_without_info=present_without_info,
    bond_type_denied=bond_type_denied,
    bond_status_revoked=bond_status_revoked,
    bond_status_posted=bond_status_posted,
    project_id=PROJECT_ID,
    base_dataset=BASE_DATASET,
    bond_table=Bond.__tablename__
)

BOND_AMOUNTS_UNKNOWN_DENIED_VIEW = bqview.BigQueryView(
    view_id=BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_NAME,
    view_query=BOND_AMOUNTS_UNKNOWN_DENIED_QUERY
)

if __name__ == '__main__':
    print(BOND_AMOUNTS_UNKNOWN_DENIED_VIEW.view_id)
    print(BOND_AMOUNTS_UNKNOWN_DENIED_VIEW.view_query)
