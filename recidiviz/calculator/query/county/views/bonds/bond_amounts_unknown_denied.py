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


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.common.constants.enum_canonical_strings import (
    bond_status_posted,
    bond_status_revoked,
    bond_type_denied,
    bond_type_not_required,
    bond_type_secured,
    present_without_info,
)
from recidiviz.persistence.database.schema.county.schema import Bond
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_NAME = "bond_amounts_unknown_denied"

BOND_AMOUNTS_UNKNOWN_DENIED_DESCRIPTION = f"""
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

NOTE: This may be easier or more readable if unknown/denied are written
as an enum unknown_or_denied column, then broken out into BOOL columns.
"""

BOND_AMOUNTS_UNKNOWN_DENIED_QUERY_TEMPLATE = """
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
"""

BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_NAME,
    view_query_template=BOND_AMOUNTS_UNKNOWN_DENIED_QUERY_TEMPLATE,
    description=BOND_AMOUNTS_UNKNOWN_DENIED_DESCRIPTION,
    bond_type_secured=bond_type_secured,
    bond_type_not_required=bond_type_not_required,
    present_without_info=present_without_info,
    bond_type_denied=bond_type_denied,
    bond_status_revoked=bond_status_revoked,
    bond_status_posted=bond_status_posted,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
    bond_table=Bond.__tablename__,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        BOND_AMOUNTS_UNKNOWN_DENIED_VIEW_BUILDER.build_and_print()
