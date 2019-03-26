# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
"""Add categorical bins to bond_amounts_all_bookings."""
# pylint: disable=line-too-long

from recidiviz.calculator.bq.views import bqview
from recidiviz.calculator.bq.views import view_config
from recidiviz.calculator.bq.views.bonds.bond_amounts_all_bookings import BOND_AMOUNTS_ALL_BOOKINGS_VIEW
from recidiviz.calculator.bq.views.vera.county_names import COUNTY_NAMES_VIEW

from recidiviz.common.constants.enum_canonical_strings import bond_type_denied

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.VIEWS_DATASET

BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_NAME = 'bond_amounts_all_bookings_bins'

BOND_AMOUNTS_ALL_BOOKINGS_BINS_DESCRIPTION = \
"""
Adds a 'bond_amount_category' field to `{bond_amounts_all_bookings_view}`
with either '{bond_type_denied}', 'UNKNOWN', or bins from:
0-500
500-100
1000-10000
10000-25000
25000-100000
100000+

Also sums the counts of populations, admissions, and releases for each
day-fips-category grouping.
""".format(
    bond_amounts_all_bookings_view=BOND_AMOUNTS_ALL_BOOKINGS_VIEW.view_id,
    bond_type_denied=bond_type_denied
)

BOND_AMOUNTS_ALL_BOOKINGS_BINS_QUERY = \
"""
/*{description}*/
WITH

Days AS (
  SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('1900-01-01', CURRENT_DATE('America/New_York'))) AS day
),

BondAmountBins AS (
  SELECT
    BondAmounts.*,
    CASE
      WHEN BondAmounts.denied THEN 'DENIED'
      WHEN BondAmounts.unknown THEN 'UNKNOWN'
      WHEN BondAmounts.total_bond_dollars <= 500 THEN 'AMT_000000_000500'
      WHEN BondAmounts.total_bond_dollars > 500 AND BondAmounts.total_bond_dollars <= 1000 THEN 'AMT_000500_001000'
      WHEN BondAmounts.total_bond_dollars > 1000 AND BondAmounts.total_bond_dollars <= 10000 THEN 'AMT_001000_010000'
      WHEN BondAmounts.total_bond_dollars > 10000 AND BondAmounts.total_bond_dollars <= 25000 THEN 'AMT_010000_025000'
      WHEN BondAmounts.total_bond_dollars > 25000 AND BondAmounts.total_bond_dollars <= 100000 THEN 'AMT_025000_100000'
      WHEN BondAmounts.total_bond_dollars > 100000 THEN 'AMT_100000_999999'
      ELSE 'UNKNOWN' END AS bond_amount_category
  FROM `{project_id}.{views_dataset}.{bond_amounts_all_bookings_view}` BondAmounts
),

AdmittedTable AS (
  SELECT day, fips, bond_amount_category, COUNT(DISTINCT(booking_id)) AS admitted
  FROM BondAmountBins
  JOIN Days
  ON day = BondAmountBins.admission_date
  GROUP BY day, fips, bond_amount_category
),

ReleasedTable AS (
  SELECT day, fips, bond_amount_category, COUNT(DISTINCT(booking_id)) AS released
  FROM BondAmountBins
  JOIN Days
  ON day = BondAmountBins.release_date
  GROUP BY day, fips, bond_amount_category
),

PersonCountTable AS (
  SELECT day, fips, bond_amount_category, COUNT(DISTINCT(booking_id)) AS person_count
  FROM BondAmountBins
  JOIN Days
  ON day BETWEEN BondAmountBins.admission_date AND COALESCE(BondAmountBins.release_date, CURRENT_DATE('America/New_York'))
  GROUP BY day, fips, bond_amount_category
)

SELECT PersonCountTable.day, PersonCountTable.fips, PersonCountTable.bond_amount_category, person_count, admitted, released, CountyNames.county_name, CountyNames.state
FROM PersonCountTable
FULL JOIN AdmittedTable
ON PersonCountTable.day = AdmittedTable.day
  AND PersonCountTable.fips = AdmittedTable.fips
FULL JOIN ReleasedTable
ON PersonCountTable.day = ReleasedTable.day
  AND PersonCountTable.fips = ReleasedTable.fips
JOIN
  `{project_id}.{views_dataset}.{county_names_view}` CountyNames
ON
  PersonCountTable.fips = CountyNames.fips
ORDER BY day DESC, fips, bond_amount_category
""".format(
    description=BOND_AMOUNTS_ALL_BOOKINGS_BINS_DESCRIPTION,
    project_id=PROJECT_ID,
    views_dataset=VIEWS_DATASET,
    bond_amounts_all_bookings_view=BOND_AMOUNTS_ALL_BOOKINGS_VIEW.view_id,
    county_names_view=COUNTY_NAMES_VIEW.view_id
)

BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW = bqview.BigQueryView(
    view_id=BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_NAME,
    view_query=BOND_AMOUNTS_ALL_BOOKINGS_BINS_QUERY
)

if __name__ == '__main__':
    print(BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW.view_id)
    print(BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW.view_query)
