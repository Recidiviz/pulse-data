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
"""Add categorical bins to bond_amounts_all_bookings."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.bonds.bond_amounts_all_bookings import (
    BOND_AMOUNTS_ALL_BOOKINGS_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.vera.county_names import (
    COUNTY_NAMES_VIEW_BUILDER,
)
from recidiviz.common.constants.enum_canonical_strings import bond_type_denied
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_NAME = "bond_amounts_all_bookings_bins"

BOND_AMOUNTS_ALL_BOOKINGS_BINS_DESCRIPTION = f"""
Adds a 'bond_amount_category' field to `{BOND_AMOUNTS_ALL_BOOKINGS_VIEW_BUILDER.view_id}`
with either '{bond_type_denied}', 'UNKNOWN', or bins from:
0-500
500-100
1000-10000
10000-25000
25000-100000
100000+

Also sums the counts of populations, admissions, and releases for each
day-fips-category grouping.
"""

BOND_AMOUNTS_ALL_BOOKINGS_BINS_QUERY_TEMPLATE = """
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

BondAmountBinsNoZero AS (
  SELECT
    BondAmounts.*,
    CASE
      WHEN BondAmounts.denied THEN 'DENIED'
      WHEN BondAmounts.unknown THEN 'UNKNOWN'
      WHEN BondAmounts.total_bond_dollars > 0 AND BondAmounts.total_bond_dollars <= 500 THEN 'AMT_000000_000500'
      WHEN BondAmounts.total_bond_dollars > 500 AND BondAmounts.total_bond_dollars <= 1000 THEN 'AMT_000500_001000'
      WHEN BondAmounts.total_bond_dollars > 1000 AND BondAmounts.total_bond_dollars <= 10000 THEN 'AMT_001000_010000'
      WHEN BondAmounts.total_bond_dollars > 10000 AND BondAmounts.total_bond_dollars <= 25000 THEN 'AMT_010000_025000'
      WHEN BondAmounts.total_bond_dollars > 25000 AND BondAmounts.total_bond_dollars <= 100000 THEN 'AMT_025000_100000'
      WHEN BondAmounts.total_bond_dollars > 100000 THEN 'AMT_100000_999999'
      ELSE 'UNKNOWN' END AS bond_amount_category_no_zero
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
),

PersonCountTableNoZero AS (
  SELECT day, fips, bond_amount_category_no_zero, COUNT(DISTINCT(booking_id)) AS person_count_no_zero
  FROM BondAmountBinsNoZero
  JOIN Days
  ON day BETWEEN BondAmountBinsNoZero.admission_date AND COALESCE(BondAmountBinsNoZero.release_date, CURRENT_DATE('America/New_York'))
  GROUP BY day, fips, bond_amount_category_no_zero
)

SELECT PersonCountTable.day, PersonCountTable.fips, PersonCountTable.bond_amount_category, person_count, admitted, released, person_count_no_zero, CountyNames.county_name, CountyNames.state
FROM PersonCountTable
FULL JOIN AdmittedTable
ON PersonCountTable.day = AdmittedTable.day
  AND PersonCountTable.fips = AdmittedTable.fips
FULL JOIN ReleasedTable
ON PersonCountTable.day = ReleasedTable.day
  AND PersonCountTable.fips = ReleasedTable.fips
LEFT JOIN PersonCountTableNoZero
ON PersonCountTable.day = PersonCountTableNoZero.day
  AND PersonCountTable.bond_amount_category = PersonCountTableNoZero.bond_amount_category_no_zero
  AND PersonCountTable.fips = PersonCountTableNoZero.fips
JOIN
  `{project_id}.{views_dataset}.{county_names_view}` CountyNames
ON
  PersonCountTable.fips = CountyNames.fips
ORDER BY day DESC, fips, bond_amount_category
"""

BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_NAME,
    view_query_template=BOND_AMOUNTS_ALL_BOOKINGS_BINS_QUERY_TEMPLATE,
    description=BOND_AMOUNTS_ALL_BOOKINGS_BINS_DESCRIPTION,
    views_dataset=dataset_config.VIEWS_DATASET,
    bond_amounts_all_bookings_view=BOND_AMOUNTS_ALL_BOOKINGS_VIEW_BUILDER.view_id,
    county_names_view=COUNTY_NAMES_VIEW_BUILDER.view_id,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        BOND_AMOUNTS_ALL_BOOKINGS_BINS_VIEW_BUILDER.build_and_print()
