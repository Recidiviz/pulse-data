# pylint: skip-file
"""pa_suboffices

Revision ID: a8c3e44943e9
Revises: cfee400e144c
Create Date: 2021-02-16 11:40:40.836657

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "a8c3e44943e9"
down_revision = "cfee400e144c"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """WITH split_sites AS (
    SELECT
        supervision_period_id,
        SPLIT_PART(supervision_site, '|', 1) AS district_code,
        SPLIT_PART(supervision_site, '|', 2) AS org_code
    FROM state_supervision_period
    WHERE state_code = 'US_PA' AND supervision_site IS NOT NULL
),
with_suboffices AS (
    SELECT
    supervision_period_id,
    district_code,
    org_code,
    CASE
        WHEN org_code IN (
            '1000', '1200', '1300', '1400', '1500', '1600', '1700', '1800', '1900', 
            '1910', '1920', '2000', '2100', '2110', '2120', '2200', '2210', '2220',
            '2221', '2230', '2240', '2250', '2300', '3000', '3010', '3020', '3030',
            '4000', '4010', '4100',  '4130', '4140', '4150', '4200', '4210', '4220',
            '4230', '4300', '4310', '4320', '6000', '8000') OR org_code LIKE '9%' 
            THEN 'CO - Central Office'
        WHEN org_code IN ('5100') THEN '01 - PHILADELPHIA DO'
        WHEN org_code LIKE '511%' THEN '01 - NORTHEAST'
        WHEN org_code LIKE '512%' THEN '01 - NORTHWEST'
        WHEN org_code IN ('5138', '5139') OR org_code LIKE '515%' THEN '01 - SPECIALIZED'
        WHEN org_code LIKE '513%' THEN '01 - COUNTYWIDE'
        WHEN org_code LIKE '514%' THEN '01 - WEST'
        WHEN org_code IN ('5201', '5204', '5205') THEN '10 - NORRISTOWN'
        WHEN org_code LIKE '520%' THEN '10 - CHESTER DO'
        WHEN org_code IN ('6105', '6108', '6114') THEN '03 - YORK'
        WHEN org_code IN ('6106', '6109', '6113') THEN '03 - LANCASTER'
        WHEN org_code LIKE '61%' THEN '03 - HARRISBURG DO'
        WHEN org_code IN ('6204', '6205', '6206', '6211') THEN '07 - READING'
        WHEN org_code IN ('6208') THEN '07 -  WERNERSVILLE'
        WHEN org_code LIKE '62%' THEN '07 - ALLENTOWN DO'
        WHEN org_code LIKE '63%' THEN '05 - WILLIAMSPORT DO'
        WHEN org_code LIKE '64%' THEN '04 - SCRANTON DO'
        WHEN org_code IN ('7111', '7112', '7113', '7114', '7115', '7116', '7117') THEN '02 - NORTH SHORE'
        WHEN org_code IN ('7123', '7125') THEN '02 - GREENSBURG'
        WHEN org_code LIKE '71%' THEN '02 - PITTSBURGH DO'
        WHEN org_code LIKE '72%' THEN '09 - ALTOONA DO'
        WHEN org_code IN ('7303', '7307')  THEN '08 - FRANKLIN'
        WHEN org_code IN ('7304', '7308') THEN '08 - BUTLER'
        WHEN org_code LIKE '73%' THEN '08 - MERCER DO'
        WHEN org_code LIKE '74%' THEN '06 - ERIE DO'
        ELSE NULL
    END AS suboffice_id
    FROM split_sites
),
new_supervision_sites AS (
    SELECT supervision_period_id, CONCAT(district_code, '|', suboffice_id, '|', org_code) AS updated_supervision_site
    FROM with_suboffices
)
UPDATE {table_name} sp
SET supervision_site = new_sp.updated_supervision_site
FROM new_supervision_sites new_sp
WHERE sp.supervision_period_id = new_sp.supervision_period_id; 
"""

DOWNGRADE_QUERY = """
WITH split_sites AS (
    SELECT
        supervision_period_id,
        SPLIT_PART(supervision_site, '|', 1) AS district_code,
        SPLIT_PART(supervision_site, '|', 2) AS suboffice_id,
        SPLIT_PART(supervision_site, '|', 3) AS org_code
    FROM state_supervision_period
    WHERE state_code = 'US_PA' AND supervision_site IS NOT NULL
),
downgraded_supervision_sites AS (
    SELECT supervision_period_id, CONCAT(district_code, '|', org_code) AS updated_supervision_site
    FROM split_sites
)
UPDATE {table_name} sp
SET supervision_site = old_sp.updated_supervision_site
FROM downgraded_supervision_sites old_sp
WHERE sp.supervision_period_id = old_sp.supervision_period_id; 
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            StrictStringFormatter().format(
                UPGRADE_QUERY, table_name="state_supervision_period"
            )
        )
        op.execute(
            StrictStringFormatter().format(
                UPGRADE_QUERY, table_name="state_supervision_period_history"
            )
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            StrictStringFormatter().format(
                DOWNGRADE_QUERY, table_name="state_supervision_period"
            )
        )
        op.execute(
            StrictStringFormatter().format(
                DOWNGRADE_QUERY, table_name="state_supervision_period_history"
            )
        )
