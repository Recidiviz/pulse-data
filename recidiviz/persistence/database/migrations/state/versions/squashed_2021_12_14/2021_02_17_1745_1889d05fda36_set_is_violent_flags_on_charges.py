# pylint: skip-file
"""set_is_violent_flags_on_charges

Revision ID: 1889d05fda36
Revises: cf62dcd5f566
Create Date: 2021-02-17 17:45:26.210779

"""
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "1889d05fda36"
down_revision = "cf62dcd5f566"
branch_labels = None
depends_on = None


# This list of statute codes that are marked as is_violent=True are taken from the us_nd_offense_codes reference table
US_ND_UPGRADE_BY_STATUTE_QUERY = """
UPDATE {table_name} ch
SET is_violent = ch.statute in (
'140922',
'0913',
'1601B',
'39-08-01.2(2)',
'14-09-22',
'14-09-22.1,'
'A1702',
'2201A',
'A1601'
'32012201'
'08041601'
'A2201'
'A1602'
'12121061'
'AA1702',
'12103061601',
'A1801',
'1712',
'031222',
'6210501',
'1801A',
'390804',
'17105',
'1702',
'390803',
'2502',
'17011',
'17106',
'1706',
'2102',
'1802',
'1801',
'12106102',
'06102',
'390806',
'1602',
'17103',
'1705',
'1601',
'17102',
'17104',
'1603',
'6210211',
'6210209',
'62210203',
'0802',
'1703',
'2201',
'1701',
'17071',
'1704',
'2017',
'0701',
'1803',
'1711',
'1319',
'20-23',
'20-24',
'12-21',
'2994',
'08-09',
'12.1-17-12',
'12.1-17-12(A)',
'1601(A)',
'44.1-212',
'12.1-40-01',
'47-21(5)',
'17-09',
'1601A',
'12.1-31-07',
'39-08-01.2(1)',
'380805A',
'11-19.1-07',
'06.2-02',
'12.1-16-01(1)(A)',
'12.1-22-01(2)',
'12.1-17-01',
'12.1-17-01.2',
'12.1-17-01(1)(A)'
)
WHERE state_code = 'US_ND'
AND ch.statute IS NOT NULL
"""


# This list of NCIC codes that are marked as is_violent=True are taken from the ncic.csv file
UPGRADE_BY_NCIC_CODE_QUERY = """
UPDATE {table_name} ch
SET is_violent = ch.ncic_code in (
'900',
'901',
'902',
'903',
'904',
'905',
'906',
'907',
'908',
'909',
'910',
'911',
'912',
'923',
'999',
'1000',
'1001',
'1002',
'1003',
'1004',
'1005',
'1006',
'1099',
'1100',
'1101',
'1102',
'1103',
'1104',
'1105',
'1106',
'1107',
'1108',
'1109',
'1110',
'1111',
'1112',
'1114',
'1115',
'1116',
'1117',
'1199',
'1200',
'1201',
'1202',
'1203',
'1204',
'1205',
'1206',
'1207',
'1208',
'1209',
'1210',
'1211',
'1299',
'1300',
'1301',
'1302',
'1303',
'1304',
'1305',
'1306',
'1307',
'1308',
'1309',
'1310',
'1311',
'1312',
'1313',
'1314',
'1315',
'1316',
'1317',
'1318',
'2000',
'2001',
'2002',
'2003',
'2004',
'2005',
'2006',
'2008',
'2009',
'2099',
'2201',
'2207',
'3600',
'3601',
'3602',
'3603',
'3604',
'3605',
'3606',
'3607',
'3699',
'3702',
'3703',
'3704',
'3705',
'3706',
'3707',
'3708',
'3802',
'3803',
'3804',
'3805',
'3806',
'3810',
'4801',
'5004',
'5211',
'5302',
'5303',
'5400',
'5403',
'5404',
'5499',
'7099'
)
WHERE ch.ncic_code IS NOT NULL
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            StrictStringFormatter().format(
                US_ND_UPGRADE_BY_STATUTE_QUERY, table_name="state_charge"
            )
        )
        op.execute(
            StrictStringFormatter().format(
                US_ND_UPGRADE_BY_STATUTE_QUERY, table_name="state_charge_history"
            )
        )

        op.execute(
            StrictStringFormatter().format(
                UPGRADE_BY_NCIC_CODE_QUERY, table_name="state_charge"
            )
        )
        op.execute(
            StrictStringFormatter().format(
                UPGRADE_BY_NCIC_CODE_QUERY, table_name="state_charge_history"
            )
        )


def downgrade() -> None:
    # There is no sensible downgrade query to run here because it's inherently lossy
    pass
