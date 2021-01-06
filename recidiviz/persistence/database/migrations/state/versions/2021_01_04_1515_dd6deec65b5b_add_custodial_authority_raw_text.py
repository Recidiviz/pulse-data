# pylint: skip-file
"""add_custodial_authority_raw_text

Revision ID: dd6deec65b5b
Revises: be48397d71ec
Create Date: 2020-12-10 15:15:06.564944

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dd6deec65b5b'
down_revision = 'be48397d71ec'
branch_labels = None
depends_on = None

UPDATE_QUERY_US_ID = """UPDATE {table_id}
                        SET custodial_authority_raw_text = 
                        CASE WHEN custodial_authority LIKE 'FEDERAL%%' OR custodial_authority LIKE 'U.S.%%' THEN 'FED'
                             WHEN custodial_authority IN ('DEPORTED', 'OTHER_COUNTRY') THEN 'DEPORTED'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT 0%%' THEN 'D0'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 1%%' THEN 'D1'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 2%%' THEN 'D2'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 3%%' THEN 'D3'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 4%%' THEN 'D4'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 5%%' THEN 'D5'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 6%%' THEN 'D6'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 7%%' THEN 'D7'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'DISTRICT OFFICE 8%%' THEN 'D8'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'PAROLE COMMISSION OFFICE%%' THEN 'PC'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'INTERSTATE PROBATION%%' THEN 'IS'
                             WHEN SPLIT_PART(supervision_site, '|', 1) LIKE 'FCS CENTRAL OFFICE%%' THEN 'FC'
                             WHEN admission_reason = 'ABSCONSION' THEN 'FI'
                        END
    WHERE state_code = 'US_ID' AND (custodial_authority IS NOT NULL OR supervision_site IS NOT NULL);"""

UPDATE_QUERY_US_PA = """UPDATE {table_id}
                        SET custodial_authority_raw_text = supervision_period_supervision_type_raw_text
                        WHERE state_code = 'US_PA';"""

# Note: custodial_authority column is currently empty for all rows in state_incarceration_period
TABLES_TO_UPDATE = [
    'state_incarceration_period_history',
    'state_incarceration_period',
    'state_supervision_period_history',
    'state_supervision_period',
]


def upgrade():
    connection = op.get_bind()

    for table_id in TABLES_TO_UPDATE:
        # Add custodial_authority_raw_text column
        op.add_column(table_id,
                      sa.Column('custodial_authority_raw_text', sa.String(length=255), nullable=True))

        # Set custodial_authority_raw_text values for the supervision tables
        if 'supervision' in table_id:
            # Set custodial_authority_raw_text value for US_ID
            connection.execute(UPDATE_QUERY_US_ID.format(table_id=table_id))

            # Set custodial_authority_raw_text value for US_PA
            connection.execute(UPDATE_QUERY_US_PA.format(table_id=table_id))


def downgrade():
    for table_id in TABLES_TO_UPDATE:
        op.drop_column(table_id, 'custodial_authority_raw_text')
