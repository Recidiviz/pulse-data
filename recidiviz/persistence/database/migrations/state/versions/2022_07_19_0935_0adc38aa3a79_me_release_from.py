# pylint: skip-file
"""me_release_from

Revision ID: 0adc38aa3a79
Revises: f32cbf0f6cdb
Create Date: 2022-07-19 09:35:06.204728

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0adc38aa3a79"
down_revision = "f32cbf0f6cdb"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'RELEASE_FROM_INCARCERATION' WHERE state_code = 
'US_ME' AND (SPLIT_PART(admission_reason_raw_text, '@@', 5) IN ('2', '7') OR
SPLIT_PART(admission_reason_raw_text, '@@', 2) IN ('SCCP', 'PAROLE'))
AND (SPLIT_PART(admission_reason_raw_text, '@@', '4') = 'NONE' OR SPLIT_PART(admission_reason_raw_text, '@@', '4') IN ('2', '7', '8', '13', 'COUNTY JAIL'))
AND SPLIT_PART(admission_reason_raw_text, '@@', 1) 
NOT IN ('SCCP', 'PAROLE', 'PROBATION', 'PENDING VIOLATION', 'PENDING VIOLATION - INCARCERATED', 'WARRANT ABSCONDED', 'PARTIAL REVOCATION - COUNTY JAIL', 'REFERRAL', 'ACTIVE');
"""

DOWNGRADE_QUERY = """
UPDATE state_supervision_period SET admission_reason = 'CONDITIONAL_RELEASE' WHERE state_code = 
'US_ME' AND (SPLIT_PART(admission_reason_raw_text, '@@', 5) IN ('2', '7') OR
SPLIT_PART(admission_reason_raw_text, '@@', 2) IN ('SCCP', 'PAROLE'))
AND (SPLIT_PART(admission_reason_raw_text, '@@', '4') = 'NONE' OR SPLIT_PART(admission_reason_raw_text, '@@', '4') IN ('2', '7', '8', '13', 'COUNTY JAIL'))
AND SPLIT_PART(admission_reason_raw_text, '@@', 1) 
NOT IN ('SCCP', 'PAROLE', 'PROBATION', 'PENDING VIOLATION', 'PENDING VIOLATION - INCARCERATED', 'WARRANT ABSCONDED', 'PARTIAL REVOCATION - COUNTY JAIL', 'REFERRAL', 'ACTIVE');
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
