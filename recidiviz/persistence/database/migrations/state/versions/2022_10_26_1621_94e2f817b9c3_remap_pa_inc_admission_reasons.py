# pylint: skip-file
"""remap_pa_inc_admission_reasons

Revision ID: 94e2f817b9c3
Revises: d449b1d3714e
Create Date: 2022-10-26 16:21:01.927632

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "94e2f817b9c3"
down_revision = "d449b1d3714e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET admission_reason = 'NEW_ADMISSION'
        WHERE admission_reason_raw_text in ('NA-FALSE-AOTH-FALSE')
        AND state_code = 'US_PA'
        """
    )
    op.execute(
        """
        UPDATE state_incarceration_period
        SET admission_reason = 'SANCTION_ADMISSION'
        WHERE admission_reason_raw_text in ('TPV-FALSE-APV-FALSE')
        AND state_code = 'US_PA'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET admission_reason = 'EXTERNAL_UNKNOWN'
        WHERE admission_reason_raw_text in ('NA-FALSE-AOTH-FALSE')
        AND state_code = 'US_PA'
        """
    )
    op.execute(
        """
        UPDATE state_incarceration_period
        SET admission_reason = 'INTERNAL_UNKNOWN'
        WHERE admission_reason_raw_text in ('TPV-FALSE-APV-FALSE')
        AND state_code = 'US_PA'
        """
    )
