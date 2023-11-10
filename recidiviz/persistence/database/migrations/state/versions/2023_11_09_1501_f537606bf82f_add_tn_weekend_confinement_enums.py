# pylint: skip-file
"""add_tn_weekend_confinement_enums

Revision ID: f537606bf82f
Revises: 24b7a376a14b
Create Date: 2023-11-09 15:01:39.026258

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f537606bf82f"
down_revision = "24b7a376a14b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period 
        SET admission_reason = 'WEEKEND_CONFINEMENT'
        WHERE 
            state_code = 'US_TN' 
            and admission_reason_raw_text in ('PRFA-WKEND', 'CCFA-WKEND');
            
        UPDATE state_incarceration_period 
        SET release_reason = 'RELEASE_FROM_WEEKEND_CONFINEMENT'
        WHERE 
            state_code = 'US_TN' 
            and release_reason_raw_text in ('PRFA-WKEND', 'CCFA-WKEND');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period 
        SET admission_reason = 'TEMPORARY_CUSTODY'
        WHERE 
            state_code = 'US_TN' 
            and admission_reason_raw_text in ('PRFA-WKEND', 'CCFA-WKEND');

        UPDATE state_incarceration_period 
        SET release_reason = 'INTERNAL_UNKNOWN'
        WHERE 
            state_code = 'US_TN' 
            and release_reason_raw_text in ('PRFA-WKEND', 'CCFA-WKEND');
        """
    )
