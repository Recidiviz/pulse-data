# pylint: skip-file
"""us_mi_sanction_admission

Revision ID: 604193f2096f
Revises: 2bfa9cac7e15
Create Date: 2023-09-21 15:28:58.049103

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "604193f2096f"
down_revision = "093471676f3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period 
        SET admission_reason = 'REVOCATION'
        WHERE 
            state_code = 'US_MI' 
            and admission_reason_raw_text = '17'
            and admission_reason = 'SANCTION_ADMISSION';

        UPDATE state_incarceration_period 
        SET specialized_purpose_for_incarceration = NULL
        WHERE 
            state_code = 'US_MI' 
            and admission_reason_raw_text = '17'
            and specialized_purpose_for_incarceration = 'TREATMENT_IN_PRISON';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period 
        SET admission_reason = 'SANCTION_ADMISSION'
        WHERE 
            state_code = 'US_MI' 
            and admission_reason_raw_text = '17'
            and admission_reason = 'REVOCATION';

        UPDATE state_incarceration_period 
        SET specialized_purpose_for_incarceration = 'TREATMENT_IN_PRISON'
        WHERE 
            state_code = 'US_MI' 
            and admission_reason_raw_text = '17'
            and specialized_purpose_for_incarceration is NULL;
        """
    )
