# pylint: skip-file
"""us_mi_in_custody_admin

Revision ID: 10917f70569e
Revises: a5c9ffb7e9cc
Create Date: 2024-01-05 11:13:32.656533

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "10917f70569e"
down_revision = "a5c9ffb7e9cc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'IN_CUSTODY'
        WHERE
            state_code = 'US_MI'
            AND supervision_level_raw_text in (
                'MINIMUM ADMINISTRATIVE_PAROLE #2 ISSUED',
                'MINIMUM ADMINISTRATIVE_PAROLE IN JAIL',
                'MINIMUM ADMINISTRATIVE_PAROLE MPVU',
                'MINIMUM ADMINISTRATIVE_PAROLE PENDING REVOCATION HEARING',
                'MINIMUM ADMINISTRATIVE_PROBATION IN JAIL',
                'MINIMUM ADMINISTRATIVE_PROBATION IN JAIL##IMPUTED'
            )
        ;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'UNSUPERVISED'
        WHERE
            state_code = 'US_MI'
            AND supervision_level_raw_text in (
                'MINIMUM ADMINISTRATIVE_PAROLE #2 ISSUED',
                'MINIMUM ADMINISTRATIVE_PAROLE IN JAIL',
                'MINIMUM ADMINISTRATIVE_PAROLE MPVU',
                'MINIMUM ADMINISTRATIVE_PAROLE PENDING REVOCATION HEARING',
                'MINIMUM ADMINISTRATIVE_PROBATION IN JAIL',
                'MINIMUM ADMINISTRATIVE_PROBATION IN JAIL##IMPUTED'
            )
        ;
        """
    )
