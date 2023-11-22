# pylint: skip-file
"""remap_tn_WKRET_releae_reason

Revision ID: d9a2156b4fe9
Revises: d4e28f3f835e
Create Date: 2023-11-22 11:10:21.762227

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d9a2156b4fe9"
down_revision = "d4e28f3f835e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET release_reason = 'RELEASE_FROM_WEEKEND_CONFINEMENT'
        WHERE
            state_code = 'US_TN'
            and release_reason_raw_text in ('FAPR-WKRET', 'FACC-WKRET');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_incarceration_period
        SET release_reason = 'RELEASED_FROM_TEMPORARY_CUSTODY'
        WHERE
            state_code = 'US_TN'
            and release_reason_raw_text in ('FAPR-WKRET', 'FACC-WKRET');
        """
    )
