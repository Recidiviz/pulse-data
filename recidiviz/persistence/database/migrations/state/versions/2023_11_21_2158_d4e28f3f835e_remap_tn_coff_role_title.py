# pylint: skip-file
"""remap_tn_COFF_role_title

Revision ID: d4e28f3f835e
Revises: e2bdea9f99ad
Create Date: 2023-11-21 21:58:37.659055

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e28f3f835e"
down_revision = "e2bdea9f99ad"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_staff_role_period
        SET role_type = 'SUPERVISION_OFFICER'
        WHERE 
            state_code = 'US_TN' 
            and role_type_raw_text = 'COFF';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_staff_role_period
        SET role_type = 'INTERNAL_UNKNOWN'
        WHERE 
            state_code = 'US_TN' 
            and role_type_raw_text = 'COFF';
        """
    )
