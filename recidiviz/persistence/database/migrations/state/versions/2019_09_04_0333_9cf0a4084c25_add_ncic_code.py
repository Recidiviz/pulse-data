"""add_ncic_code

Revision ID: 9cf0a4084c25
Revises: 31262ba39a41
Create Date: 2019-09-04 03:33:35.054732

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9cf0a4084c25"
down_revision = "31262ba39a41"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "state_charge", sa.Column("ncic_code", sa.String(length=255), nullable=True)
    )
    op.add_column(
        "state_charge_history",
        sa.Column("ncic_code", sa.String(length=255), nullable=True),
    )


def downgrade():
    op.drop_column("state_charge_history", "ncic_code")
    op.drop_column("state_charge", "ncic_code")
