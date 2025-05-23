# pylint: skip-file
"""update_dashboard_user_restrictions_table

Revision ID: d0f89f3939c9
Revises: 9ec4c6ed9019
Create Date: 2021-08-11 18:43:38.961069

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d0f89f3939c9"
down_revision = "9ec4c6ed9019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "dashboard_user_restrictions",
        sa.Column(
            "routes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Page level restrictions for the user",
        ),
    )
    op.drop_column(
        "dashboard_user_restrictions", "allowed_level_1_supervision_location_ids"
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "dashboard_user_restrictions",
        sa.Column(
            "allowed_level_1_supervision_location_ids",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=False,
            comment="Deprecated column for allowed supervision location ids",
        ),
    )
    op.drop_column("dashboard_user_restrictions", "routes")
    # ### end Alembic commands ###
