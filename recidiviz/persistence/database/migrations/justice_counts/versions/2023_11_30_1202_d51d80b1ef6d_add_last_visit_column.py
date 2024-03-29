# pylint: skip-file
"""add last_visit column

Revision ID: d51d80b1ef6d
Revises: 64c0067d864e
Create Date: 2023-11-30 12:02:58.475299

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d51d80b1ef6d"
down_revision = "64c0067d864e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "agency_user_account_association",
        sa.Column("last_visit", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("agency_user_account_association", "last_visit")
    # ### end Alembic commands ###
