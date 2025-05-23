# pylint: skip-file
"""add sub_dimension_name column to Datapoint table

Revision ID: ec12989d54a1
Revises: 1d55ae322ca7
Create Date: 2025-04-14 14:06:55.695517

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ec12989d54a1"
down_revision = "1d55ae322ca7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "datapoint", sa.Column("sub_dimension_name", sa.String(), nullable=True)
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("datapoint", "sub_dimension_name")
    # ### end Alembic commands ###
