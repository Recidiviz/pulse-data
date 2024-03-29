# pylint: skip-file
"""add last_updated to datapoint table

Revision ID: 6df9b0363677
Revises: aa08b9b6f1e1
Create Date: 2023-09-21 10:19:16.334928

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6df9b0363677"
down_revision = "aa08b9b6f1e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("datapoint", sa.Column("last_updated", sa.DateTime(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("datapoint", "last_updated")
    # ### end Alembic commands ###
