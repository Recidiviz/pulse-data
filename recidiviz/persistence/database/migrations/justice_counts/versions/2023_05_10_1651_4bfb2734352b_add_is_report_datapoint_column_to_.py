# pylint: skip-file
"""add is_report_datapoint column to datapoint table

Revision ID: 4bfb2734352b
Revises: 4ce15d24d1d0
Create Date: 2023-05-10 16:51:44.817085

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4bfb2734352b"
down_revision = "4ce15d24d1d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("datapoint", sa.Column("is_report_datapoint", sa.Boolean()))


def downgrade() -> None:
    op.drop_column("datapoint", "is_report_datapoint")
