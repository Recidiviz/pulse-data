# pylint: skip-file
"""double_effective_date

Revision ID: e18db32597e1
Revises: 0c8cd5855782
Create Date: 2023-02-23 14:56:01.428646

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e18db32597e1"
down_revision = "0c8cd5855782"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE state_incarceration_sentence SET effective_date = start_date;")
    op.execute("UPDATE state_supervision_sentence SET effective_date = start_date;")


def downgrade() -> None:
    op.execute("UPDATE state_incarceration_sentence SET effective_date = NULL;")
    op.execute("UPDATE state_supervision_sentence SET effective_date = NULL;")
