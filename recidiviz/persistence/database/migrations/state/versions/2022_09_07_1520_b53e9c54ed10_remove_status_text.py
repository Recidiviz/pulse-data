# pylint: skip-file
"""remove_status_text

Revision ID: b53e9c54ed10
Revises: bcce4a5fc626
Create Date: 2022-09-07 15:20:25.505227

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b53e9c54ed10"
down_revision = "bcce4a5fc626"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE state_charge SET status_raw_text = NULL WHERE state_code = 'US_ND' AND status = 'CONVICTED';"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_charge SET status_raw_text = 'CONVICTED' WHERE state_code = 'US_ND' AND status = 'CONVICTED';"
    )
