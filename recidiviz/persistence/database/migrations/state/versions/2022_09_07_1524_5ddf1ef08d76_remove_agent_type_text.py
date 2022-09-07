# pylint: skip-file
"""remove_agent_type_text

Revision ID: 5ddf1ef08d76
Revises: b53e9c54ed10
Create Date: 2022-09-07 15:24:52.404987

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5ddf1ef08d76"
down_revision = "b53e9c54ed10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE state_agent SET agent_type_raw_text = NULL WHERE state_code = 'US_ND' AND agent_type = 'JUDGE';"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_agent SET agent_type_raw_text = 'JUDGE' WHERE state_code = 'US_ND' AND agent_type = 'JUDGE';"
    )
