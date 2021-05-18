# pylint: skip-file
"""update_contacts

Revision ID: ca75d6f5dfcb
Revises: 9a623575162e
Create Date: 2021-05-18 08:43:35.889795

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ca75d6f5dfcb"
down_revision = "9a623575162e"
branch_labels = None
depends_on = None

QUERY = """
    UPDATE {table}
    SET status = '{status}'
    WHERE status_raw_text = '{status_raw_text}'
    AND state_code = 'US_PA'
"""

TABLES = ["state_supervision_contact", "state_supervision_contact_history"]


def upgrade() -> None:
    status_to_raw_text = {"ATTEMPTED": "YES", "COMPLETED": "NO"}
    with op.get_context().autocommit_block():
        for table in TABLES:
            for status, raw_text in status_to_raw_text.items():
                op.execute(
                    QUERY.format(table=table, status=status, status_raw_text=raw_text)
                )


def downgrade() -> None:
    status_to_raw_text = {"ATTEMPTED": "NO", "COMPLETED": "YES"}
    with op.get_context().autocommit_block():
        for table in TABLES:
            for status, raw_text in status_to_raw_text.items():
                op.execute(
                    QUERY.format(table=table, status=status, status_raw_text=raw_text)
                )
