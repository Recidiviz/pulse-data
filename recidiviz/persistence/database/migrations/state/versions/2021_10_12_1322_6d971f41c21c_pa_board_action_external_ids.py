# pylint: skip-file
"""pa_board_action_external_ids

Revision ID: 6d971f41c21c
Revises: 564eb3671e93
Create Date: 2021-10-12 13:22:01.588010

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6d971f41c21c"
down_revision = "564eb3671e93"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
UPDATE {table_name}
SET external_id = CONCAT('BOARD-', external_id)
WHERE state_code = 'US_PA'
  AND response_type = 'PERMANENT_DECISION' 
  AND deciding_body_type = 'PAROLE_BOARD';
"""

DOWNGRADE_QUERY = """
UPDATE {table_name}
SET external_id = SUBSTRING(external_id, LENGTH('BOARD-') + 1)
WHERE state_code = 'US_PA'
  AND response_type = 'PERMANENT_DECISION' 
  AND deciding_body_type = 'PAROLE_BOARD';
"""

TABLES_TO_UPDATE = [
    "state_supervision_violation_response",
    "state_supervision_violation_response_history",
]


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            op.execute(UPGRADE_QUERY.format(table_name=table))


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            op.execute(DOWNGRADE_QUERY.format(table_name=table))
