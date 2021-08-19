# pylint: skip-file
"""delete_us_mo_permanent_decision_ssvrs

Revision ID: 190df7ac651c
Revises: 787c13afabf2
Create Date: 2021-08-17 17:47:39.086443

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "190df7ac651c"
down_revision = "787c13afabf2"
branch_labels = None
depends_on = None


DELETE_QUERY = (
    "DELETE FROM {table_name}"
    " WHERE supervision_violation_response_id IN ("
    " SELECT supervision_violation_response_id"
    " FROM state_supervision_violation_response"
    " WHERE state_code IN ('US_MO') AND response_type = 'PERMANENT_DECISION');"
)

TABLES_TO_UPDATE = [
    # Must delete from history table first to avoid violating foreign key constraint
    "state_supervision_violation_response_history",
    "state_supervision_violation_response",
]


def upgrade() -> None:
    for table in TABLES_TO_UPDATE:
        op.execute(DELETE_QUERY.format(table_name=table))


def downgrade() -> None:
    # Row deletion cannot be undone
    pass
