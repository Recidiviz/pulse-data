# pylint: skip-file
"""delete_revocation_type_decisions_responses_nd

Revision ID: ff4abfa45a61
Revises: 6178a06afa16
Create Date: 2021-08-30 18:35:25.693168

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "ff4abfa45a61"
down_revision = "ff21279b54db"
branch_labels = None
depends_on = None

UPDATE_QUERY = "UPDATE {table_name} SET {col} = NULL WHERE state_code IN ('US_ND');"

COLS_TO_NULLIFY = [
    "revocation_type",
    "revocation_type_raw_text",
    "decision",
    "decision_raw_text",
]

TABLES_TO_UPDATE = [
    "state_supervision_violation_response",
    "state_supervision_violation_response_history",
]


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            for col in COLS_TO_NULLIFY:
                op.execute(
                    StrictStringFormatter().format(
                        UPDATE_QUERY, table_name=table, col=col
                    )
                )


def downgrade() -> None:
    # Field deprecation cannot be undone
    pass
