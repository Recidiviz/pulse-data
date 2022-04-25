# pylint: skip-file
"""us_me_no_sanction_decision

Revision ID: c0faf8fe539f
Revises: 36e2bcf8cd3f
Create Date: 2022-04-20 15:16:14.563213

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "c0faf8fe539f"
down_revision = "36e2bcf8cd3f"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET decision = '{updated_value}'"
    " WHERE state_code = 'US_ME'"
    " and decision_raw_text = 'VIOLATION FOUND@@VIOLATION FOUND - NO SANCTION';"
)

TABLES = [
    "state_supervision_violation_response_decision_entry",
    "state_supervision_violation_response_decision_entry_history",
]


def upgrade() -> None:
    new_decision = "CONTINUANCE"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                updated_value=new_decision,
            )
        )


def downgrade() -> None:
    new_decision = "NO_SANCTION"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                updated_value=new_decision,
            )
        )
