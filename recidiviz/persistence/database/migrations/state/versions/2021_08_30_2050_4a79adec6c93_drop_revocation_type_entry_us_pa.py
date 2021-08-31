# pylint: skip-file
"""drop_revocation_type_entry_us_pa

Revision ID: 4a79adec6c93
Revises: 0f6b32fcb1a1
Create Date: 2021-08-26 16:02:03.241572

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4a79adec6c93"
down_revision = "d38a342c35c5"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE {table_name} SET {nullify_cols_statement} WHERE state_code IN ('US_PA');"
)

COLS_TO_NULLIFY = ["revocation_type", "revocation_type_raw_text"]

TABLES_TO_UPDATE = [
    "state_supervision_violation_response_decision_entry",
    "state_supervision_violation_response_decision_entry_history",
]


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            nullify_cols_statement = ", ".join(
                [f"{col} = NULL" for col in COLS_TO_NULLIFY]
            )
            op.execute(
                UPDATE_QUERY.format(
                    table_name=table, nullify_cols_statement=nullify_cols_statement
                )
            )


def downgrade() -> None:
    # Field deprecation cannot be undone
    pass
