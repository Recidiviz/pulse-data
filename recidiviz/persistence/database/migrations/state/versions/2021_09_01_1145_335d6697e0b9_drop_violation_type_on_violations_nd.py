# pylint: skip-file
"""drop_violation_type_on_violations_nd

Revision ID: 335d6697e0b9
Revises: 43acad0ef417
Create Date: 2021-09-01 11:45:24.185528

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "335d6697e0b9"
down_revision = "43acad0ef417"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE {table_name} SET {nullify_cols_statement} WHERE state_code IN ('US_ND');"
)

COLS_TO_NULLIFY = ["violation_type", "violation_type_raw_text"]

TABLES_TO_UPDATE = [
    "state_supervision_violation",
    "state_supervision_violation_history",
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
