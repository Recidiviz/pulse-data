# pylint: skip-file
"""drop_revocation_type_entry_us_mo

Revision ID: d38a342c35c5
Revises: efa2534060e3
Create Date: 2021-08-30 20:49:32.130942

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "d38a342c35c5"
down_revision = "d1d60c10e782"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE {table_name} SET {nullify_cols_statement} WHERE state_code IN ('US_MO');"
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
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table,
                    nullify_cols_statement=nullify_cols_statement,
                )
            )


def downgrade() -> None:
    # Field deprecation cannot be undone
    pass
