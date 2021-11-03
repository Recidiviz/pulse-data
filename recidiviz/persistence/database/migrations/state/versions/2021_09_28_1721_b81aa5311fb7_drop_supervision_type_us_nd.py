# pylint: skip-file
"""drop_supervision_type_us_nd

Revision ID: b81aa5311fb7
Revises: fa392d2c4eb1
Create Date: 2021-09-28 17:21:07.793446

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "b81aa5311fb7"
down_revision = "fa392d2c4eb1"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE {table_name} SET {nullify_cols_statement} WHERE state_code IN ('US_ND');"
)

COLS_TO_NULLIFY = ["supervision_type", "supervision_type_raw_text"]

TABLES_TO_UPDATE = ["state_supervision_period", "state_supervision_period_history"]


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
