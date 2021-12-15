# pylint: skip-file
"""clear_pa_ssvr_raw_text

Revision ID: 9bb34b630955
Revises: 68da927c31a4
Create Date: 2021-10-07 20:45:04.027092

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "9bb34b630955"
down_revision = "68da927c31a4"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET {raw_text_col} = NULL"
    " WHERE state_code = '{state_code}' AND {raw_text_col} IN ('{raw_text_values_str}');"
)

DOWNGRADE_QUERY = (
    "UPDATE {table_name} SET {raw_text_col} = {enum_col}"
    " WHERE state_code = '{state_code}' AND {enum_col} IN ('{raw_text_values_str}');"
)


MIGRATION_INFO = {
    "US_PA": [
        ("state_supervision_violation_response", "response_type", ["VIOLATION_REPORT"]),
    ]
}


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for state_code, update_info_list in MIGRATION_INFO.items():
            for update_info in update_info_list:
                base_table, enum_col, raw_text_values_to_clear = update_info
                for table in [base_table, f"{base_table}_history"]:
                    op.execute(
                        StrictStringFormatter().format(
                            UPDATE_QUERY,
                            table_name=table,
                            state_code=state_code,
                            raw_text_col=f"{enum_col}_raw_text",
                            raw_text_values_str="', '".join(raw_text_values_to_clear),
                        )
                    )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for state_code, update_info_list in MIGRATION_INFO.items():
            for update_info in update_info_list:
                base_table, enum_col, raw_text_values = update_info
                for table in [base_table, f"{base_table}_history"]:
                    op.execute(
                        StrictStringFormatter().format(
                            DOWNGRADE_QUERY,
                            table_name=table,
                            state_code=state_code,
                            enum_col=enum_col,
                            raw_text_col=f"{enum_col}_raw_text",
                            raw_text_values_str="', '".join(raw_text_values),
                        )
                    )
