# pylint: skip-file
"""clear_nd_raw_texts

Revision ID: 68da927c31a4
Revises: ec185c0a8d76
Create Date: 2021-10-07 19:00:19.523185

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "68da927c31a4"
down_revision = "ec185c0a8d76"
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
    "US_ND": [
        ("state_supervision_sentence", "status", ["COMPLETED"]),
        ("state_agent", "agent_type", ["JUDGE", "SUPERVISION_OFFICER"]),
        (
            "state_supervision_violation_type_entry",
            "violation_type",
            ["LAW", "ABSCONDED", "TECHNICAL"],
        ),
        (
            "state_supervision_violation_response",
            "response_type",
            ["PERMANENT_DECISION"],
        ),
    ]
}


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for state_code, update_info_list in MIGRATION_INFO.items():
            for update_info in update_info_list:
                base_table, enum_col, raw_text_values_to_clear = update_info
                for table in [base_table, f"{base_table}_history"]:
                    op.execute(
                        UPDATE_QUERY.format(
                            table_name=table,
                            state_code=state_code,
                            enum_col=enum_col,
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
                        DOWNGRADE_QUERY.format(
                            table_name=table,
                            state_code=state_code,
                            enum_col=enum_col,
                            raw_text_col=f"{enum_col}_raw_text",
                            raw_text_values_str="', '".join(raw_text_values),
                        )
                    )
