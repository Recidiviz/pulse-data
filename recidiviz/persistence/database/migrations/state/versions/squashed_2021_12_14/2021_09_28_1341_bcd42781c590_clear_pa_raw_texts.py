# pylint: skip-file
"""clear_pa_raw_texts

Revision ID: bcd42781c590
Revises: c5e0b7ec2b9a
Create Date: 2021-09-28 13:41:32.099729

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "bcd42781c590"
down_revision = "c5e0b7ec2b9a"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET {raw_text_col} = NULL"
    " WHERE state_code = '{state_code}' AND {enum_col} IN ('{raw_text_values_str}');"
)

DOWNGRADE_QUERY = (
    "UPDATE {table_name} SET {raw_text_col} = {enum_col}"
    " WHERE state_code = '{state_code}' AND {enum_col} IN ('{raw_text_values_str}');"
)


MIGRATION_INFO = {
    "US_PA": [
        ("state_person_alias", "alias_type", ["GIVEN_NAME"]),
        ("state_incarceration_incident", "incident_type", ["REPORT", "CONTRABAND"]),
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
                        StrictStringFormatter().format(
                            DOWNGRADE_QUERY,
                            table_name=table,
                            state_code=state_code,
                            enum_col=enum_col,
                            raw_text_col=f"{enum_col}_raw_text",
                            raw_text_values_str="', '".join(raw_text_values),
                        )
                    )
