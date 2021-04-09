# pylint: skip-file
"""migrate_to_sex_offense

Revision ID: 3c524f19a972
Revises: 33805fc8578f
Create Date: 2020-11-16 11:05:43.620455

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "3c524f19a972"
down_revision = "33805fc8578f"
branch_labels = None
depends_on = None


SEX_OFFENDER_QUERY = (
    "SELECT supervision_case_type_entry_id FROM"
    " {table_name}"
    " WHERE case_type = 'SEX_OFFENDER'"
)

SEX_OFFENSE_QUERY = (
    "SELECT supervision_case_type_entry_id FROM"
    " {table_name}"
    " WHERE case_type = 'SEX_OFFENSE'"
)

UPDATE_QUERY = (
    "UPDATE {table_name} SET case_type = '{new_value}'"
    " WHERE supervision_case_type_entry_id IN ({ids_query});"
)

CASE_TYPE_TABLE_NAME = "state_supervision_case_type_entry"
CASE_TYPE_HISTORY_TABLE_NAME = "state_supervision_case_type_entry_history"


def upgrade() -> None:
    connection = op.get_bind()

    new_sex_offense = "SEX_OFFENSE"

    connection.execute(
        UPDATE_QUERY.format(
            table_name=CASE_TYPE_TABLE_NAME,
            new_value=new_sex_offense,
            ids_query=SEX_OFFENDER_QUERY.format(table_name=CASE_TYPE_TABLE_NAME),
        )
    )

    connection.execute(
        UPDATE_QUERY.format(
            table_name=CASE_TYPE_HISTORY_TABLE_NAME,
            new_value=new_sex_offense,
            ids_query=SEX_OFFENDER_QUERY.format(
                table_name=CASE_TYPE_HISTORY_TABLE_NAME
            ),
        )
    )


def downgrade() -> None:
    connection = op.get_bind()

    deprecated_sex_offender = "SEX_OFFENDER"

    connection.execute(
        UPDATE_QUERY.format(
            table_name=CASE_TYPE_TABLE_NAME,
            new_value=deprecated_sex_offender,
            ids_query=SEX_OFFENSE_QUERY.format(table_name=CASE_TYPE_TABLE_NAME),
        )
    )

    connection.execute(
        UPDATE_QUERY.format(
            table_name=CASE_TYPE_HISTORY_TABLE_NAME,
            new_value=deprecated_sex_offender,
            ids_query=SEX_OFFENSE_QUERY.format(table_name=CASE_TYPE_HISTORY_TABLE_NAME),
        )
    )
