# pylint: skip-file
"""pa_internal_unknown_admissions

Revision ID: fd60c913b759
Revises: b86b7c2bacb1
Create Date: 2021-07-20 13:10:48.348816

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "fd60c913b759"
down_revision = "b86b7c2bacb1"
branch_labels = None
depends_on = None

RAW_TEXT_VALUES_TO_MIGRATE = [
    "COV-FALSE-APV-FALSE",
    "COV-FALSE-APV-NONE",
    "CPV-FALSE-APV-FALSE",
    "CPV-NONE-APV-FALSE",
    "CPV-NONE-APV-NONE",
    "NONE-FALSE-APD-FALSE",
    "NONE-FALSE-APV-FALSE",
    "NONE-FALSE-APV-NONE",
    "TCV-FALSE-APV-FALSE",
    "TPV-FALSE-APV-FALSE",
    "TPV-NONE-APV-FALSE",
    "TPV-NONE-APV-NONE",
]

RAW_TEXT_VALUES_AS_LIST_STRING = ",".join(
    [f"'{value}'" for value in RAW_TEXT_VALUES_TO_MIGRATE]
)

TABLES_TO_UPDATE = ["state_incarceration_period", "state_incarceration_period_history"]

UPDATE_QUERY = (
    "UPDATE {table_name} SET admission_reason = '{new_value}'"
    " WHERE state_code = 'US_PA'"
    " AND admission_reason_raw_text IN ({raw_text_values});"
)


def upgrade() -> None:
    connection = op.get_bind()

    new_value = "INTERNAL_UNKNOWN"

    with op.get_context().autocommit_block():
        for table_name in TABLES_TO_UPDATE:
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    new_value=new_value,
                    raw_text_values=RAW_TEXT_VALUES_AS_LIST_STRING,
                )
            )


def downgrade() -> None:
    connection = op.get_bind()

    new_value = "ADMITTED_FROM_SUPERVISION"

    with op.get_context().autocommit_block():
        for table_name in TABLES_TO_UPDATE:
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    new_value=new_value,
                    raw_text_values=RAW_TEXT_VALUES_AS_LIST_STRING,
                )
            )
