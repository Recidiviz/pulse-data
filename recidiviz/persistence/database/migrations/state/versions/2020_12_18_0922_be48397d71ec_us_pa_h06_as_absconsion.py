# pylint: skip-file
"""us_pa_H06_as_absconsion

Revision ID: be48397d71ec
Revises: a54c66450b78
Create Date: 2020-12-18 09:22:11.699720

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "be48397d71ec"
down_revision = "a54c66450b78"
branch_labels = None
depends_on = None


UPDATE_TO_NEW_VALUE_QUERY = (
    "UPDATE {table_id}"
    " SET violation_type = '{new_violation_type_enum_value}'"
    " WHERE supervision_violation_type_entry_id IN ("
    "   SELECT supervision_violation_type_entry_id FROM"
    "   state_supervision_violation_type_entry"
    "   WHERE state_code = 'US_PA' AND violation_type_raw_text = 'H06'"
    ");"
)


def upgrade() -> None:
    connection = op.get_bind()

    new_enum_value = "ABSCONDED"
    connection.execute(
        UPDATE_TO_NEW_VALUE_QUERY.format(
            table_id="state_supervision_violation_type_entry",
            new_violation_type_enum_value=new_enum_value,
        )
    )
    connection.execute(
        UPDATE_TO_NEW_VALUE_QUERY.format(
            table_id="state_supervision_violation_type_entry_history",
            new_violation_type_enum_value=new_enum_value,
        )
    )


def downgrade() -> None:
    connection = op.get_bind()

    new_enum_value = "TECHNICAL"

    connection.execute(
        UPDATE_TO_NEW_VALUE_QUERY.format(
            table_id="state_supervision_violation_type_entry",
            new_violation_type_enum_value=new_enum_value,
        )
    )
    connection.execute(
        UPDATE_TO_NEW_VALUE_QUERY.format(
            table_id="state_supervision_violation_type_entry_history",
            new_violation_type_enum_value=new_enum_value,
        )
    )
