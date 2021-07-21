# pylint: skip-file
"""pa_parole_board_hold_admissions

Revision ID: 6b4aad103cc6
Revises: fd60c913b759
Create Date: 2021-07-20 13:49:23.321847

"""
from typing import Dict, List, Tuple

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6b4aad103cc6"
down_revision = "fd60c913b759"
branch_labels = None
depends_on = None


TABLES_TO_UPDATE = ["state_incarceration_period", "state_incarceration_period_history"]

UPGRADE_UPDATE_QUERY = (
    "UPDATE {table_name} SET admission_reason = '{new_value}'"
    " WHERE state_code = 'US_PA'"
    " AND admission_reason_raw_text LIKE 'PVP%%';"
)

RAW_TEXT_ORIGINAL_VALUES_ORDERED_MAP: List[Tuple[str, str]] = [
    ("%%-TRUE", "STATUS_CHANGE"),
    ("%%-AB-%%", "NEW_ADMISSION"),
    ("%%-AC-%%", "NEW_ADMISSION"),
    ("%%-ACT-%%", "NEW_ADMISSION"),
    ("%%-APV-%%", "ADMITTED_FROM_SUPERVISION"),
    ("%%-AA-%%", "TRANSFER"),
    ("%%-AIT-%%", "TRANSFER"),
    ("%%-ASH-%%", "TRANSFER"),
    ("%%-ATT-%%", "TRANSFER"),
    ("%%-AW-%%", "TRANSFER"),
    ("%%-PLC-%%", "TRANSFER"),
    ("%%-DTT-%%", "TRANSFER"),
    ("%%-RTT-%%", "TRANSFER"),
    ("%%-STT-%%", "TRANSFER"),
    ("%%-TFM-%%", "TRANSFER"),
    ("%%-TRN-%%", "TRANSFER"),
    ("%%-TTM-%%", "TRANSFER"),
    ("%%-XPT-%%", "TRANSFER"),
    ("%%-SC-%%", "TRANSFER"),
    ("%%-AOPV-%%", "TRANSFERRED_FROM_OUT_OF_STATE"),
    ("%%-AE-%%", "RETURN_FROM_ESCAPE"),
    ("%%-AOTH-%%", "EXTERNAL_UNKNOWN"),
    ("%%-RTN-%%", "INTERNAL_UNKNOWN"),
]

DOWNGRADE_UPDATE_QUERY = """UPDATE {table_name}
                         SET admission_reason = '{new_value}'
                         WHERE state_code = 'US_PA' AND 
                         specialized_purpose_for_incarceration = 'PAROLE_BOARD_HOLD'
                         AND admission_reason_raw_text LIKE '{raw_text_value}';  
                         """


def upgrade() -> None:
    connection = op.get_bind()

    new_value = "TEMPORARY_CUSTODY"

    with op.get_context().autocommit_block():
        for table_name in TABLES_TO_UPDATE:
            connection.execute(
                UPGRADE_UPDATE_QUERY.format(
                    table_name=table_name,
                    new_value=new_value,
                )
            )


def downgrade() -> None:
    connection = op.get_bind()

    with op.get_context().autocommit_block():
        for table_name in TABLES_TO_UPDATE:
            for (
                raw_text_value,
                original_value,
            ) in RAW_TEXT_ORIGINAL_VALUES_ORDERED_MAP:
                connection.execute(
                    DOWNGRADE_UPDATE_QUERY.format(
                        table_name=table_name,
                        new_value=original_value,
                        raw_text_value=raw_text_value,
                    )
                )
