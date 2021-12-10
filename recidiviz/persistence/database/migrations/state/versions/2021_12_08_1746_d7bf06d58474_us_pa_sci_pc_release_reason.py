# pylint: skip-file
"""us_pa_sci_pc_release_reason

Revision ID: d7bf06d58474
Revises: b6f1fc45c89c
Create Date: 2021-12-08 17:46:58.466869

"""
from typing import List

import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "d7bf06d58474"
down_revision = "b6f1fc45c89c"
branch_labels = None
depends_on = None


PERIOD_TABLES_TO_UPDATE = [
    "state_incarceration_period",
    "state_incarceration_period_history",
]

PERIOD_PURPOSE_FOR_INCARCERATION_UPDATE_QUERY = (
    "UPDATE {table_name} SET specialized_purpose_for_incarceration = '{new_value}'"
    " WHERE state_code = 'US_PA'"
    " AND specialized_purpose_for_incarceration_raw_text IN ({raw_text_values});"
)

PERIOD_RELEASE_REASON_UPDATE_QUERY = (
    "UPDATE {table_name} SET release_reason = '{new_value}'"
    " WHERE state_code = 'US_PA'"
    " AND release_reason_raw_text = '{raw_text_value}';"
)

SENTENCE_TABLES_TO_UPDATE = [
    "state_incarceration_sentence",
    "state_incarceration_sentence_history",
]

SENTENCE_UPDATE_QUERY = (
    "UPDATE {table_name} SET status = '{new_value}'"
    " WHERE state_code = 'US_PA'"
    " AND status_raw_text = '{raw_text_value}';"
)


def values_to_list_string(values: List[str]) -> str:
    return ",".join([f"'{value}'" for value in values])


def upgrade() -> None:
    connection = op.get_bind()

    with op.get_context().autocommit_block():
        for table_name in PERIOD_TABLES_TO_UPDATE:
            connection.execute(
                StrictStringFormatter().format(
                    PERIOD_PURPOSE_FOR_INCARCERATION_UPDATE_QUERY,
                    table_name=table_name,
                    new_value="TREATMENT_IN_PRISON",
                    raw_text_values=values_to_list_string(
                        [
                            "CCIS-62",
                            "CCIS-63",
                            "CCIS-64",
                            "CCIS-65",
                            "CCIS-66",
                        ]
                    ),
                )
            )
        for table_name in PERIOD_TABLES_TO_UPDATE:
            connection.execute(
                StrictStringFormatter().format(
                    PERIOD_RELEASE_REASON_UPDATE_QUERY,
                    table_name=table_name,
                    new_value="SENTENCE_SERVED",
                    raw_text_value="PC-NA-D-NONE",
                )
            )
        for table_name in SENTENCE_TABLES_TO_UPDATE:
            connection.execute(
                StrictStringFormatter().format(
                    SENTENCE_UPDATE_QUERY,
                    table_name=table_name,
                    new_value="COMPLETED",
                    raw_text_value="PC",
                )
            )


def downgrade() -> None:
    connection = op.get_bind()

    with op.get_context().autocommit_block():
        for table_name in PERIOD_TABLES_TO_UPDATE:
            connection.execute(
                StrictStringFormatter().format(
                    PERIOD_PURPOSE_FOR_INCARCERATION_UPDATE_QUERY,
                    table_name=table_name,
                    new_value="INTERNAL_UNKNOWN",
                    raw_text_values=values_to_list_string(
                        [
                            "CCIS-62",
                            "CCIS-63",
                            "CCIS-64",
                            "CCIS-65",
                            "CCIS-66",
                        ]
                    ),
                )
            )
        for table_name in PERIOD_TABLES_TO_UPDATE:
            connection.execute(
                StrictStringFormatter().format(
                    PERIOD_RELEASE_REASON_UPDATE_QUERY,
                    table_name=table_name,
                    new_value="INTERNAL_UNKNOWN",
                    raw_text_value="PC-NA-D-NONE",
                )
            )
        # Note: There is some information loss here as the prior behavior for sentences
        # was to leave the status untouched. We don't know what the previous status was
        # so we can't reset it back to that. Instead we do nothing.
