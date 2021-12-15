# pylint: skip-file
"""us-nd-sp-supervision-type

Revision ID: fa392d2c4eb1
Revises: 581ca2c69255
Create Date: 2021-09-28 16:46:15.910611

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "fa392d2c4eb1"
down_revision = "581ca2c69255"
branch_labels = None
depends_on = None


NEW_ENUM_MAPPINGS = {
    "'COMMUNITY PLACEMENT PGRM'": "COMMUNITY_CONFINEMENT",
    "'CCC'": "INTERNAL_UNKNOWN",
    "'PRE-TRIAL'": "INVESTIGATION",
    "'IC PAROLE', 'PAROLE', 'SSOP'": "PAROLE",
    "'DEFERRED', 'IC PROBATION', 'SUSPENDED'": "PROBATION",
}

SELECT_IDS_QUERY = (
    "SELECT supervision_period_id FROM {table_name}"
    " WHERE state_code = 'US_ND'"
    " AND {filter_clause}"
)

UPDATE_QUERY = (
    "UPDATE {table_name} SET {column_name} = {new_value}"
    " WHERE supervision_period_id IN ({ids_query});"
)

TABLES_TO_UPDATE = ["state_supervision_period", "state_supervision_period_history"]


def upgrade() -> None:
    connection = op.get_bind()

    with op.get_context().autocommit_block():
        for table_name in TABLES_TO_UPDATE:
            # Put supervision_type_raw_text values into the
            # supervision_period_supervision_type_raw_text column
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    column_name="supervision_period_supervision_type_raw_text",
                    new_value="supervision_type_raw_text",
                    ids_query=StrictStringFormatter().format(
                        SELECT_IDS_QUERY,
                        table_name=table_name,
                        filter_clause="supervision_type_raw_text IS NOT NULL",
                    ),
                )
            )

            # Set the supervision_period_supervision_type column based on the defined
            # mappings
            for raw_values, new_value in NEW_ENUM_MAPPINGS.items():
                connection.execute(
                    StrictStringFormatter().format(
                        UPDATE_QUERY,
                        table_name=table_name,
                        column_name="supervision_period_supervision_type",
                        new_value=f"'{new_value}'",
                        ids_query=StrictStringFormatter().format(
                            SELECT_IDS_QUERY,
                            table_name=table_name,
                            filter_clause=f"supervision_type_raw_text IN ({raw_values})",
                        ),
                    )
                )


DOWNGRADE_UPDATE_QUERY = (
    "UPDATE {table_name} SET {nullify_cols_statement} WHERE state_code IN ('US_ND');"
)

COLS_TO_NULLIFY = [
    "supervision_period_supervision_type",
    "supervision_period_supervision_type_raw_text",
]


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            nullify_cols_statement = ", ".join(
                [f"{col} = NULL" for col in COLS_TO_NULLIFY]
            )
            op.execute(
                StrictStringFormatter().format(
                    DOWNGRADE_UPDATE_QUERY,
                    table_name=table,
                    nullify_cols_statement=nullify_cols_statement,
                )
            )
