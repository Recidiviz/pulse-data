# pylint: skip-file
"""rename_supervision_period_supervision_type

Revision ID: d759f439bae3
Revises: b4a5dd037630
Create Date: 2021-09-29 12:52:53.638824

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "d759f439bae3"
down_revision = "b4a5dd037630"
branch_labels = None
depends_on = None

SELECT_IDS_QUERY = (
    "SELECT supervision_period_id FROM {table_name} WHERE {filter_clause}"
)

UPDATE_QUERY = (
    "UPDATE {table_name} SET {column_name} = {new_value}"
    " WHERE supervision_period_id IN ({ids_query});"
)

TABLES_TO_UPDATE = ["state_supervision_period", "state_supervision_period_history"]


def upgrade() -> None:
    connection = op.get_bind()

    with op.get_context().autocommit_block():
        # Add new supervision_type columns
        op.add_column(
            "state_supervision_period",
            sa.Column(
                "supervision_type",
                sa.Enum(
                    "EXTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INFORMAL_PROBATION",
                    "INVESTIGATION",
                    "PAROLE",
                    "PROBATION",
                    "DUAL",
                    "COMMUNITY_CONFINEMENT",
                    name="state_supervision_period_supervision_type",
                ),
                nullable=True,
                comment="The type of supervision the person is serving during this time period.",
            ),
        )
        op.add_column(
            "state_supervision_period",
            sa.Column(
                "supervision_type_raw_text",
                sa.String(length=255),
                nullable=True,
                comment="The raw text value of the supervision period supervision type.",
            ),
        )
        op.add_column(
            "state_supervision_period_history",
            sa.Column(
                "supervision_type",
                sa.Enum(
                    "EXTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INFORMAL_PROBATION",
                    "INVESTIGATION",
                    "PAROLE",
                    "PROBATION",
                    "DUAL",
                    "COMMUNITY_CONFINEMENT",
                    name="state_supervision_period_supervision_type",
                ),
                nullable=True,
                comment="The type of supervision the person is serving during this time period.",
            ),
        )
        op.add_column(
            "state_supervision_period_history",
            sa.Column(
                "supervision_type_raw_text",
                sa.String(length=255),
                nullable=True,
                comment="The raw text value of the supervision period supervision type.",
            ),
        )

        # Migrate values from supervision_period_supervision_type to supervision_type
        for table_name in TABLES_TO_UPDATE:
            # Put supervision_period_supervision_type_raw_text values into the
            # supervision_type_raw_text column
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    column_name="supervision_type_raw_text",
                    new_value="supervision_period_supervision_type_raw_text",
                    ids_query=StrictStringFormatter().format(
                        SELECT_IDS_QUERY,
                        table_name=table_name,
                        filter_clause="supervision_period_supervision_type_raw_text IS NOT NULL",
                    ),
                )
            )

            # Put supervision_period_supervision_type values into the
            # supervision_type column
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    column_name="supervision_type",
                    new_value="supervision_period_supervision_type",
                    ids_query=StrictStringFormatter().format(
                        SELECT_IDS_QUERY,
                        table_name=table_name,
                        filter_clause="supervision_period_supervision_type IS NOT NULL",
                    ),
                )
            )

        # Drop supervision_period_supervision_type columns
        op.drop_column(
            "state_supervision_period", "supervision_period_supervision_type"
        )
        op.drop_column(
            "state_supervision_period", "supervision_period_supervision_type_raw_text"
        )
        op.drop_column(
            "state_supervision_period_history", "supervision_period_supervision_type"
        )
        op.drop_column(
            "state_supervision_period_history",
            "supervision_period_supervision_type_raw_text",
        )


def downgrade() -> None:
    connection = op.get_bind()

    with op.get_context().autocommit_block():
        # Add supervision_period_supervision_type columns
        op.add_column(
            "state_supervision_period_history",
            sa.Column(
                "supervision_period_supervision_type_raw_text",
                sa.VARCHAR(length=255),
                autoincrement=False,
                nullable=True,
                comment="The raw text value of the supervision period supervision type.",
            ),
        )
        op.add_column(
            "state_supervision_period_history",
            sa.Column(
                "supervision_period_supervision_type",
                postgresql.ENUM(
                    "DUAL",
                    "EXTERNAL_UNKNOWN",
                    "INFORMAL_PROBATION",
                    "INTERNAL_UNKNOWN",
                    "INVESTIGATION",
                    "PAROLE",
                    "PROBATION",
                    "COMMUNITY_CONFINEMENT",
                    name="state_supervision_period_supervision_type",
                ),
                autoincrement=False,
                nullable=True,
                comment="The type of supervision the person is serving during this time period.",
            ),
        )
        op.add_column(
            "state_supervision_period",
            sa.Column(
                "supervision_period_supervision_type_raw_text",
                sa.VARCHAR(length=255),
                autoincrement=False,
                nullable=True,
                comment="The raw text value of the supervision period supervision type.",
            ),
        )
        op.add_column(
            "state_supervision_period",
            sa.Column(
                "supervision_period_supervision_type",
                postgresql.ENUM(
                    "DUAL",
                    "EXTERNAL_UNKNOWN",
                    "INFORMAL_PROBATION",
                    "INTERNAL_UNKNOWN",
                    "INVESTIGATION",
                    "PAROLE",
                    "PROBATION",
                    "COMMUNITY_CONFINEMENT",
                    name="state_supervision_period_supervision_type",
                ),
                autoincrement=False,
                nullable=True,
                comment="The type of supervision the person is serving during this time period.",
            ),
        )

        # Migrate values from supervision_type to supervision_period_supervision_type
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

            # Put supervision_type values into the
            # supervision_period_supervision_type column
            connection.execute(
                StrictStringFormatter().format(
                    UPDATE_QUERY,
                    table_name=table_name,
                    column_name="supervision_period_supervision_type",
                    new_value="supervision_type",
                    ids_query=StrictStringFormatter().format(
                        SELECT_IDS_QUERY,
                        table_name=table_name,
                        filter_clause="supervision_type IS NOT NULL",
                    ),
                )
            )

        # Drop supervision_type columns
        op.drop_column("state_supervision_period_history", "supervision_type_raw_text")
        op.drop_column("state_supervision_period_history", "supervision_type")
        op.drop_column("state_supervision_period", "supervision_type_raw_text")
        op.drop_column("state_supervision_period", "supervision_type")
